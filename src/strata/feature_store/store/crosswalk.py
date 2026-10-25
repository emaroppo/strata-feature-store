"""Crosswalks: mapping one entity vocabulary onto another.

A crosswalk is a source whose registered release becomes a pinned
``dataset_version`` of ``kind='crosswalk'``, validated one-to-one at registration.
See ``docs/adr/0005`` and ``docs/adr/0006``.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from strata.feature_store.spec.canonical import hash_file, to_jsonable
from strata.feature_store.store import catalog, duck, objects
from strata.feature_store.store.config import Settings
from strata.feature_store.store.runs import Run
from strata.feature_store.store.runs import run as open_run
from strata.feature_store.store.tables import dataset, dataset_version

CROSSWALK_PREFIX = "crosswalks"

#: The five columns a crosswalk parquet holds; a mapping is a relation over an interval, not a fact.
COLUMNS = ("source_id", "target_id", "valid_from", "valid_to", "confidence")


class CrosswalkError(RuntimeError):
    """The mapping would not have been safe to apply."""


@dataclass(frozen=True, slots=True)
class CrosswalkVersion:
    dataset_id: int
    version_id: int
    version_num: int
    object_key: str
    rows: int
    distinct_sources: int


def _overlapping_targets(con, table: str) -> list[tuple[str, int]]:
    """Source ids with more than one target live over the same interval."""
    return con.execute(
        f"""
        SELECT a.source_id, count(*) FROM {table} a
        JOIN {table} b
          ON a.source_id = b.source_id
         AND a.target_id <> b.target_id
         AND a.valid_from < b.valid_to
         AND b.valid_from < a.valid_to
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """
    ).fetchall()


def register(
    engine: sa.Engine,
    settings: Settings,
    *,
    name: str,
    digest_prefix: str,
    s3=None,
    work_dir: str | None = None,
) -> CrosswalkVersion:
    """Freeze a release of a crosswalk source as a pinned version.

    Refuses an empty, null-bearing or multi-target mapping. See ``docs/adr/0006``.
    """
    s3 = s3 or objects.client(settings.storage)
    bucket = settings.storage.bucket

    with engine.begin() as connection:
        release = catalog.find_release(connection, digest_prefix)
        if release is None:
            raise CrosswalkError(f"no release whose digest starts with {digest_prefix!r}")
        source = catalog.get_source_by_id(connection, release["source_id"])
        if source["kind"] != "crosswalk":
            raise CrosswalkError(
                f"source {source['name']!r} is registered as {source['kind']!r}. "
                "Register a crosswalk source with --kind crosswalk."
            )

    work = Path(work_dir or tempfile.mkdtemp(prefix="feature-store-cw-"))
    work.mkdir(parents=True, exist_ok=True)
    local = work / release["original_filename"]
    parquet_path = work / "crosswalk.parquet"

    with open_run(
        engine,
        "register_crosswalk",
        f"register crosswalk {name}",
        {"crosswalk": name, "release_sha256": release["sha256"]},
        s3=s3,
        bucket=bucket,
    ) as current:
        current.read("release", release["id"], role="release")
        objects.get_to_file(s3, bucket, release["object_key"], local)

        con = duck.connect(memory_limit="2GB")
        con.execute(
            f"CREATE TABLE cw AS SELECT "
            f"CAST(source_id AS VARCHAR) AS source_id, "
            f"CAST(target_id AS VARCHAR) AS target_id, "
            f"CAST(valid_from AS DATE) AS valid_from, "
            f"CAST(valid_to AS DATE) AS valid_to, "
            f"CAST(confidence AS DOUBLE) AS confidence "
            f"FROM read_csv('{local}', header=true)"
        )
        rows, sources = con.execute("SELECT count(*), count(DISTINCT source_id) FROM cw").fetchone()
        if rows == 0:
            raise CrosswalkError("the crosswalk is empty")

        nulls = con.execute(
            "SELECT count(*) FROM cw WHERE source_id IS NULL OR target_id IS NULL"
        ).fetchone()[0]
        if nulls:
            raise CrosswalkError(f"{nulls} row(s) have a null source or target id")

        offenders = _overlapping_targets(con, "cw")
        if offenders:
            raise CrosswalkError(
                f"{len(offenders)} source id(s) map to more than one target over the same "
                f"interval: {offenders[:5]}. One fact would become several, the row count "
                "would grow, and nothing would report it. Express a succession with "
                "non-overlapping date ranges, or leave the id unmapped."
            )

        con.execute(
            f"COPY (SELECT * FROM cw ORDER BY source_id, valid_from) "
            f"TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        digest, size = hash_file(str(parquet_path))

        dataset_id = _ensure_dataset(current, name)
        with current.transaction() as connection:
            version_num = (
                connection.execute(
                    sa.select(
                        sa.func.coalesce(sa.func.max(dataset_version.c.version_num), 0)
                    ).where(dataset_version.c.dataset_id == dataset_id)
                ).scalar_one()
                + 1
            )
        key = f"{CROSSWALK_PREFIX}/{name}/v{version_num}/part-{digest[:16]}.parquet"

        s3.upload_file(
            Filename=str(parquet_path),
            Bucket=bucket,
            Key=key,
            ExtraArgs={"Metadata": {objects.SHA256_METADATA_KEY: digest}},
        )
        landed = objects.head(s3, bucket, key)
        if landed["ContentLength"] != size:
            raise CrosswalkError(f"upload of {key} did not land intact")

        spec = {
            "kind": "crosswalk",
            "name": name,
            "source": source["name"],
            "release_sha256": release["sha256"],
            "rows": rows,
            "distinct_sources": sources,
        }
        with current.transaction() as connection:
            version_id = connection.execute(
                sa.insert(dataset_version)
                .values(
                    dataset_id=dataset_id,
                    version_num=version_num,
                    resolution_mode="materialized",
                    spec=to_jsonable(spec),
                    content_hash=digest,
                    ingest_watermark=release["ingested_at"],
                    object_prefix=key,
                    created_by_run_id=current.id,
                )
                .returning(dataset_version.c.id)
            ).scalar_one()
        current.wrote("dataset_version", version_id, role="crosswalk")
        current.note(rows=rows, distinct_sources=sources, version=version_num)

    return CrosswalkVersion(
        dataset_id=dataset_id,
        version_id=version_id,
        version_num=version_num,
        object_key=key,
        rows=rows,
        distinct_sources=sources,
    )


def _ensure_dataset(run: Run, name: str) -> int:
    with run.transaction() as connection:
        existing = connection.execute(
            sa.select(dataset.c.id).where(dataset.c.name == name)
        ).scalar_one_or_none()
        if existing is not None:
            return existing
        return connection.execute(
            sa.insert(dataset).values(name=name, kind="crosswalk").returning(dataset.c.id)
        ).scalar_one()


def resolve(connection: sa.Connection, name: str, version_num: int) -> dict[str, Any]:
    """The pinned version's row, or an error naming what does exist."""
    return catalog.version_row(
        connection,
        name,
        version_num,
        error=lambda available: CrosswalkError(
            f"no crosswalk {name!r} at version {version_num}. "
            f"Available: {available or 'none - is it registered?'}"
        ),
    )
