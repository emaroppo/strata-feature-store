"""Workflow step 3: a release becomes long-format facts, in DuckDB so the melt can spill.

A projection only: columns are renamed, reshaped and typed, and no value is
computed from another. Null cells do not become rows. See ``docs/adr/0002``,
``docs/adr/0005`` and ``docs/adr/0009``.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from strata.feature_store.spec import schema as long_schema
from strata.feature_store.spec.canonical import content_hash, hash_file, to_jsonable
from strata.feature_store.spec.periods import UNBOUNDED
from strata.feature_store.spec.standardise import StandardiseSpec, decompose
from strata.feature_store.spec.timebasis import period_end_sql, period_start_sql
from strata.feature_store.store import catalog, duck, objects
from strata.feature_store.store import crosswalk as cw_module
from strata.feature_store.store.config import Settings
from strata.feature_store.store.runs import Run
from strata.feature_store.store.runs import run as open_run
from strata.feature_store.store.tables import (
    feature,
    feature_group,
    feature_partition,
    run_input,
    run_output,
)
from strata.feature_store.store.tables import run as run_table

FEATURES_PREFIX = "features"

#: Big enough for useful row-group statistics, small enough that a narrow query skips most parts.
ROW_GROUP_SIZE = 122_880


class StandardiseError(RuntimeError):
    """The release could not be mapped as the config describes."""


@dataclass(frozen=True, slots=True)
class StandardiseResult:
    run_id: int
    feature_group_id: int
    partition_id: int | None
    object_key: str | None
    rows_written: int
    features_registered: int
    unmapped_entities: int
    uncastable_values: int
    reused_existing: bool


def _naive_utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _quote(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def guard_namespace(spec: StandardiseSpec, source: dict[str, Any], canonical: str) -> None:
    """Invariant 2 at ingest: a non-canonical source needs a pinned crosswalk, a canonical one none.

    See ``docs/adr/0005``.
    """
    native = source["native_namespace"]
    if native == canonical:
        if spec.crosswalk is not None:
            raise StandardiseError(
                f"source {source['name']!r} already publishes in the canonical vocabulary "
                f"{canonical!r}, so a crosswalk would have nothing to convert."
            )
        return
    if spec.crosswalk is None:
        raise StandardiseError(
            f"source {source['name']!r} publishes entity ids in {native!r}, and this store's "
            f"canonical vocabulary is {canonical!r}. Standardising it needs a pinned crosswalk "
            "version, or every later join would silently drop the ids that differ."
        )


def _event_time_expression(spec: StandardiseSpec) -> str:
    column = _quote(spec.event_time.column)
    if spec.event_time.format:
        return f"strptime({column}, '{spec.event_time.format}')::DATE"
    return f"{column}::DATE"


#: Source columns melted per batch. By column rather than by row, so each output
#: part holds a disjoint feature range and a narrow query reads one part.
COLUMN_BATCH = 300


def _melt_query(
    spec: StandardiseSpec,
    relation: str,
    columns: list[str],
    ingest_time: datetime,
) -> str:
    """Reshape a batch of melted columns into the long schema, under either melted layout."""
    entity = _quote(spec.entity.column)
    for step in spec.entity.normalise:
        entity = {
            "strip": f"trim({entity})",
            "upper": f"upper({entity})",
            "lower": f"lower({entity})",
        }[step]

    # Only modeled_lag leaves this null; any other basis must write something,
    # since a null knowledge_time never satisfies kt <= cutoff. docs/adr/0003
    knowledge = "NULL::TIMESTAMP"
    if spec.knowledge_time.basis == "ingest_fallback":
        knowledge = f"TIMESTAMP '{ingest_time}'"
    elif spec.knowledge_time.basis == "file_release":
        knowledge = f"TIMESTAMP '{spec.knowledge_time.release_date} 00:00:00'"
    elif spec.knowledge_time.basis == "publisher_timestamp":
        column = _quote(spec.knowledge_time.column)
        knowledge = (
            f"strptime({column}, '{spec.knowledge_time.format}')"
            if spec.knowledge_time.format
            else f"{column}::TIMESTAMP"
        )

    if spec.event_time.from_column_name:
        # The header is a period label, so the anchor decides its date. docs/adr/0001
        parsed = (
            f"strptime(raw_name, '{spec.event_time.format}')::DATE"
            if spec.event_time.format
            else "raw_name::DATE"
        )
        event_time = _anchor_sql(parsed, spec.event_time.period, spec.event_time.anchor)
        feature_name = _quote(spec.features.name.column)
        carried = [spec.entity.column, spec.features.name.column]
    else:
        event_time = _event_time_expression(spec)
        renames = spec.features.name.rename
        if renames:
            cases = " ".join(
                f"WHEN '{name}' THEN '{alias}'" for name, alias in sorted(renames.items())
            )
            feature_name = f"CASE raw_name {cases} ELSE raw_name END"
        else:
            feature_name = "raw_name"
        carried = [spec.entity.column, spec.event_time.column]

    if spec.knowledge_time.column:
        carried.append(spec.knowledge_time.column)

    value_num = (
        "TRY_CAST(raw_value AS DOUBLE)" if spec.features.kind == "numeric" else "NULL::DOUBLE"
    )
    value_str = "NULL::VARCHAR" if spec.features.kind == "numeric" else "raw_value"

    keep = ", ".join(_quote(c) for c in dict.fromkeys([*carried, *columns]))
    melted_on = ", ".join(_quote(c) for c in columns)

    return f"""
    WITH melted AS (
        UNPIVOT (SELECT {keep} FROM {relation})
        ON {melted_on}
        INTO NAME raw_name VALUE raw_value
    )
    SELECT
        {entity}                  AS {long_schema.ENTITY_ID},
        {event_time}              AS {long_schema.EVENT_TIME},
        {knowledge}               AS {long_schema.KNOWLEDGE_TIME},
        TIMESTAMP '{ingest_time}' AS {long_schema.INGEST_TIME},
        {feature_name}            AS {long_schema.FEATURE_NAME},
        {value_num}               AS {long_schema.VALUE_NUM},
        {value_str}               AS {long_schema.VALUE_STR}
    FROM melted
    WHERE raw_value IS NOT NULL
      AND {entity} IS NOT NULL
    """


def _anchor_sql(expression: str, period: str, anchor: str) -> str:
    """Snap a parsed period label to the end (or start) of its period."""
    if period in UNBOUNDED:
        return expression
    snapped = (
        period_start_sql(expression, period)
        if anchor == "start"
        else period_end_sql(expression, period)
    )
    return f"({snapped})::DATE"


def batches(items: list[str], size: int = COLUMN_BATCH) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _feature_columns(connection, source_path: str, spec: StandardiseSpec) -> list[str]:
    """Which columns of the release are features rather than identifiers."""
    reader = f"read_csv('{source_path}', header=true, all_varchar=true, sample_size=-1)"
    header = connection.execute(f"SELECT * FROM {reader} LIMIT 0").description
    names = [row[0] for row in header]

    identifiers = {spec.entity.column, *spec.features.exclude}
    if spec.event_time.column:
        identifiers.add(spec.event_time.column)
    if spec.features.name.column:
        identifiers.add(spec.features.name.column)
    if spec.knowledge_time.column:
        identifiers.add(spec.knowledge_time.column)

    missing = identifiers - set(names)
    if missing:
        raise StandardiseError(
            f"the config names columns the release does not have: {sorted(missing)}. "
            f"The file has {len(names)} columns."
        )

    candidates = [n for n in names if n not in identifiers]
    if spec.features.include is not None:
        unknown = set(spec.features.include) - set(candidates)
        if unknown:
            raise StandardiseError(f"features.include names unknown columns: {sorted(unknown)}")
        candidates = [n for n in candidates if n in spec.features.include]

    if not spec.features.name.allow_unlisted and not spec.event_time.from_column_name:
        unlisted = [n for n in candidates if n not in spec.features.name.rename]
        if unlisted:
            raise StandardiseError(
                f"{len(unlisted)} column(s) are not named in features.name.rename, and "
                f"allow_unlisted is false: {sorted(unlisted)[:8]}"
                + (" ..." if len(unlisted) > 8 else "")
                + ". A column nobody named is a feature nobody named."
            )
    return candidates


def _feature_vocabulary(con, relation: str, spec: StandardiseSpec, melted: list[str]) -> list[str]:
    """The feature names this group will hold: the melted headers, or the name column's values.

    A vocabulary rather than a summary of what was non-null, so an unpublished
    feature resolves to null while a misspelled one is refused.
    """
    if not spec.event_time.from_column_name:
        return [spec.features.name.rename.get(c, c) for c in melted]

    column = _quote(spec.features.name.column)
    names = [
        row[0]
        for row in con.execute(
            f"SELECT DISTINCT {column} FROM {relation} WHERE {column} IS NOT NULL ORDER BY 1"
        ).fetchall()
    ]
    if spec.features.name.allow is not None:
        names = [n for n in names if n in spec.features.name.allow]
    return [spec.features.name.rename.get(n, n) for n in names]


def read_vocabulary(con, local_path: str, spec: StandardiseSpec) -> dict[str, dict[str, Any]]:
    """Load the companion file describing the features, as a mapping from name to what it says.

    Refuses a file missing a declared column.
    """
    vocabulary = spec.features.vocabulary
    reader = f"read_csv('{local_path}', header=true, all_varchar=true, sample_size=-1)"
    wanted = [vocabulary.key]
    for column in (vocabulary.description, vocabulary.unit):
        if column:
            wanted.append(column)
    wanted.extend(vocabulary.meta)

    selected = ", ".join(_quote(c) for c in dict.fromkeys(wanted))
    header = [row[0] for row in con.execute(f"SELECT * FROM {reader} LIMIT 0").description]
    missing = [c for c in dict.fromkeys(wanted) if c not in header]
    if missing:
        raise StandardiseError(
            f"the vocabulary file has no column(s) {missing}. It has {len(header)} columns, "
            f"starting {header[:5]}."
        )

    described: dict[str, dict[str, Any]] = {}
    for row in con.execute(f"SELECT {selected} FROM {reader}").fetchall():
        entry = dict(zip(dict.fromkeys(wanted), row, strict=True))
        key = entry.pop(vocabulary.key, None)
        if not key:
            continue
        described[key] = {k: v for k, v in entry.items() if v not in (None, "")}
    return described


def _register_features(
    run: Run,
    group_id: int,
    names: list[str],
    spec: StandardiseSpec,
    described: dict[str, dict[str, Any]] | None = None,
) -> int:
    """Write the vocabulary registry for what this group actually contains."""
    described = described or {}
    vocabulary = spec.features.vocabulary
    dtype = "num" if spec.features.kind == "numeric" else "str"
    rows = []
    for name in dict.fromkeys(names):
        entry = described.get(name, {})
        # One blob: what a publisher documents differs by publisher.
        meta = to_jsonable(decompose(name, spec.features.meta))
        if vocabulary:
            meta.update({k: v for k, v in entry.items() if k in vocabulary.meta})
        rows.append(
            {
                "feature_group_id": group_id,
                "name": name,
                "dtype": dtype,
                # The config's units map wins over the file as the more specific statement.
                "unit": spec.features.units.get(name)
                or (entry.get(vocabulary.unit) if vocabulary and vocabulary.unit else None),
                "description": (
                    entry.get(vocabulary.description)
                    if vocabulary and vocabulary.description
                    else None
                ),
                "created_by_run_id": run.id,
                "meta": meta,
            }
        )
    if not rows:
        return 0
    with run.transaction() as connection:
        statement = pg_insert(feature).values(rows)
        # Upsert so a vocabulary added after the first ingest lands; `dtype` is
        # never updated, because one feature name is one dtype forever.
        connection.execute(
            statement.on_conflict_do_update(
                index_elements=["feature_group_id", "name"],
                set_={
                    "unit": sa.func.coalesce(statement.excluded.unit, feature.c.unit),
                    "description": sa.func.coalesce(
                        statement.excluded.description, feature.c.description
                    ),
                    # Merged, not replaced: a run knowing one meta source must not erase the other.
                    "meta": sa.func.coalesce(feature.c.meta, sa.text("'{}'::jsonb")).op("||")(
                        sa.func.coalesce(statement.excluded.meta, sa.text("'{}'::jsonb"))
                    ),
                },
            )
        )
    return len(rows)


def _ensure_group(
    run: Run,
    spec: StandardiseSpec,
    source: dict[str, Any],
    namespace: str,
    group_name: str | None = None,
) -> int:
    group_name = group_name or spec.feature_group
    with run.transaction() as connection:
        existing = (
            connection.execute(sa.select(feature_group).where(feature_group.c.name == group_name))
            .mappings()
            .first()
        )
        if existing is not None:
            return existing["id"]
        return connection.execute(
            sa.insert(feature_group)
            .values(
                name=group_name,
                source_id=source["id"],
                entity_namespace=namespace,
                time_basis=spec.knowledge_time.basis,
                declared_lag=spec.knowledge_time.declared_lag,
                lag_anchor_period=spec.knowledge_time.lag_anchor_period,
                period_anchor=spec.event_time.anchor,
                retraction_policy=spec.retraction_policy,
                storage_prefix=f"{FEATURES_PREFIX}/{group_name}",
                partition_spec={"by": []},
                meta={"rationale": spec.knowledge_time.rationale}
                if spec.knowledge_time.rationale
                else None,
                created_by_run_id=run.id,
            )
            .returning(feature_group.c.id)
        ).scalar_one()


def _source_relation(source_path: str) -> str:
    """Read the release directly, once per column batch, everything as text.

    DuckDB prunes the projection while parsing, so re-reading beats staging a
    wide parquet, and inference over thousands of columns is unreliable.
    """
    return f"read_csv('{source_path}', header=true, all_varchar=true, sample_size=-1)"


def _existing_part(run: Run, group_id: int, input_hash: str) -> dict[str, Any] | None:
    """A live partition a finished run already produced from this exact work, by input hash."""
    with run.transaction() as connection:
        row = (
            connection.execute(
                sa.select(feature_partition)
                .join(run_table, run_table.c.id == feature_partition.c.created_by_run_id)
                .where(
                    feature_partition.c.feature_group_id == group_id,
                    feature_partition.c.superseded_by_run_id.is_(None),
                    run_table.c.status == "ok",
                    feature_partition.c.partition_values["input_hash"].astext == input_hash,
                )
            )
            .mappings()
            .first()
        )
    return dict(row) if row else None


@dataclass(frozen=True, slots=True)
class WrittenPart:
    partition_id: int
    object_key: str
    rows: int


def _write_part(
    current: Run,
    con,
    *,
    view: str,
    group_id: int,
    group_name: str,
    namespace: str,
    parquet_path: Path,
    s3,
    bucket: str,
    role: str,
    input_hash: str,
    release_sha256: str,
) -> WrittenPart | None:
    """Sort one view into parquet, put it in the bucket, then record it; ``None`` for an empty view.

    Idempotence is keyed on the inputs, not the bytes: every fact carries an
    ``ingest_time``, so identical work never yields identical bytes.
    """
    existing = _existing_part(current, group_id, input_hash)
    if existing is not None:
        current.wrote("feature_partition", existing["id"], role=role)
        return WrittenPart(existing["id"], existing["object_key"], existing["row_count"])

    rows = con.execute(f"SELECT count(*) FROM {view}").fetchone()[0]
    if rows == 0:
        return None

    sort = ", ".join(long_schema.SORT_ORDER)
    columns = ", ".join(long_schema.COLUMNS)
    con.execute(
        f"COPY (SELECT {columns} FROM {view} ORDER BY {sort}) TO '{parquet_path}' "
        f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {ROW_GROUP_SIZE})"
    )
    digest, size = hash_file(str(parquet_path))
    key = f"{FEATURES_PREFIX}/{group_name}/entity_namespace={namespace}/part-{digest[:16]}.parquet"

    # Bytes first, verified, then the catalog row (invariant 3).
    s3.upload_file(
        Filename=str(parquet_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs={"Metadata": {objects.SHA256_METADATA_KEY: digest}},
    )
    landed = objects.head(s3, bucket, key)
    if landed["ContentLength"] != size:
        raise StandardiseError(
            f"wrote {size} bytes to {key}, store reports {landed['ContentLength']}"
        )

    bounds = con.execute(
        f"SELECT min(event_time), max(event_time), max(knowledge_time), min(ingest_time), "
        f"min(feature_name), max(feature_name) FROM {view}"
    ).fetchone()

    with current.transaction() as connection:
        partition_id = connection.execute(
            sa.insert(feature_partition)
            .values(
                feature_group_id=group_id,
                object_key=key,
                content_sha256=digest,
                size_bytes=size,
                row_count=rows,
                partition_values={
                    "entity_namespace": namespace,
                    "input_hash": input_hash,
                    "release_sha256": release_sha256,
                    "feature_min": bounds[4],
                    "feature_max": bounds[5],
                },
                min_event_time=bounds[0],
                max_event_time=bounds[1],
                max_knowledge_time=bounds[2],
                ingest_time=bounds[3],
                created_by_run_id=current.id,
            )
            .returning(feature_partition.c.id)
        ).scalar_one()
    current.wrote("feature_partition", partition_id, role=role)
    current.observed_knowledge_time(bounds[2])
    parquet_path.unlink(missing_ok=True)
    return WrittenPart(partition_id, key, rows)


def _supersede_previous(run: Run, group_ids: list[int], release_id: int) -> int:
    """Supersede the live parts an earlier run wrote from this release that this run did not reuse.

    Matched through lineage, so parts written before this rule existed are
    covered; marked rather than deleted so earlier pins keep resolving. See ``docs/adr/0007``.
    """
    if not group_ids:
        return 0

    # A reused part is this run's output too, and must not be superseded.
    kept = (
        sa.select(run_output.c.ref_id)
        .where(run_output.c.run_id == run.id, run_output.c.ref_type == "feature_partition")
        .scalar_subquery()
    )
    earlier_runs = (
        sa.select(run_input.c.run_id)
        .where(run_input.c.ref_type == "release", run_input.c.ref_id == release_id)
        .scalar_subquery()
    )
    with run.transaction() as connection:
        result = connection.execute(
            sa.update(feature_partition)
            .where(
                feature_partition.c.feature_group_id.in_(group_ids),
                feature_partition.c.created_by_run_id != run.id,
                feature_partition.c.created_by_run_id.in_(earlier_runs),
                feature_partition.c.id.notin_(kept),
                feature_partition.c.superseded_by_run_id.is_(None),
            )
            .values(superseded_by_run_id=run.id)
        )
    return result.rowcount or 0


def standardise(
    engine: sa.Engine,
    settings: Settings,
    spec: StandardiseSpec,
    *,
    digest_prefix: str,
    s3=None,
    work_dir: str | Path | None = None,
    memory_limit: str = duck.DEFAULT_MEMORY_LIMIT,
    dry_run: bool = False,
    progress=None,
) -> StandardiseResult:
    """Turn one release into long-format parquet, and record what happened."""
    s3 = s3 or objects.client(settings.storage)
    bucket = settings.storage.bucket

    with engine.begin() as connection:
        release = catalog.find_release(connection, digest_prefix)
        if release is None:
            raise StandardiseError(f"no release whose digest starts with {digest_prefix!r}")
        source = catalog.get_source_by_id(connection, release["source_id"])
        if source["name"] != spec.source:
            raise StandardiseError(
                f"the config is for source {spec.source!r} but that release belongs to "
                f"{source['name']!r}."
            )
        guard_namespace(spec, source, settings.canonical_namespace)
        pinned = (
            cw_module.resolve(connection, spec.crosswalk.dataset, spec.crosswalk.version)
            if spec.crosswalk
            else None
        )

    params = to_jsonable(spec.model_dump(mode="python"))
    params["release_sha256"] = release["sha256"]

    work = Path(work_dir or tempfile.mkdtemp(prefix="feature-store-"))
    work.mkdir(parents=True, exist_ok=True)
    local_source = work / release["original_filename"]

    with open_run(
        engine,
        "standardise",
        f"standardise {release['original_filename']} into {spec.feature_group}",
        params,
        s3=s3,
        bucket=bucket,
    ) as current:
        current.read("release", release["id"], role="release")
        if pinned:
            current.read("dataset_version", pinned["id"], role="crosswalk")

        if not local_source.exists():
            objects.get_to_file(s3, bucket, release["object_key"], local_source)

        con = duck.connect(
            settings.storage, memory_limit=memory_limit, temp_directory=work / "spill"
        )
        names = _feature_columns(con, str(local_source), spec)
        relation = _source_relation(str(local_source))
        vocabulary = _feature_vocabulary(con, relation, spec, names)

        described: dict[str, dict[str, Any]] = {}
        if spec.features.vocabulary:
            with engine.connect() as connection:
                companion = catalog.find_release(connection, spec.features.vocabulary.release)
            if companion is None:
                raise StandardiseError(
                    f"no release whose digest starts with "
                    f"{spec.features.vocabulary.release!r} to read the vocabulary from"
                )
            current.read("release", companion["id"], role="vocabulary")
            local_vocabulary = work / companion["original_filename"]
            if not local_vocabulary.exists():
                objects.get_to_file(s3, bucket, companion["object_key"], local_vocabulary)
            described = read_vocabulary(con, str(local_vocabulary), spec)
        ingest_time = _naive_utc_now()

        canonical = settings.canonical_namespace
        if pinned:
            url = duck.s3_url(bucket, pinned["object_prefix"])
            con.execute(f"CREATE OR REPLACE VIEW cw AS SELECT * FROM read_parquet('{url}')")

        groups: dict[str, int] = {}
        written = unmapped_rows = 0
        unmapped_ids: set[str] = set()
        off_boundary = uncastable = 0
        primary_key = None
        primary_partition = None
        column_batches = batches(names)

        for index, batch in enumerate(column_batches):
            melt = _melt_query(spec, relation, batch, ingest_time)
            con.execute(f"CREATE OR REPLACE VIEW facts AS {melt}")

            uncastable += con.execute(
                "SELECT count(*) FROM facts WHERE value_num IS NULL AND value_str IS NULL"
            ).fetchone()[0]
            if uncastable:
                worst = con.execute(
                    "SELECT feature_name, count(*) FROM facts "
                    "WHERE value_num IS NULL AND value_str IS NULL "
                    "GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
                ).fetchall()
                raise StandardiseError(
                    f"{uncastable:,} value(s) could not be cast to the declared type. "
                    f"Worst offenders: {worst}. Declare the right kind, or exclude them."
                )
            off_boundary += check_granularity(con, spec, source["granularity"])

            if pinned:
                match = (
                    "cw.source_id = f.entity_id AND cw.valid_from <= f.event_time "
                    "AND f.event_time < cw.valid_to"
                )
                con.execute(
                    "CREATE OR REPLACE VIEW mapped AS SELECT cw.target_id AS entity_id, "
                    "f.event_time, f.knowledge_time, f.ingest_time, f.feature_name, "
                    f"f.value_num, f.value_str FROM facts f JOIN cw ON {match}"
                )
                con.execute(
                    f"CREATE OR REPLACE VIEW unmapped AS SELECT f.* FROM facts f "
                    f"LEFT JOIN cw ON {match} WHERE cw.source_id IS NULL"
                )
                targets = [
                    ("mapped", spec.feature_group, canonical, "facts"),
                    (
                        "unmapped",
                        f"{spec.feature_group}__unmapped",
                        source["native_namespace"],
                        "unmapped",
                    ),
                ]
            else:
                targets = [("facts", spec.feature_group, canonical, "facts")]

            if dry_run:
                written += con.execute("SELECT count(*) FROM facts").fetchone()[0]
                if pinned:
                    unmapped_rows += con.execute("SELECT count(*) FROM unmapped").fetchone()[0]
                if progress:
                    progress(index + 1, len(column_batches), written)
                continue

            for view, group_name, namespace, role in targets:
                if group_name not in groups:
                    groups[group_name] = _ensure_group(current, spec, source, namespace, group_name)
                part = _write_part(
                    current,
                    con,
                    view=view,
                    group_id=groups[group_name],
                    group_name=group_name,
                    namespace=namespace,
                    parquet_path=work / f"{group_name}-{index:04d}.parquet",
                    s3=s3,
                    bucket=bucket,
                    role=role,
                    input_hash=content_hash(
                        {
                            "config": params,
                            "release": release["sha256"],
                            "group": group_name,
                            "batch": index,
                        }
                    ),
                    release_sha256=release["sha256"],
                )
                if part is None:
                    continue
                if role == "facts":
                    written += part.rows
                    primary_key, primary_partition = part.object_key, part.partition_id
                else:
                    unmapped_rows += part.rows
            if progress:
                progress(index + 1, len(column_batches), written)

        if off_boundary:
            raise StandardiseError(
                f"{off_boundary:,} event time(s) do not fall on a {source['granularity']} "
                f"{spec.event_time.anchor} boundary. The anchor is probably wrong, and a "
                "wrong anchor shifts every derived knowledge_time by up to a period."
            )

        if pinned and not dry_run:
            unmapped_ids = set(
                con.execute(
                    "SELECT DISTINCT f.entity_id FROM (SELECT DISTINCT "
                    f'"{spec.entity.column}" AS entity_id FROM {relation}) f '
                    "LEFT JOIN cw ON cw.source_id = f.entity_id "
                    "WHERE cw.source_id IS NULL"
                ).fetchall()
            )

        registered = 0
        if not dry_run:
            for _group_name, group_id in groups.items():
                registered += _register_features(current, group_id, vocabulary, spec, described)
                current.wrote("feature_group", group_id, role="group")

        # Once at the end, not per part, so a run that dies halfway leaves the
        # old parts whole rather than a group half described by each config.
        superseded = (
            0 if dry_run else _supersede_previous(current, list(groups.values()), release["id"])
        )

        current.note(
            superseded_partitions=superseded,
            rows=written,
            unmapped_rows=unmapped_rows,
            unmapped_entities=len(unmapped_ids),
            features=registered,
            parts=len(column_batches),
            crosswalk=(
                f"{spec.crosswalk.dataset}@{spec.crosswalk.version}" if spec.crosswalk else None
            ),
            dry_run=dry_run or None,
        )

        return StandardiseResult(
            run_id=current.id,
            feature_group_id=groups.get(spec.feature_group, -1),
            partition_id=primary_partition,
            object_key=primary_key,
            rows_written=written,
            features_registered=registered if not dry_run else len(vocabulary),
            unmapped_entities=len(unmapped_ids) or unmapped_rows,
            uncastable_values=uncastable,
            reused_existing=False,
        )


def load_spec(path: str | Path, overrides: dict[str, Any] | None = None) -> StandardiseSpec:
    """Read a config, apply ``--set`` overrides, and validate the result."""
    payload = json.loads(Path(path).read_text())
    for dotted, value in (overrides or {}).items():
        target = payload
        *parents, leaf = dotted.split(".")
        for part in parents:
            target = target.setdefault(part, {})
        target[leaf] = value
    return StandardiseSpec.model_validate(payload)


def check_granularity(con, spec: StandardiseSpec, granularity: str) -> int:
    """Invariant 11: how many event times miss the declared boundary under either anchor.

    See ``docs/adr/0001``.
    """
    if granularity in UNBOUNDED:
        return 0
    boundary = (
        period_start_sql("event_time", granularity)
        if spec.event_time.anchor == "start"
        else period_end_sql("event_time", granularity)
    )
    return con.execute(
        f"SELECT count(*) FROM facts WHERE event_time <> ({boundary})::DATE"
    ).fetchone()[0]
