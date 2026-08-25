"""Workflow step 3: a release becomes long-format facts.

The whole of it is a projection. Columns are renamed, reshaped and typed; no
value is computed from another. That restraint is the boundary between this
store and the post-processing that reads it, and it is easy to lose — a single
"while we're here" expression in a config is all it takes.

The reshape runs in DuckDB rather than in memory. A wide panel of four thousand
columns melts to a few hundred million facts, which is comfortably larger than
memory, and the sort that makes row-group statistics useful is larger still.
DuckDB spills; a dataframe does not.

**Nulls do not become rows.** A wide file is mostly empty — the seed panel is
about half — and a null cell means "this was not reported", not "this was
reported as nothing". Skipping them is what makes long format cheaper than wide
here rather than more expensive, and it is only correct because the group
declares its publisher's convention in ``retraction_policy``.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from feature_store.spec import schema as long_schema
from feature_store.spec.canonical import hash_file, short_hash, to_jsonable
from feature_store.spec.periods import UNBOUNDED
from feature_store.spec.standardise import StandardiseSpec, decompose
from feature_store.store import catalog, duck, objects
from feature_store.store.config import Settings
from feature_store.store.runs import Run
from feature_store.store.runs import run as open_run
from feature_store.store.tables import feature, feature_group, feature_partition

FEATURES_PREFIX = "features"

#: Big enough that statistics are worth having, small enough that a query for a
#: few features skips most of the file.
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
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _quote(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def guard_namespace(spec: StandardiseSpec, source: dict[str, Any], canonical: str) -> None:
    """Invariant 2, enforced at ingest rather than inside a later join.

    A source publishing in a vocabulary that is not the canonical one needs a
    named crosswalk version, because the stored parquet is always canonical.
    Failing here means an operator is watching one file; failing at query time
    means an empty result set and a long afternoon.
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


#: How many source columns to melt at a time. A four-thousand-column UNPIVOT
#: builds an intermediate far larger than memory and fails outright; a few
#: hundred streams comfortably. Batching by column rather than by row is
#: deliberate: each batch holds a disjoint set of feature names, so each output
#: part does too, and a query for three features reads the one part holding them
#: instead of every part holding a slice of everything.
COLUMN_BATCH = 300


def _melt_query(
    spec: StandardiseSpec,
    relation: str,
    columns: list[str],
    ingest_time: datetime,
) -> str:
    """Reshape a batch of wide columns into the long schema."""
    entity = _quote(spec.entity.column)
    for step in spec.entity.normalise:
        entity = {
            "strip": f"trim({entity})",
            "upper": f"upper({entity})",
            "lower": f"lower({entity})",
        }[step]

    knowledge = "NULL::TIMESTAMP"
    if spec.knowledge_time.basis == "file_release":
        knowledge = f"TIMESTAMP '{spec.knowledge_time.release_date} 00:00:00'"
    elif spec.knowledge_time.basis == "publisher_timestamp":
        column = _quote(spec.knowledge_time.column)
        knowledge = (
            f"strptime({column}, '{spec.knowledge_time.format}')"
            if spec.knowledge_time.format
            else f"{column}::TIMESTAMP"
        )

    renames = spec.features.name.rename
    if renames:
        cases = " ".join(
            f"WHEN '{name}' THEN '{alias}'" for name, alias in sorted(renames.items())
        )
        feature_name = f"CASE raw_name {cases} ELSE raw_name END"
    else:
        feature_name = "raw_name"

    value_num = (
        "TRY_CAST(raw_value AS DOUBLE)" if spec.features.kind == "numeric" else "NULL::DOUBLE"
    )
    value_str = "NULL::VARCHAR" if spec.features.kind == "numeric" else "raw_value"

    keep = ", ".join(
        _quote(c)
        for c in dict.fromkeys([spec.entity.column, spec.event_time.column, *columns])
    )
    melted_on = ", ".join(_quote(c) for c in columns)

    return f"""
    WITH melted AS (
        UNPIVOT (SELECT {keep} FROM {relation})
        ON {melted_on}
        INTO NAME raw_name VALUE raw_value
    )
    SELECT
        {entity}                       AS {long_schema.ENTITY_ID},
        {_event_time_expression(spec)} AS {long_schema.EVENT_TIME},
        {knowledge}                    AS {long_schema.KNOWLEDGE_TIME},
        TIMESTAMP '{ingest_time}'      AS {long_schema.INGEST_TIME},
        {feature_name}                 AS {long_schema.FEATURE_NAME},
        {value_num}                    AS {long_schema.VALUE_NUM},
        {value_str}                    AS {long_schema.VALUE_STR}
    FROM melted
    WHERE raw_value IS NOT NULL
      AND {entity} IS NOT NULL
    """


def batches(items: list[str], size: int = COLUMN_BATCH) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _feature_columns(connection, source_path: str, spec: StandardiseSpec) -> list[str]:
    """Which columns of the release are features rather than identifiers."""
    reader = f"read_csv('{source_path}', header=true, all_varchar=true, sample_size=-1)"
    header = connection.execute(f"SELECT * FROM {reader} LIMIT 0").description
    names = [row[0] for row in header]

    identifiers = {spec.entity.column, spec.event_time.column, *spec.features.exclude}
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

    if not spec.features.name.allow_unlisted:
        unlisted = [n for n in candidates if n not in spec.features.name.rename]
        if unlisted:
            raise StandardiseError(
                f"{len(unlisted)} column(s) are not named in features.name.rename, and "
                f"allow_unlisted is false: {sorted(unlisted)[:8]}"
                + (" ..." if len(unlisted) > 8 else "")
                + ". A column nobody named is a feature nobody named."
            )
    return candidates


def _register_features(run: Run, group_id: int, names: list[str], spec: StandardiseSpec) -> int:
    """Write the vocabulary registry for what this group actually contains."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    dtype = "num" if spec.features.kind == "numeric" else "str"
    rows = []
    for raw in names:
        name = spec.features.name.rename.get(raw, raw)
        rows.append(
            {
                "feature_group_id": group_id,
                "name": name,
                "dtype": dtype,
                "unit": spec.features.units.get(name),
                "created_by_run_id": run.id,
                "meta": to_jsonable(decompose(name, spec.features.meta)),
            }
        )
    if not rows:
        return 0
    run.connection.execute(
        pg_insert(feature)
        .values(rows)
        .on_conflict_do_nothing(index_elements=["feature_group_id", "name"])
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
    existing = (
        run.connection.execute(
            sa.select(feature_group).where(feature_group.c.name == group_name)
        )
        .mappings()
        .first()
    )
    if existing is not None:
        return existing["id"]
    return run.connection.execute(
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
    """Read the release directly, once per column batch.

    An earlier version staged the whole file to a wide parquet first, on the
    assumption that re-reading a 750MB CSV per batch would dominate. It does
    not, and the staging step could not be made to work at all: a four-thousand
    column CSV read buffers enough per thread to exhaust memory whatever the row
    group size, because the cost is in the reader rather than the writer.

    Reading per batch is both simpler and faster, because DuckDB prunes the
    projection during parsing — a batch touching 300 of 3,953 columns takes a
    few seconds, and the whole melt is a couple of minutes.

    Everything is read as text and cast afterwards. Type inference over
    thousands of columns is expensive and unreliable, and a column that looks
    integral for its first thousand rows and is not in its last is the kind of
    thing that fails an hour in.
    """
    return f"read_csv('{source_path}', header=true, all_varchar=true, sample_size=-1)"


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
) -> WrittenPart | None:
    """Sort one view into parquet, put it in the bucket, then record it."""
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

    existing = (
        current.connection.execute(
            sa.select(feature_partition).where(feature_partition.c.object_key == key)
        )
        .mappings()
        .first()
    )
    if existing is not None:
        current.wrote("feature_partition", existing["id"], role=role)
        return WrittenPart(existing["id"], key, rows)

    # Bytes first, verified, then the catalog row.
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

    partition_id = current.connection.execute(
        sa.insert(feature_partition)
        .values(
            feature_group_id=group_id,
            object_key=key,
            content_sha256=digest,
            size_bytes=size,
            row_count=rows,
            # The feature range is what lets a reader skip a whole part without
            # opening it, which is the point of batching by column.
            partition_values={
                "entity_namespace": namespace,
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
    from feature_store.store import crosswalk as cw_module

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

    with engine.begin() as connection:
        with open_run(
            connection,
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
                        unmapped_rows += con.execute(
                            "SELECT count(*) FROM unmapped"
                        ).fetchone()[0]
                    if progress:
                        progress(index + 1, len(column_batches), written)
                    continue

                for view, group_name, namespace, role in targets:
                    if group_name not in groups:
                        groups[group_name] = _ensure_group(
                            current, spec, source, namespace, group_name
                        )
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
                for group_name, group_id in groups.items():
                    registered += _register_features(current, group_id, names, spec)
                    current.wrote("feature_group", group_id, role="group")

            current.note(
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
                features_registered=registered if not dry_run else len(names),
                unmapped_entities=len(unmapped_ids) or unmapped_rows,
                uncastable_values=uncastable,
                reused_existing=False,
            )


def load_spec(path: str | Path, overrides: dict[str, Any] | None = None) -> StandardiseSpec:
    """Read a config, apply ``--set`` overrides, and validate the result."""
    import json

    payload = json.loads(Path(path).read_text())
    for dotted, value in (overrides or {}).items():
        target = payload
        *parents, leaf = dotted.split(".")
        for part in parents:
            target = target.setdefault(part, {})
        target[leaf] = value
    return StandardiseSpec.model_validate(payload)


def check_granularity(con, spec: StandardiseSpec, granularity: str) -> int:
    """Invariant 11: event times must land on the source's declared boundary.

    A wrong anchor shifts every derived knowledge_time by up to a period, in a
    direction that is half the time the dangerous one.
    """
    if granularity in UNBOUNDED:
        return 0
    from feature_store.spec.timebasis import period_end_sql

    anchored = period_end_sql("event_time", granularity)
    if spec.event_time.anchor == "start":
        return 0
    return con.execute(
        f"SELECT count(*) FROM facts WHERE event_time <> ({anchored})::DATE"
    ).fetchone()[0]


def short_key(payload: Any) -> str:
    return short_hash(payload)
