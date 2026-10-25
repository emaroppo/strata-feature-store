"""Workflow step 4: resolve a composition to what was knowable, and freeze it.

One query serves both freeze modes, so a materialised version is a cache of the
query form and never a second implementation. See ``docs/adr/0007``.
"""

from __future__ import annotations

import calendar
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from strata.feature_store.spec import schema as long_schema
from strata.feature_store.spec.canonical import content_hash, hash_file, to_jsonable
from strata.feature_store.spec.compose import ComposeSpec, FeatureRef
from strata.feature_store.spec.duration import add_duration, parse_duration
from strata.feature_store.spec.timebasis import knowledge_time_sql
from strata.feature_store.store import catalog, duck, objects
from strata.feature_store.store.config import Settings
from strata.feature_store.store.runs import run as open_run
from strata.feature_store.store.tables import (
    dataset,
    dataset_version,
    entity_list,
    entity_list_member,
    entity_list_version,
    feature_partition,
)
from strata.feature_store.store.tables import feature as feature_table
from strata.feature_store.store.tables import feature_group as feature_group_table


class ComposeError(RuntimeError):
    """The spec could not be resolved against what the catalog holds."""


@dataclass(frozen=True, slots=True)
class ResolvedGroup:
    """A feature group, its live files, and how to read its knowledge_time."""

    id: int
    name: str
    entity_namespace: str
    #: Full s3:// URLs from the catalog, never from listing a prefix (invariant 7).
    urls: list[str]
    knowledge_time: str
    retraction_policy: str = "honour"


@dataclass(frozen=True, slots=True)
class Plan:
    """Everything resolution needs, gathered before a single byte is read."""

    spec: ComposeSpec
    groups: dict[str, ResolvedGroup]
    entities: list[str]
    times: list[date]
    watermark: datetime
    namespace: str
    storage: Any = None
    #: Each selected feature's stored type, so a wide output emits one value column per feature.
    dtypes: dict = field(default_factory=dict)


def _quote_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def plan(connection: sa.Connection, spec: ComposeSpec, settings: Settings) -> Plan:
    """Validate the spec against the catalog and gather what resolution needs, opening no object.

    Refuses an unknown group, feature or entity list, a group outside the
    canonical namespace, and a group with no readable partitions.
    """
    groups: dict[str, ResolvedGroup] = {}
    for name in spec.groups:
        row = (
            connection.execute(
                sa.select(feature_group_table).where(feature_group_table.c.name == name)
            )
            .mappings()
            .first()
        )
        if row is None:
            available = (
                connection.execute(
                    sa.select(feature_group_table.c.name).order_by(feature_group_table.c.name)
                )
                .scalars()
                .all()
            )
            raise ComposeError(f"no feature group named {name!r}. Available: {available}")

        # Invariant 2 at read: a non-canonical group is the unmapped sibling. docs/adr/0006
        if row["entity_namespace"] != settings.canonical_namespace:
            raise ComposeError(
                f"group {name!r} is in {row['entity_namespace']!r}, not the canonical "
                f"{settings.canonical_namespace!r}. It holds the entities that had no "
                "canonical id, and joining it to canonical data would be comparing "
                "different vocabularies."
            )

        partitions = catalog.live_partitions(connection, row["id"])
        if not partitions:
            raise ComposeError(
                f"group {name!r} has no readable partitions. Either nothing has been "
                "standardised into it, or the run that did never finished."
            )
        lag = parse_duration(row["declared_lag"]) if row["declared_lag"] else None
        groups[name] = ResolvedGroup(
            id=row["id"],
            name=name,
            entity_namespace=row["entity_namespace"],
            urls=[duck.s3_url(settings.storage.bucket, p["object_key"]) for p in partitions],
            knowledge_time=knowledge_time_sql(
                row["time_basis"],
                lag,
                table="f",
                lag_anchor_period=row["lag_anchor_period"],
            ),
            retraction_policy=row["retraction_policy"],
        )

    dtypes = _resolve_dtypes(connection, spec, groups)
    entities = _resolve_entities(connection, spec, groups, settings)
    times = _resolve_times(spec)
    watermark = _watermark(connection, groups)
    return Plan(
        spec=spec,
        groups=groups,
        entities=entities,
        times=times,
        watermark=watermark,
        namespace=settings.canonical_namespace,
        storage=settings.storage,
        dtypes=dtypes,
    )


def _resolve_dtypes(
    connection: sa.Connection, spec: ComposeSpec, groups: dict[str, ResolvedGroup]
) -> dict[str, str]:
    """What each selected feature is stored as; refuses a name the registry does not hold."""
    out: dict[str, str] = {}
    for ref in spec.features:
        dtype = connection.execute(
            sa.select(feature_table.c.dtype).where(
                feature_table.c.feature_group_id == groups[ref.group].id,
                feature_table.c.name == ref.name,
            )
        ).scalar_one_or_none()
        if dtype is None:
            near = (
                connection.execute(
                    sa.select(feature_table.c.name)
                    .where(
                        feature_table.c.feature_group_id == groups[ref.group].id,
                        feature_table.c.name.like(f"{ref.name[:6]}%"),
                    )
                    .limit(5)
                )
                .scalars()
                .all()
            )
            raise ComposeError(
                f"group {ref.group!r} has no feature named {ref.name!r}."
                + (f" Did you mean one of {near}?" if near else "")
            )
        out[ref.output_name] = dtype
    return out


def _resolve_entities(
    connection: sa.Connection,
    spec: ComposeSpec,
    groups: dict[str, ResolvedGroup],
    settings: Settings,
) -> list[str]:
    source = spec.spine.entities
    if source.ids is not None:
        return sorted(set(source.ids))
    if source.name is not None:
        version = connection.execute(
            sa.select(entity_list_version.c.id)
            .join(entity_list, entity_list.c.id == entity_list_version.c.entity_list_id)
            .where(
                entity_list.c.name == source.name,
                entity_list_version.c.version_num == source.version,
            )
        ).scalar_one_or_none()
        if version is None:
            raise ComposeError(f"no entity list {source.name!r} at version {source.version}")
        members = (
            connection.execute(
                sa.select(entity_list_member.c.entity_id).where(
                    entity_list_member.c.entity_list_version_id == version
                )
            )
            .scalars()
            .all()
        )
        if not members:
            raise ComposeError(f"entity list {source.name!r} v{source.version} is empty")
        return sorted(members)

    # Every entity in a group; reproducible because the watermark is pinned. docs/adr/0007
    if source.from_group not in groups:
        raise ComposeError(
            f"the spine draws entities from {source.from_group!r}, which the spec does "
            "not select any features from"
        )
    group = groups[source.from_group]
    con = duck.connect(settings.storage)
    urls = ", ".join(_quote_literal(u) for u in group.urls)
    rows = con.execute(
        f"SELECT DISTINCT entity_id FROM read_parquet([{urls}]) ORDER BY 1"
    ).fetchall()
    return [r[0] for r in rows]


def _resolve_times(spec: ComposeSpec) -> list[date]:
    grid = spec.spine.times
    if grid.dates is not None:
        return sorted(set(grid.dates))
    step = parse_duration(grid.step)
    out: list[date] = []
    current = grid.start
    while current <= grid.end:
        out.append(current)
        nxt = add_duration(current, step)
        if nxt <= current:  # pragma: no cover - Duration forbids zero
            raise ComposeError(f"step {grid.step!r} does not advance")
        current = nxt
    return out


def _watermark(connection: sa.Connection, groups: dict[str, ResolvedGroup]) -> datetime:
    """The highest ingest_time this composition can currently see. See ``docs/adr/0007``."""
    highest = connection.execute(
        sa.select(sa.func.max(feature_partition.c.ingest_time)).where(
            feature_partition.c.feature_group_id.in_([g.id for g in groups.values()])
        )
    ).scalar_one_or_none()
    if highest is None:  # pragma: no cover - plan() already refused empty groups
        raise ComposeError("no partitions to read")
    return highest


def cutoff_sql(spec: ComposeSpec, feature: FeatureRef) -> str:
    """The moment a given feature is allowed to know things, per spine row."""
    policy, offset, at = spec.as_of.policy, spec.as_of.offset, spec.as_of.at
    for exception in spec.as_of.exceptions:
        if exception.feature in (feature.name, feature.output_name):
            policy, offset, at = exception.policy, exception.offset, exception.at
            break

    if policy == "fixed":
        return f"TIMESTAMP '{at} 00:00:00'"
    if policy == "spine_time":
        return "t.spine_time::TIMESTAMP"
    interval = _interval(offset)
    return f"(t.spine_time::TIMESTAMP - {interval})"


def _interval(offset: str) -> str:
    duration = parse_duration(offset)
    terms = []
    if duration.months:
        terms.append(f"INTERVAL '{duration.months} months'")
    if duration.days:
        terms.append(f"INTERVAL '{duration.days} days'")
    return " - ".join(terms) if len(terms) == 1 else f"({' + '.join(terms)})"


def build_sql(plan: Plan) -> str:
    """The one query. Both freeze modes run exactly this."""
    spec = plan.spec
    entity_values = ", ".join(f"({_quote_literal(e)})" for e in plan.entities)
    time_values = ", ".join(f"(DATE '{t}')" for t in plan.times)

    # One spine arm per feature, each carrying its own cutoff expression; the
    # ASOF join must key on feature as well as entity or every value lands on
    # the wrong feature's timeline.
    arms = []
    for ref in spec.features:
        arms.append(
            f"""
        SELECT e.entity_id, t.spine_time,
               {_quote_literal(ref.group)} AS group_name,
               {_quote_literal(ref.name)} AS feature_name,
               {_quote_literal(ref.output_name)} AS output_name,
               {cutoff_sql(spec, ref)} AS cutoff
        FROM entities e CROSS JOIN times t"""
        )
    spine = "\n        UNION ALL".join(arms)

    fact_unions = []
    for name, group in plan.groups.items():
        urls = ", ".join(_quote_literal(u) for u in group.urls)
        wanted = ", ".join(_quote_literal(r.name) for r in spec.features if r.group == name)
        fact_unions.append(
            f"""
        SELECT
            {_quote_literal(name)} AS group_name,
            f.entity_id,
            f.event_time,
            {group.knowledge_time} AS kt,
            f.feature_name,
            f.value_num,
            f.value_str
        FROM read_parquet([{urls}]) f
        WHERE f.ingest_time <= TIMESTAMP '{plan.watermark}'
          AND f.feature_name IN ({wanted})"""
        )
    facts = "\n        UNION ALL".join(fact_unions)

    # Calendar arithmetic on the interval, so a bound in months is exact for every month length.
    stale_cases = [
        f"WHEN output_name = {_quote_literal(ref.output_name)} "
        f"AND kt + {_age_interval(ref.max_age)} < cutoff THEN TRUE"
        for ref in spec.features
        if ref.max_age
    ]
    stale = f"CASE {' '.join(stale_cases)} ELSE FALSE END" if stale_cases else "FALSE"

    # A null withdraws a value only in groups that honour retraction. docs/adr/0009
    honouring = [
        _quote_literal(ref.group)
        for ref in spec.features
        if plan.groups[ref.group].retraction_policy == "honour"
    ]
    if honouring:
        retraction = (
            "AND NOT (f.value_num IS NULL AND f.value_str IS NULL "
            f"AND f.group_name NOT IN ({', '.join(sorted(set(honouring)))}))"
        )
    else:
        retraction = "AND NOT (f.value_num IS NULL AND f.value_str IS NULL)"

    body = f"""
    WITH entities(entity_id) AS (VALUES {entity_values}),
    times(spine_time) AS (VALUES {time_values}),
    spine AS ({spine}
    ),
    facts AS ({facts}
    ),
    visible AS (
        SELECT * FROM facts f
        WHERE TRUE {retraction}
    ),
    matched AS (
        SELECT s.entity_id, s.spine_time, s.output_name, s.cutoff,
               f.value_num, f.value_str, f.kt, f.event_time,
               date_diff('day', f.kt, s.cutoff) AS age_days
        FROM spine s
        ASOF LEFT JOIN visible f
          ON s.entity_id = f.entity_id
         AND s.group_name = f.group_name
         AND s.feature_name = f.feature_name
         AND f.kt <= s.cutoff
    ),
    aged AS (
        SELECT *, {stale} AS stale FROM matched
    ),
    resolved AS (
        SELECT
            entity_id,
            spine_time,
            cutoff AS data_through,
            output_name AS feature_name,
            -- Staleness nulls the value and its metadata together, before any pivot.
            CASE WHEN stale THEN NULL ELSE value_num END AS value_num,
            CASE WHEN stale THEN NULL ELSE value_str END AS value_str,
            CASE WHEN stale THEN NULL ELSE kt END AS knowledge_time,
            CASE WHEN stale THEN NULL ELSE age_days END AS age_days,
            CASE WHEN stale THEN NULL ELSE event_time END AS event_time
        FROM aged
    )"""

    if spec.output.orientation == "wide":
        return body + _wide_select(plan)
    return body + _long_select(spec)


def _long_select(spec: ComposeSpec) -> str:
    """One row per sample, which is what the store holds."""
    columns = ["entity_id", "spine_time"]
    if spec.output.include_data_through:
        columns.append("data_through")
    columns += ["feature_name", "value_num", "value_str"]
    if spec.output.include_knowledge_time:
        columns.append("knowledge_time")
    if spec.output.include_age_days:
        columns.append("age_days")
    columns.append("event_time")
    return f"""
    SELECT {", ".join(columns)} FROM resolved
    ORDER BY entity_id, spine_time, feature_name
    """


def _wide_select(plan: Plan) -> str:
    """The same samples pivoted, observations as rows and features as columns.

    Written out rather than with DuckDB's PIVOT, which infers columns from the
    data and would drop a feature that resolved to nothing. See ``docs/adr/0002``.
    """
    spec = plan.spec
    columns = ["entity_id", "spine_time"]
    if spec.output.include_data_through:
        columns.append("any_value(data_through) AS data_through")
    for ref in spec.features:
        name = ref.output_name
        quoted = _quote_literal(name)
        source = "value_str" if plan.dtypes.get(name) == "str" else "value_num"
        columns.append(
            f"max(CASE WHEN feature_name = {quoted} THEN {source} END) AS {_quote_ident(name)}"
        )
        if spec.output.include_knowledge_time:
            columns.append(
                f"max(CASE WHEN feature_name = {quoted} THEN knowledge_time END) "
                f"AS {_quote_ident(name + '__knowledge_time')}"
            )
        if spec.output.include_age_days:
            columns.append(
                f"max(CASE WHEN feature_name = {quoted} THEN age_days END) "
                f"AS {_quote_ident(name + '__age_days')}"
            )
    return f"""
    SELECT {", ".join(columns)}
    FROM resolved
    GROUP BY entity_id, spine_time
    ORDER BY entity_id, spine_time
    """


def _quote_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _age_interval(max_age: str) -> str:
    """A staleness bound as a DuckDB interval, months and days kept apart."""
    duration = parse_duration(max_age)
    terms = []
    if duration.months:
        terms.append(f"INTERVAL '{duration.months} months'")
    if duration.days:
        terms.append(f"INTERVAL '{duration.days} days'")
    return " + ".join(terms)


def resolve(plan: Plan, connection=None, memory_limit: str = "4GB"):
    """Run the query and hand back a DuckDB relation, so a freeze can COPY it out of memory."""
    con = connection or duck.connect(plan.storage, memory_limit=memory_limit)
    return con.sql(build_sql(plan))


def max_cutoff(plan: Plan) -> date:
    """The latest moment any spine row may know something; bounds the withheld count.

    See ``docs/adr/0007``.
    """
    spec = plan.spec
    latest = max(plan.times)
    cutoffs = [
        spec.as_of.at
        if spec.as_of.policy == "fixed"
        else latest
        if spec.as_of.policy == "spine_time"
        else _subtract(latest, spec.as_of.offset)
    ]
    for exception in spec.as_of.exceptions:
        cutoffs.append(
            exception.at
            if exception.policy == "fixed"
            else latest
            if exception.policy == "spine_time"
            else _subtract(latest, exception.offset)
        )
    return max(cutoffs)


def _subtract(day: date, offset: str) -> date:
    """*day* minus an ISO-8601 duration, months first."""
    duration = parse_duration(offset)
    index = day.month - 1 - duration.months
    year = day.year + index // 12
    month = index % 12 + 1
    shifted = date(year, month, min(day.day, calendar.monthrange(year, month)[1]))
    if duration.days:
        shifted = date.fromordinal(shifted.toordinal() - duration.days)
    return shifted


def withheld_by_watermark(plan: Plan) -> dict[str, Any]:
    """The facts knowable by the latest cutoff that arrived after the watermark, counted per group.

    See ``docs/adr/0007``.
    """
    con = duck.connect(plan.storage)
    ceiling = max_cutoff(plan)
    counts: dict[str, int] = {}
    newest: datetime | None = None
    for name, group in plan.groups.items():
        urls = ", ".join(_quote_literal(u) for u in group.urls)
        row = con.execute(
            f"""
            SELECT count(*), max({group.knowledge_time})
            FROM read_parquet([{urls}]) f
            WHERE f.ingest_time > TIMESTAMP '{plan.watermark}'
              AND {group.knowledge_time} <= TIMESTAMP '{ceiling} 23:59:59'
            """
        ).fetchone()
        if row[0]:
            counts[name] = row[0]
            if newest is None or (row[1] and row[1] > newest):
                newest = row[1]
    return {
        "groups": counts,
        "facts": sum(counts.values()),
        "newest_knowledge_time": newest,
        "cutoff": ceiling,
    }


LONG_COLUMNS = (
    "entity_id",
    "spine_time",
    "data_through",
    "feature_name",
    long_schema.VALUE_NUM,
    long_schema.VALUE_STR,
    "knowledge_time",
    "age_days",
    "event_time",
)


# --------------------------------------------------------------------------
# freezing and reading


@dataclass(frozen=True, slots=True)
class FrozenVersion:
    dataset_id: int
    version_id: int
    version_num: int
    content_hash: str
    resolution_mode: str
    watermark: datetime
    rows: int | None
    object_prefix: str | None


def freeze(
    engine: sa.Engine,
    settings: Settings,
    spec: ComposeSpec,
    *,
    s3=None,
    memory_limit: str = "4GB",
) -> FrozenVersion:
    """Pin a composition as a new dataset version; ``materialized`` also writes the parquet.

    Both modes run the same ``resolve``. See ``docs/adr/0007`` and ``docs/adr/0008``.
    """
    s3 = s3 or objects.client(settings.storage)
    canonical = to_jsonable(spec.canonical())
    digest = content_hash(canonical)

    with engine.connect() as connection:
        resolved = plan(connection, spec, settings)

    with open_run(
        engine,
        "compose" if spec.freeze.mode == "query" else "materialize",
        f"freeze {spec.name} ({spec.freeze.mode})",
        canonical,
        s3=s3,
        bucket=settings.storage.bucket,
    ) as current:
        for group in resolved.groups.values():
            current.wrote("feature_group", group.id, role="read")

        con = duck.connect(settings.storage, memory_limit=memory_limit)
        relation = resolve(resolved, connection=con)

        with current.transaction() as connection:
            dataset_id = connection.execute(
                sa.select(dataset.c.id).where(dataset.c.name == spec.name)
            ).scalar_one_or_none()
            if dataset_id is None:
                dataset_id = connection.execute(
                    sa.insert(dataset)
                    .values(name=spec.name, kind="composed", description=spec.description)
                    .returning(dataset.c.id)
                ).scalar_one()
            version_num = (
                connection.execute(
                    sa.select(
                        sa.func.coalesce(sa.func.max(dataset_version.c.version_num), 0)
                    ).where(dataset_version.c.dataset_id == dataset_id)
                ).scalar_one()
                + 1
            )

        rows = None
        prefix = None
        if spec.freeze.mode == "materialized":
            work = Path(tempfile.mkdtemp(prefix="feature-store-compose-"))
            local = work / "part.parquet"
            con.execute(f"COPY ({relation.sql_query()}) TO '{local}' (FORMAT PARQUET)")
            rows = con.execute(f"SELECT count(*) FROM read_parquet('{local}')").fetchone()[0]
            file_digest, size = hash_file(str(local))
            prefix = f"composed/{spec.name}/v{version_num}/part-{file_digest[:16]}.parquet"
            s3.upload_file(
                Filename=str(local),
                Bucket=settings.storage.bucket,
                Key=prefix,
                ExtraArgs={"Metadata": {objects.SHA256_METADATA_KEY: file_digest}},
            )
            landed = objects.head(s3, settings.storage.bucket, prefix)
            if landed["ContentLength"] != size:
                raise ComposeError(f"upload of {prefix} did not land intact")
        else:
            # Run it once anyway, so a frozen spec is known to execute.
            rows = con.execute(f"SELECT count(*) FROM ({relation.sql_query()})").fetchone()[0]

        # The spec travels next to the bytes, so a frozen output outlives the catalog.
        objects.put_bytes(
            s3,
            settings.storage.bucket,
            f"composed/{spec.name}/v{version_num}/spec.json",
            __import__("json").dumps(canonical, indent=2, sort_keys=True).encode(),
            content_type="application/json",
        )

        with current.transaction() as connection:
            version_id = connection.execute(
                sa.insert(dataset_version)
                .values(
                    dataset_id=dataset_id,
                    version_num=version_num,
                    resolution_mode=spec.freeze.mode,
                    spec=canonical,
                    content_hash=digest,
                    ingest_watermark=resolved.watermark,
                    object_prefix=prefix,
                    created_by_run_id=current.id,
                )
                .returning(dataset_version.c.id)
            ).scalar_one()
        current.wrote("dataset_version", version_id, role="composed")
        current.note(rows=rows, mode=spec.freeze.mode, watermark=str(resolved.watermark))

    return FrozenVersion(
        dataset_id=dataset_id,
        version_id=version_id,
        version_num=version_num,
        content_hash=digest,
        resolution_mode=spec.freeze.mode,
        watermark=resolved.watermark,
        rows=rows,
        object_prefix=prefix,
    )


def load_version(connection: sa.Connection, name: str, version: int | None = None) -> dict:
    """A frozen version's row, defaulting to the newest."""
    return catalog.version_row(
        connection,
        name,
        version,
        error=lambda available: ComposeError(
            f"no dataset {name!r}"
            + (f" at version {version}. Available: {available}" if available else "")
        ),
    )


def read(
    engine: sa.Engine,
    settings: Settings,
    name: str,
    version: int | None = None,
    *,
    latest: bool = False,
    memory_limit: str = "4GB",
):
    """Resolve a frozen version and return the relation with what its watermark withholds.

    ``latest=True`` ignores the watermark and returns an empty warning. See ``docs/adr/0007``.
    """
    with engine.connect() as connection:
        row = load_version(connection, name, version)
        spec = ComposeSpec.model_validate(row["spec"])
        resolved = plan(connection, spec, settings)

    if not latest:
        resolved = Plan(
            spec=resolved.spec,
            groups=resolved.groups,
            entities=resolved.entities,
            times=resolved.times,
            watermark=row["ingest_watermark"],
            namespace=resolved.namespace,
            storage=resolved.storage,
        )

    if row["resolution_mode"] == "materialized" and not latest:
        con = duck.connect(settings.storage, memory_limit=memory_limit)
        url = duck.s3_url(settings.storage.bucket, row["object_prefix"])
        return con.sql(f"SELECT * FROM read_parquet('{url}')"), {}

    con = duck.connect(settings.storage, memory_limit=memory_limit)
    relation = resolve(resolved, connection=con)
    warning = {} if latest else withheld_by_watermark(resolved)
    return relation, warning
