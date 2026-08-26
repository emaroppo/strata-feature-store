"""Workflow step 4: resolve a spec to what was knowable.

One function builds the query, and both freeze modes call it. ``query`` runs it
to prove the spec executes and stores the spec; ``materialized`` runs it and
writes the parquet. The materialised path is therefore a cache of the query
path, never a second implementation, and a test asserts the two produce equal
frames rather than the code merely intending them to.

**The ASOF join keys on entity and feature together.** Keying on entity alone
matches a country's GDP against its population's timeline — the join succeeds,
the row count looks right, and every value is attached to the wrong feature's
history. The architecture sketch omits ``feature_name`` from its ON clause and
is easy to copy verbatim and be wrong.

**Two time axes are pinned, not one.** A pinned as-of cutoff alone does not make
a version reproducible: a fact ingested later whose knowledge_time falls before
the cutoff would change the answer, which is precisely what a bitemporal store
exists to represent. So resolution also filters on an ``ingest_watermark``, and
reads of a pinned version warn when that withholds something.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import sqlalchemy as sa

from feature_store.spec import schema as long_schema
from feature_store.spec.compose import ComposeSpec, FeatureRef
from feature_store.spec.duration import add_duration, parse_duration
from feature_store.spec.timebasis import knowledge_time_sql
from feature_store.store import catalog, duck
from feature_store.store.config import Settings
from feature_store.store.tables import entity_list, entity_list_member, entity_list_version
from feature_store.store.tables import feature_group as feature_group_table


class ComposeError(RuntimeError):
    """The spec could not be resolved against what the catalog holds."""


@dataclass(frozen=True, slots=True)
class ResolvedGroup:
    """A feature group, its live files, and how to read its knowledge_time."""

    id: int
    name: str
    entity_namespace: str
    #: Full s3:// URLs, built from the catalog rather than by listing a prefix.
    #: That is invariant 7: a glob would also read the orphans a failed run
    #: leaves behind, and those describe part of a group with nothing saying so.
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
    #: Each selected feature's stored type, from the vocabulary registry. A wide
    #: output emits one value column per feature rather than both, because a
    #: numeric feature's string column is null in every row of every panel and
    #: doubling the column count with guaranteed nulls helps nobody.
    dtypes: dict = field(default_factory=dict)


def _quote_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def plan(connection: sa.Connection, spec: ComposeSpec, settings: Settings) -> Plan:
    """Validate the spec against the catalog and gather what it needs.

    Everything that can fail on a misconfiguration fails here, before any
    object is opened. A spec naming a group that does not exist should say so
    in a second, not after streaming a gigabyte.
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
            available = connection.execute(
                sa.select(feature_group_table.c.name).order_by(feature_group_table.c.name)
            ).scalars().all()
            raise ComposeError(f"no feature group named {name!r}. Available: {available}")

        # Invariant 2 at read: every group in one composition must speak one
        # vocabulary. Ids were canonicalised at write, so a group that is not
        # canonical is the unmapped sibling, and joining it to the others would
        # match ids that only look alike.
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
            urls=[
                duck.s3_url(settings.storage.bucket, p["object_key"]) for p in partitions
            ],
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
    """What each selected feature is stored as, and a refusal if it is unknown.

    The registry is what makes a typo in a feature name fail here rather than
    resolving to a column of nulls that looks like missing data.
    """
    from feature_store.store.tables import feature as feature_table

    out: dict[str, str] = {}
    for ref in spec.features:
        dtype = connection.execute(
            sa.select(feature_table.c.dtype).where(
                feature_table.c.feature_group_id == groups[ref.group].id,
                feature_table.c.name == ref.name,
            )
        ).scalar_one_or_none()
        if dtype is None:
            near = connection.execute(
                sa.select(feature_table.c.name)
                .where(
                    feature_table.c.feature_group_id == groups[ref.group].id,
                    feature_table.c.name.like(f"{ref.name[:6]}%"),
                )
                .limit(5)
            ).scalars().all()
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
        version = (
            connection.execute(
                sa.select(entity_list_version.c.id)
                .join(entity_list, entity_list.c.id == entity_list_version.c.entity_list_id)
                .where(
                    entity_list.c.name == source.name,
                    entity_list_version.c.version_num == source.version,
                )
            ).scalar_one_or_none()
        )
        if version is None:
            raise ComposeError(
                f"no entity list {source.name!r} at version {source.version}"
            )
        members = connection.execute(
            sa.select(entity_list_member.c.entity_id).where(
                entity_list_member.c.entity_list_version_id == version
            )
        ).scalars().all()
        if not members:
            raise ComposeError(f"entity list {source.name!r} v{source.version} is empty")
        return sorted(members)

    # Every entity present in a group. Reproducible only because the version
    # pins an ingest watermark; without one the set would grow under a frozen
    # spec and two resolutions would differ in their row count.
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
    """The highest ingest_time this composition can currently see.

    Pinned into a frozen version so a later arrival cannot change the answer.
    """
    from feature_store.store.tables import feature_partition

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

    # One spine arm per feature, so each carries its own cutoff expression. A
    # single arm with the cutoff in a VALUES list cannot work: the expression
    # references the spine timestamp, which does not exist yet inside a literal.
    #
    # The spine is keyed by feature as well as entity and time, because the ASOF
    # join resolves each feature against its own timeline. Keying on entity
    # alone matches a country's GDP against its population's history — the join
    # succeeds and every value is attached to the wrong feature.
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
        wanted = ", ".join(
            _quote_literal(r.name) for r in spec.features if r.group == name
        )
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
    # UNION ALL rather than a join: identical schemas mean no column collisions
    # and nothing to reconcile.
    facts = "\n        UNION ALL".join(fact_unions)

    # Real calendar arithmetic, not an approximation. Comparing a resolved
    # knowledge time plus the bound against the cutoff is exact for every month
    # length; converting months to days is not, and a staleness bound that
    # drifts by two days a year is a bound nobody can reason about.
    stale_cases = [
        f"WHEN output_name = {_quote_literal(ref.output_name)} "
        f"AND kt + {_age_interval(ref.max_age)} < cutoff THEN TRUE"
        for ref in spec.features
        if ref.max_age
    ]
    stale = f"CASE {' '.join(stale_cases)} ELSE FALSE END" if stale_cases else "FALSE"

    # Only groups whose publisher withdraws a value by emitting a null. For the
    # others a null is filler meaning "not reported", and treating it as a
    # withdrawal would erase a perfectly good earlier figure.
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
            -- Staleness nulls the value and its metadata together. Applied
            -- before any pivot, so a nulled value takes its age with it rather
            -- than leaving an age beside an empty cell.
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
    """The same samples, pivoted so observations are rows and features columns.

    Written out explicitly rather than with DuckDB's PIVOT, because the column
    set has to be exactly what the spec asked for, in that order, whether or not
    a feature happened to resolve to anything. A dynamic pivot infers its
    columns from the data, so a feature that resolved to nothing for every row
    would silently vanish from the output.

    Wide is a presentation of the same samples: an output covering n
    observations and f features holds n x f samples either way.
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
    """Run the query and hand back the relation.

    Callers get a DuckDB relation rather than a frame, so a materialising freeze
    can COPY it straight out without it ever being in memory.
    """
    con = connection or duck.connect(plan.storage, memory_limit=memory_limit)
    return con.sql(build_sql(plan))


def max_cutoff(plan: Plan) -> date:
    """The latest moment any spine row is allowed to know something.

    Used to bound what the watermark is withholding: a fact that became
    knowable after every cutoff was never going to be visible anyway, so
    counting it would report a problem that does not exist.
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
    import calendar

    duration = parse_duration(offset)
    index = day.month - 1 - duration.months
    year = day.year + index // 12
    month = index % 12 + 1
    shifted = date(year, month, min(day.day, calendar.monthrange(year, month)[1]))
    if duration.days:
        shifted = date.fromordinal(shifted.toordinal() - duration.days)
    return shifted


def withheld_by_watermark(plan: Plan) -> dict[str, Any]:
    """What a pinned watermark is currently excluding, and from where.

    These are the facts that were genuinely knowable by the composition's latest
    cutoff but arrived here afterwards. Both conditions matter: something
    ingested late that only became knowable later was never in scope, and
    reporting it would raise an alarm about nothing.

    Pinning silently withholds data, and the silence is the danger — a pinned
    version resolving against a store that has since learned more is how a
    backtest stays reproducible and wrong at the same time.
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
    """Pin a composition so it resolves to the same data forever.

    ``query`` stores the spec and the watermark and copies no bytes; the answer
    is recomputed on demand and cannot drift, because both time axes are pinned.
    ``materialized`` additionally writes the parquet.

    Both run the *same* ``resolve``. The materialised form is a cache of the
    query form, and ``test_materialized_equals_query`` is what keeps that true
    rather than merely intended.
    """
    from feature_store.spec.canonical import content_hash, to_jsonable
    from feature_store.store import objects
    from feature_store.store.runs import run as open_run
    from feature_store.store.tables import dataset, dataset_version

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
            import tempfile
            from pathlib import Path

            from feature_store.spec.canonical import hash_file

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
            # Run it once anyway. A frozen spec that does not execute is a
            # promise nobody checked, and the failure would surface much later
            # to whoever trusted the pin.
            rows = con.execute(f"SELECT count(*) FROM ({relation.sql_query()})").fetchone()[0]

        # The spec travels next to the bytes as well as into the catalog, so a
        # frozen output stays interpretable if the catalog is ever lost.
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
    from feature_store.store.tables import dataset, dataset_version

    query = (
        sa.select(dataset_version)
        .join(dataset, dataset.c.id == dataset_version.c.dataset_id)
        .where(dataset.c.name == name)
        .order_by(dataset_version.c.version_num.desc())
    )
    if version is not None:
        query = query.where(dataset_version.c.version_num == version)
    row = connection.execute(query).mappings().first()
    if row is None:
        available = connection.execute(
            sa.select(dataset_version.c.version_num)
            .join(dataset, dataset.c.id == dataset_version.c.dataset_id)
            .where(dataset.c.name == name)
            .order_by(dataset_version.c.version_num)
        ).scalars().all()
        raise ComposeError(
            f"no dataset {name!r}"
            + (f" at version {version}. Available: {available}" if available else "")
        )
    return dict(row)


def read(
    engine: sa.Engine,
    settings: Settings,
    name: str,
    version: int | None = None,
    *,
    latest: bool = False,
    memory_limit: str = "4GB",
):
    """Resolve a frozen version, and report what its watermark is withholding.

    Returns the relation and a warning dictionary. Pinning silently withholds
    data, and silence is the problem: a pinned version resolving against a store
    that has since learned more is how a backtest stays reproducible and wrong at
    the same time. ``latest=True`` ignores the watermark, which answers "what
    have we learned since" without freezing anything.
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
