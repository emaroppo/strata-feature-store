"""A small synthetic panel, and the machinery to build a store from part of it.

The panel is deliberately awkward. A well-behaved fixture — one value per
entity-period, published on time, never revised — cannot fail the property
these tests check, because there is nothing for a leak to leak. So this one has
revisions arriving years late, features on different publication schedules,
coverage gaps, and a value published before its own event date.

Everything is generated deterministically. A property test that cannot be
reproduced from its seed is a property test that reports failures nobody can
investigate.
"""

from __future__ import annotations

import tempfile
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import sqlalchemy as sa

from strata.feature_store.spec.canonical import hash_file
from strata.feature_store.store import catalog, duck
from strata.feature_store.store.runs import run as open_run
from strata.feature_store.store.tables import feature_group, feature_partition

NAMESPACE = "iso3166-1-alpha3"
ENTITIES = ("AAA", "BBB", "CCC")


@dataclass(frozen=True, slots=True)
class Fact:
    entity_id: str
    event_time: date
    knowledge_time: datetime
    feature_name: str
    value_num: float

    def as_row(self, ingest_time: datetime) -> tuple:
        return (
            self.entity_id,
            self.event_time,
            self.knowledge_time,
            ingest_time,
            self.feature_name,
            self.value_num,
            None,
        )


def _annual(entity: str, year: int, value: float, published: datetime, name: str) -> Fact:
    return Fact(entity, date(year, 12, 31), published, name, value)


def build_facts() -> list[Fact]:
    """The panel. Every awkwardness here corresponds to a real one.

    ``revised``   an annual figure republished twice, years apart. The ordinary
                  case, and the one a join on event date gets wrong.
    ``stable``    an annual figure published once and never touched, so a leak
                  cannot hide behind a later vintage.
    ``late``      published four years after the period it describes, which is
                  what a backfilled archive looks like.
    ``sparse``    present for some entities and periods and absent for others,
                  because a leak that only shows up where data is missing is
                  still a leak.
    """
    facts: list[Fact] = []
    for index, entity in enumerate(ENTITIES):
        base = 10.0 * (index + 1)

        for year in (2020, 2021):
            # First estimate, then two revisions at widening intervals.
            facts.append(_annual(entity, year, base + 1, datetime(year + 1, 3, 1), "revised"))
            facts.append(_annual(entity, year, base + 2, datetime(year + 1, 9, 1), "revised"))
            facts.append(_annual(entity, year, base + 3, datetime(year + 3, 1, 15), "revised"))

            # Published once, on a different schedule from `revised`.
            facts.append(_annual(entity, year, base + 4, datetime(year + 1, 6, 30), "stable"))

            # A backfilled archive: the period is old, the publication is not.
            facts.append(_annual(entity, year, base + 5, datetime(2025, 2, 1), "late"))

        # Gaps: only the first entity has 2020, only the last has 2021.
        if entity == ENTITIES[0]:
            facts.append(_annual(entity, 2020, base + 6, datetime(2021, 4, 1), "sparse"))
        if entity == ENTITIES[-1]:
            facts.append(_annual(entity, 2021, base + 7, datetime(2022, 4, 1), "sparse"))
    return facts


FACTS = build_facts()
FEATURES = sorted({f.feature_name for f in FACTS})


def knowledge_times() -> list[datetime]:
    return sorted({f.knowledge_time for f in FACTS})


def cutoffs() -> list[datetime]:
    """Every cutoff worth testing: each publication moment, and either side of it.

    Boundaries are where an inclusive comparison turns into an exclusive one, so
    a test that only samples the interior would pass with ``<`` where ``<=`` was
    meant — an off-by-one that hides a whole vintage.
    """
    out: list[datetime] = [datetime(2019, 1, 1)]  # before anything was published
    for moment in knowledge_times():
        out.append(moment - timedelta_us())
        out.append(moment)
        out.append(moment + timedelta_us())
    out.append(datetime(2030, 1, 1))  # after everything
    return sorted(set(out))


def timedelta_us():
    from datetime import timedelta

    return timedelta(microseconds=1)


def build_store(
    engine,
    settings,
    s3,
    facts: list[Fact],
    *,
    group_name: str | None = None,
    ingest_time: datetime | None = None,
    time_basis: str = "publisher_timestamp",
    declared_lag: str | None = None,
    lag_anchor_period: str | None = None,
    vocabulary: list[str] | None = None,
) -> str:
    """Write *facts* as a real feature group: parquet in the bucket, rows in the catalog.

    A genuinely separate store, not a filtered view. That is the whole point of
    the truncation property: if the comparison were a predicate applied to one
    store, it would only test the predicate. Writing the truncated data as its
    own files means any path that reads something it should not — the join, a
    carry-forward, a row-group statistic, a future addition — produces a
    different answer on one side than the other.
    """
    name = group_name or f"pit_{uuid.uuid4().hex[:10]}"
    ingest_time = ingest_time or datetime(2026, 1, 1)

    work = Path(tempfile.mkdtemp())
    parquet = work / "part.parquet"
    con = duck.connect()
    con.execute(
        "CREATE TABLE f (entity_id VARCHAR, event_time DATE, knowledge_time TIMESTAMP, "
        "ingest_time TIMESTAMP, feature_name VARCHAR, value_num DOUBLE, value_str VARCHAR)"
    )
    rows = [f.as_row(ingest_time) for f in facts]
    for row in rows:
        con.execute("INSERT INTO f VALUES (?, ?, ?, ?, ?, ?, ?)", row)
    if declared_lag:
        # A modeled_lag group leaves the column null and derives it at read.
        con.execute("UPDATE f SET knowledge_time = NULL")
    con.execute(
        f"COPY (SELECT * FROM f ORDER BY feature_name, entity_id, event_time) "
        f"TO '{parquet}' (FORMAT PARQUET)"
    )
    digest, size = hash_file(str(parquet))
    key = f"features/{name}/entity_namespace={NAMESPACE}/part-{digest[:16]}.parquet"
    s3.upload_file(Filename=str(parquet), Bucket=settings.storage.bucket, Key=key)

    with open_run(engine, "standardise", f"pit fixture {name}", {}) as current:
        catalog.ensure_namespace(current, NAMESPACE)
        source_name = f"src_{name}"
        with current.transaction() as connection:
            existing = catalog.get_source(connection, source_name)
        if existing is None:
            catalog.add_source(
                current, name=source_name, native_namespace=NAMESPACE, granularity="annual"
            )
        with current.transaction() as connection:
            source = catalog.get_source(connection, source_name)
            group_id = connection.execute(
                sa.insert(feature_group)
                .values(
                    name=name,
                    source_id=source["id"],
                    entity_namespace=NAMESPACE,
                    time_basis=time_basis,
                    declared_lag=declared_lag,
                    lag_anchor_period=lag_anchor_period,
                    storage_prefix=f"features/{name}",
                    created_by_run_id=current.id,
                )
                .returning(feature_group.c.id)
            ).scalar_one()
            partition_id = connection.execute(
                sa.insert(feature_partition)
                .values(
                    feature_group_id=group_id,
                    object_key=key,
                    content_sha256=digest,
                    size_bytes=size,
                    row_count=len(rows),
                    ingest_time=ingest_time,
                    max_knowledge_time=(
                        None if declared_lag else max(f.knowledge_time for f in facts)
                    ),
                    created_by_run_id=current.id,
                )
                .returning(feature_partition.c.id)
            ).scalar_one()
            # The vocabulary registry, which a real group always has because
            # standardise writes it.
            #
            # Registered from the *vocabulary*, not from the facts that happen
            # to be present. Standardise records a column whether or not this
            # release had any non-null values in it, so a group knows a feature
            # exists before it holds any. That distinction is what lets a typo
            # in a spec fail while a feature with nothing published yet
            # correctly resolves to null.
            from strata.feature_store.store.tables import feature as feature_table

            connection.execute(
                sa.insert(feature_table),
                [
                    {
                        "feature_group_id": group_id,
                        "name": feature_name,
                        "dtype": "num",
                        "created_by_run_id": current.id,
                    }
                    for feature_name in (
                        vocabulary
                        if vocabulary is not None
                        else sorted({f.feature_name for f in facts})
                    )
                ],
            )
        current.wrote("feature_partition", partition_id)
        if not declared_lag:
            current.observed_knowledge_time(max(f.knowledge_time for f in facts))
    return name


def spec_for(group: str, cutoff: datetime, *, name: str | None = None, max_age: str | None = None):
    """One spec, resolved at a fixed moment, selecting every feature."""
    from strata.feature_store.spec.compose import ComposeSpec

    feature = {"group": group}
    return ComposeSpec.model_validate(
        {
            "name": name or f"pit_{uuid.uuid4().hex[:8]}",
            "spine": {
                "entities": {"ids": list(ENTITIES)},
                # Spine times deliberately straddle the data: some rows have
                # nothing to resolve against, and a leak often shows up first
                # where the answer should be null.
                "times": {
                    "dates": [
                        "2019-12-31",
                        "2020-12-31",
                        "2021-12-31",
                        "2022-12-31",
                        "2025-12-31",
                    ]
                },
            },
            "features": [
                {**feature, "name": f, **({"max_age": max_age} if max_age else {})}
                for f in FEATURES
            ],
            "as_of": {"policy": "fixed", "at": cutoff.date().isoformat()},
        }
    )


def resolve_rows(engine, settings, spec) -> list[tuple]:
    from strata.feature_store.store import compose

    with engine.connect() as connection:
        plan = compose.plan(connection, spec, settings)
    rows = compose.resolve(plan).fetchall()
    # Drop the spine/cutoff columns, which name the spec rather than the data,
    # and sort so two stores are compared on content alone.
    return sorted((r[0], str(r[1]), r[3], r[4], str(r[6]), r[7]) for r in rows)
