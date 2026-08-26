"""A derived knowledge_time must mean exactly what a stored one means.

The architecture makes a specific promise about ``modeled_lag``: the column is
left null in parquet, the rule lives in the catalog, and correcting the rule is
a metadata update rather than a rewrite of every file in the group. That promise
has two halves, and both are testable.

**The derived path is not a second implementation.** A group whose knowledge
times are computed at read must answer exactly as a group whose knowledge times
were written down, over the same facts and at every cutoff. If it does not, then
the store has two notions of when something became knowable and only one of them
is right.

**Correcting the rule must not touch the bytes.** That is the entire reason for
deriving rather than materialising, and it is worth checking rather than
believing, because the cheap thing to do while fixing a lag is to rewrite the
data and move on.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

import pytest
import sqlalchemy as sa

from feature_store.store import catalog, compose
from feature_store.store.tables import feature_group, feature_partition

from . import panel

pytestmark = pytest.mark.integration

#: The publisher releases annually in March, covering the previous calendar
#: year: year-end plus this lands on 12 March of the following year.
LAG = "P2M12D"
YEARS = (2020, 2021, 2022)


def _facts(materialised: bool) -> list[panel.Fact]:
    """The same values, with knowledge times written down or left to be derived."""
    out = []
    for index, entity in enumerate(panel.ENTITIES):
        for year in YEARS:
            out.append(
                panel.Fact(
                    entity_id=entity,
                    event_time=date(year, 12, 31),
                    # Ignored entirely when the group derives its own; kept
                    # equal so the two fixtures differ in one respect only.
                    knowledge_time=datetime(year + 1, 3, 12),
                    feature_name="indicator",
                    value_num=float(index * 100 + year),
                )
            )
    return out


@pytest.fixture(scope="module")
def stored(engine, settings, s3) -> str:
    """Knowledge times materialised into the parquet."""
    return panel.build_store(
        engine, settings, s3, _facts(True), time_basis="publisher_timestamp"
    )


@pytest.fixture(scope="module")
def derived(engine, settings, s3) -> str:
    """Knowledge times left null, and resolved at read from the catalog."""
    return panel.build_store(
        engine,
        settings,
        s3,
        _facts(False),
        time_basis="modeled_lag",
        declared_lag=LAG,
        lag_anchor_period="annual",
    )


def _spec(group: str, cutoff: datetime):
    from feature_store.spec.compose import ComposeSpec

    return ComposeSpec.model_validate(
        {
            "name": f"lag_{uuid.uuid4().hex[:8]}",
            "spine": {
                "entities": {"ids": list(panel.ENTITIES)},
                "times": {"dates": [f"{y}-12-31" for y in (2020, 2021, 2022, 2023)]},
            },
            "features": [{"group": group, "name": "indicator"}],
            "as_of": {"policy": "fixed", "at": cutoff.date().isoformat()},
        }
    )


def _rows(engine, settings, group: str, cutoff: datetime):
    with engine.connect() as connection:
        plan = compose.plan(connection, _spec(group, cutoff), settings)
    return sorted(
        (r[0], str(r[1]), r[4], str(r[6])) for r in compose.resolve(plan).fetchall()
    )


CUTOFFS = [
    datetime(2020, 12, 31),  # before the first publication
    datetime(2021, 3, 11),  # the day before
    datetime(2021, 3, 12),  # exactly on it, which must count as knowable
    datetime(2021, 3, 13),
    datetime(2022, 6, 1),
    datetime(2026, 1, 1),  # after everything
]


@pytest.mark.parametrize("cutoff", CUTOFFS, ids=lambda c: c.strftime("%Y%m%d"))
def test_derived_equals_stored(engine, settings, stored, derived, cutoff):
    """Deriving a knowledge time must answer as if it had been written down."""
    assert _rows(engine, settings, derived, cutoff) == _rows(
        engine, settings, stored, cutoff
    ), f"the derived and stored groups disagree as of {cutoff}"


def test_the_derived_time_is_the_publication_date(engine, settings, derived):
    """Not merely equal to the other group — equal to the right thing.

    Both fixtures could be wrong in the same way, so the value is pinned
    independently: a 2021 figure becomes knowable on 12 March 2022.
    """
    # Values encode their own event year: index * 100 + year. So the value tells
    # you which vintage surfaced, which is what actually needs checking — the
    # spine row is the same on both sides of the boundary.
    def years_seen(cutoff):
        rows = _rows(engine, settings, derived, cutoff)
        return {int(r[2]) % 100 + 2000 for r in rows if r[2] is not None}

    assert years_seen(datetime(2022, 3, 11)) == {2020}, (
        "the day before publication, the newest knowable figure is still 2020's"
    )
    assert years_seen(datetime(2022, 3, 12)) == {2021}, (
        "on 12 March 2022 the 2021 figure is published and becomes the newest"
    )


def test_resolution_carries_the_most_recently_published_value(engine, settings, derived):
    """Which fact a spine row gets, and why it is not the matching period.

    Resolution answers "what was the latest known value at the cutoff", not
    "what was the value for the period this row is labelled with". So under a
    fixed cutoff every spine row gets the same figure — the newest one published
    by then — whatever period each row names.

    That is deliberate, and it is what makes granularity mismatch free: an
    annual indicator attached to a monthly spine needs no resampling, because
    each month carries the most recent annual figure forward until the next one
    is published. Constraining event_time to the spine row instead would leave
    eleven months of every year empty.
    """
    rows = _rows(engine, settings, derived, datetime(2022, 3, 12))
    by_entity: dict[str, set] = {}
    for entity, _spine, value, _kt in rows:
        by_entity.setdefault(entity, set()).add(value)
    assert all(len(values) == 1 for values in by_entity.values()), (
        "under a fixed cutoff, every spine row for an entity resolves alike"
    )


def test_correcting_the_lag_changes_the_answer(engine, settings, derived):
    """The rule lives in the catalog, so changing it changes what is knowable."""
    cutoff = datetime(2022, 3, 12)
    original = _rows(engine, settings, derived, cutoff)

    with engine.begin() as connection:
        connection.execute(
            sa.update(feature_group)
            .where(feature_group.c.name == derived)
            .values(declared_lag="P14M")
        )
    try:
        corrected = _rows(engine, settings, derived, cutoff)
        assert corrected != original, (
            "a longer lag must withhold something the shorter one revealed"
        )
    finally:
        with engine.begin() as connection:
            connection.execute(
                sa.update(feature_group)
                .where(feature_group.c.name == derived)
                .values(declared_lag=LAG)
            )
    assert _rows(engine, settings, derived, cutoff) == original, "and restoring it restores"


def test_correcting_the_lag_does_not_touch_the_bytes(engine, settings, derived):
    """The whole reason for deriving rather than materialising.

    If a lag correction required rewriting every parquet in the group, the
    design would have bought nothing over storing the column — and the cheap
    thing to do while fixing a lag is exactly that.
    """

    def digests() -> list[str]:
        with engine.connect() as connection:
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == derived)
            ).scalar_one()
            return [p["content_sha256"] for p in catalog.live_partitions(connection, group_id)]

    before = digests()
    with engine.begin() as connection:
        connection.execute(
            sa.update(feature_group)
            .where(feature_group.c.name == derived)
            .values(declared_lag="P24M")
        )
    try:
        _rows(engine, settings, derived, datetime(2024, 1, 1))
        assert digests() == before, "correcting a lag rewrote data it should not have"
    finally:
        with engine.begin() as connection:
            connection.execute(
                sa.update(feature_group)
                .where(feature_group.c.name == derived)
                .values(declared_lag=LAG)
            )


def test_the_stored_column_is_actually_null(engine, settings, s3, derived):
    """The claim is that nothing was written, not that it was written and ignored."""
    from feature_store.store import duck

    with engine.connect() as connection:
        group_id = connection.execute(
            sa.select(feature_group.c.id).where(feature_group.c.name == derived)
        ).scalar_one()
        keys = [p["object_key"] for p in catalog.live_partitions(connection, group_id)]
    con = duck.connect(settings.storage)
    urls = ", ".join(f"'{duck.s3_url(settings.storage.bucket, k)}'" for k in keys)
    counts = con.execute(
        f"SELECT count(*), count(knowledge_time) FROM read_parquet([{urls}])"
    ).fetchone()
    assert counts[0] > 0
    assert counts[1] == 0, "a modeled_lag group must leave the column empty"


def test_truncation_holds_for_a_derived_group(engine, settings, s3, derived):
    """The property from test_truncation, applied where kt does not exist on disk.

    Truncating a derived group means deciding which facts to keep using a
    knowledge time that is nowhere in the data — so this checks the rule and the
    read agree about what that time is.
    """
    cutoff = datetime(2022, 3, 12)
    survivors = [
        f
        for f in _facts(False)
        if datetime(f.event_time.year + 1, 3, 12) <= cutoff
    ]
    truncated = panel.build_store(
        engine,
        settings,
        s3,
        survivors,
        time_basis="modeled_lag",
        declared_lag=LAG,
        lag_anchor_period="annual",
    )
    assert _rows(engine, settings, derived, cutoff) == _rows(
        engine, settings, truncated, cutoff
    )


def test_a_partition_records_no_max_knowledge_time_when_derived(engine, derived):
    """There is no stored maximum to record, and inventing one would be a claim."""
    with engine.connect() as connection:
        group_id = connection.execute(
            sa.select(feature_group.c.id).where(feature_group.c.name == derived)
        ).scalar_one()
        values = connection.execute(
            sa.select(feature_partition.c.max_knowledge_time).where(
                feature_partition.c.feature_group_id == group_id
            )
        ).scalars().all()
    assert values and all(v is None for v in values)
