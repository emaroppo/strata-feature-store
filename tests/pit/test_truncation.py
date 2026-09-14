"""The truncation property: the test that catches lookahead anywhere.

    For any spec S and cutoff T, resolving S as of T over the full store must
    equal resolving S as of T over a store from which every fact with
    knowledge_time > T has been physically deleted.

Every other test here guards a leak somebody thought of: the ASOF join keying,
the staleness bound, the watermark. This one guards the ones nobody thought of.
It does not know or care how the answer is produced, so any path that reads a
row it should not — a carry-forward, a row-group statistic, a groupby computing
a max over a whole partition, an optimisation added next year — produces a
different answer on one side than the other and the test goes red.

The truncated side is a **real store**, written as its own parquet and
registered as its own group. Filtering one store with a predicate would only
test the predicate.

This is the most valuable test in the repository and also the slowest, which is
a bad combination: a slow test gets marked slow, then skipped, then deleted. The
panel is kept to three entities, four features and two years so the whole battery
runs in seconds.
"""

from __future__ import annotations

from datetime import datetime
from itertools import pairwise

import pytest

from . import panel

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def full_store(engine, settings, s3) -> str:
    """Every fact, as one registered group."""
    return panel.build_store(engine, settings, s3, panel.FACTS)


def truncated_store(engine, settings, s3, cutoff: datetime) -> str:
    """The same panel with everything published after *cutoff* physically absent."""
    survivors = [f for f in panel.FACTS if f.knowledge_time <= cutoff]
    if not survivors:
        return ""
    # The full vocabulary, not just the features that survived. A store that
    # has not yet received a value for a feature still knows the feature
    # exists, and truncation is about facts rather than about the schema.
    return panel.build_store(engine, settings, s3, survivors, vocabulary=panel.FEATURES)


#: Each publication moment and both sides of it, plus one cutoff before anything
#: existed and one after everything did.
CUTOFFS = panel.cutoffs()


@pytest.mark.parametrize("cutoff", CUTOFFS, ids=lambda c: c.strftime("%Y%m%dT%H%M%S%f"))
def test_truncation_property(engine, settings, s3, full_store, cutoff):
    """Resolving as of T must not depend on what the store learned after T."""
    truncated = truncated_store(engine, settings, s3, cutoff)

    from_full = panel.resolve_rows(engine, settings, panel.spec_for(full_store, cutoff))

    if not truncated:
        # Nothing was knowable yet, so every cell must be empty. An empty store
        # cannot be built as a group, and the assertion is stronger anyway.
        assert all(row[3] is None for row in from_full), (
            "a cutoff before the first publication must resolve to nothing"
        )
        return

    from_truncated = panel.resolve_rows(engine, settings, panel.spec_for(truncated, cutoff))
    assert from_full == from_truncated, (
        f"as of {cutoff}, the full store and a store without later facts disagree. "
        "Something read a value that was not yet knowable."
    )


@pytest.mark.parametrize("cutoff", CUTOFFS[::3], ids=lambda c: c.strftime("%Y%m%d"))
def test_truncation_holds_with_staleness_bounds(engine, settings, s3, full_store, cutoff):
    """The property must survive `max_age`, which nulls values after the join.

    A staleness bound is applied to the resolved answer, so a bug there could
    null different rows on the two sides while every value it kept was correct.
    """
    truncated = truncated_store(engine, settings, s3, cutoff)
    if not truncated:
        return
    left = panel.resolve_rows(engine, settings, panel.spec_for(full_store, cutoff, max_age="P18M"))
    right = panel.resolve_rows(engine, settings, panel.spec_for(truncated, cutoff, max_age="P18M"))
    assert left == right


def test_the_property_can_fail(engine, settings, s3, full_store):
    """The test that tests the test.

    A property test that cannot fail is worse than no test: it reports success
    forever and nobody looks again. This resolves the full store at one cutoff
    and the truncated store at a *later* one, which is exactly the lookahead the
    property forbids, and asserts they disagree. If this ever passes, the
    comparison above has stopped comparing anything.
    """
    early = datetime(2021, 4, 1)
    late = datetime(2024, 1, 1)
    truncated = truncated_store(engine, settings, s3, early)

    honest = panel.resolve_rows(engine, settings, panel.spec_for(truncated, early))
    leaked = panel.resolve_rows(engine, settings, panel.spec_for(full_store, late))
    assert honest != leaked, (
        "resolving at a later cutoff returned the same answer as an earlier one; "
        "the fixture has no revisions and the property is vacuous"
    )


def test_a_later_cutoff_sees_at_least_as_much(engine, settings, s3, full_store):
    """Monotonic restriction: knowledge accumulates, it does not evaporate.

    For T1 < T2, a value present at T1 must either be identical at T2 or have
    been superseded by something published in between. There is no third case,
    and a carry-forward or staleness bug is the usual way one appears.
    """
    moments = [datetime(2021, 6, 1), datetime(2022, 6, 1), datetime(2026, 1, 1)]
    seen = [
        {
            (r[0], r[1], r[2]): r[3]
            for r in panel.resolve_rows(engine, settings, panel.spec_for(full_store, t))
        }
        for t in moments
    ]
    for (earlier, later), (t1, t2) in zip(pairwise(seen), pairwise(moments), strict=True):
        for key, value in earlier.items():
            if value is None:
                continue
            assert later[key] is not None, (
                f"{key} had a value as of {t1} and none as of {t2}; knowledge does not disappear"
            )


def test_nothing_is_knowable_before_the_first_publication(engine, settings, s3, full_store):
    rows = panel.resolve_rows(engine, settings, panel.spec_for(full_store, datetime(2019, 6, 1)))
    assert rows, "the spine should still produce rows"
    assert all(r[3] is None for r in rows), "with nothing resolved in any of them"


def test_a_backfilled_archive_is_not_visible_early(engine, settings, s3, full_store):
    """`late` describes 2020 and 2021 but was not published until 2025.

    An archive loaded years after the fact is the case where event time and
    publication time diverge most, and where a join on event date is most
    confidently wrong.
    """
    before = {
        (r[0], r[1], r[2]): r[3]
        for r in panel.resolve_rows(
            engine, settings, panel.spec_for(full_store, datetime(2024, 12, 31))
        )
    }
    after = {
        (r[0], r[1], r[2]): r[3]
        for r in panel.resolve_rows(
            engine, settings, panel.spec_for(full_store, datetime(2025, 3, 1))
        )
    }
    late_before = [v for (_, _, f), v in before.items() if f == "late"]
    late_after = [v for (_, _, f), v in after.items() if f == "late"]
    assert set(late_before) == {None}, "not knowable in 2024"
    assert any(v is not None for v in late_after), "knowable once published in 2025"
