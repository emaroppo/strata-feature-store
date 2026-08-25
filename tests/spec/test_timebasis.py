"""The knowledge-time expression, asserted on the string it produces.

Asserting on the SQL text rather than only on query results is deliberate. This
expression decides what every historical query means, so a change to it should
be visible in a diff and should require someone to change a test on purpose.
"""

from __future__ import annotations

import pytest

from feature_store.spec.duration import parse_duration
from feature_store.spec.timebasis import (
    DERIVED,
    FIDELITY,
    interval_sql,
    is_upgrade,
    knowledge_time_sql,
)


@pytest.mark.parametrize("basis", ["publisher_timestamp", "file_release", "ingest_fallback"])
def test_materialised_bases_read_the_stored_column(basis):
    assert knowledge_time_sql(basis) == "knowledge_time"


def test_modeled_lag_coalesces_to_the_derived_time():
    """The exact expression the architecture doc specifies."""
    assert knowledge_time_sql("modeled_lag", parse_duration("P18M")) == (
        "COALESCE(knowledge_time, event_time + INTERVAL '18 months')"
    )


def test_the_coalesce_is_what_lets_revisions_and_backfill_share_a_group():
    """A revision carries a real release date and wins over the estimate.

    This is the whole mechanism by which a declared lag can be an estimate for
    an initial load without also being a lie about a later revision: the
    backfilled row is null and falls through, the revised row is not and does
    not.
    """
    sql = knowledge_time_sql("modeled_lag", parse_duration("P18M"))
    assert sql.startswith("COALESCE(knowledge_time,")


def test_table_qualifier_reaches_every_column():
    sql = knowledge_time_sql("modeled_lag", parse_duration("P18M"), table="f")
    assert sql == "COALESCE(f.knowledge_time, f.event_time + INTERVAL '18 months')"


def test_months_and_days_stay_separate_terms():
    """Folding them into one number would drift at month boundaries."""
    assert interval_sql(parse_duration("P1M15D")) == (
        "INTERVAL '1 months' + INTERVAL '15 days'"
    )


def test_modeled_lag_without_a_lag_is_refused():
    """There is nothing to derive, and defaulting would invent a rule."""
    with pytest.raises(ValueError, match="must declare a lag"):
        knowledge_time_sql("modeled_lag")


def test_a_lag_on_a_stored_basis_is_refused():
    """It would be silently ignored, which is worse than being rejected."""
    with pytest.raises(ValueError, match="must not declare a lag"):
        knowledge_time_sql("file_release", parse_duration("P18M"))


def test_unknown_basis_is_refused():
    with pytest.raises(ValueError, match="unknown time basis"):
        knowledge_time_sql("vibes")


class TestFidelity:
    def test_ordering_runs_worst_to_best(self):
        assert FIDELITY == (
            "ingest_fallback",
            "modeled_lag",
            "file_release",
            "publisher_timestamp",
        )

    def test_upgrades_move_forward_only(self):
        assert is_upgrade("ingest_fallback", "publisher_timestamp")
        assert is_upgrade("modeled_lag", "file_release")

    def test_downgrades_are_not_upgrades(self):
        assert not is_upgrade("publisher_timestamp", "ingest_fallback")
        assert not is_upgrade("file_release", "modeled_lag")

    def test_a_basis_is_not_an_upgrade_on_itself(self):
        for basis in FIDELITY:
            assert not is_upgrade(basis, basis)


def test_only_modeled_lag_is_derived():
    """Every other basis materialises the column, so a null there is an error."""
    assert DERIVED == frozenset({"modeled_lag"})
