"""Period arithmetic, and the boundary check that catches a wrong anchor."""

from __future__ import annotations

from datetime import date

import pytest

from feature_store.spec.periods import (
    PERIODS,
    anchor_date,
    from_year,
    on_boundary,
    period_bounds,
)


@pytest.mark.parametrize(
    ("day", "period", "expected"),
    [
        (date(2024, 6, 15), "annual", (date(2024, 1, 1), date(2024, 12, 31))),
        (date(2024, 6, 15), "quarterly", (date(2024, 4, 1), date(2024, 6, 30))),
        (date(2024, 1, 1), "quarterly", (date(2024, 1, 1), date(2024, 3, 31))),
        (date(2024, 11, 5), "quarterly", (date(2024, 10, 1), date(2024, 12, 31))),
        (date(2024, 2, 15), "monthly", (date(2024, 2, 1), date(2024, 2, 29))),
        (date(2023, 2, 15), "monthly", (date(2023, 2, 1), date(2023, 2, 28))),
        # 15 June 2024 is a Saturday; ISO weeks run Monday to Sunday.
        (date(2024, 6, 15), "weekly", (date(2024, 6, 10), date(2024, 6, 16))),
    ],
)
def test_period_bounds(day, period, expected):
    assert period_bounds(day, period) == expected


def test_leap_year_february_is_not_hardcoded():
    assert period_bounds(date(2024, 2, 1), "monthly")[1] == date(2024, 2, 29)
    assert period_bounds(date(2100, 2, 1), "monthly")[1] == date(2100, 2, 28)


@pytest.mark.parametrize("period", ["irregular", "daily"])
def test_unbounded_periods_are_their_own_bounds(period):
    """A country-date panel emits a row whenever something changed.

    Any date is legitimate, so asserting a boundary would reject the data for
    being shaped the way the publisher shapes it.
    """
    day = date(1789, 10, 17)
    assert period_bounds(day, period) == (day, day)
    assert on_boundary(day, period) is True


def test_anchor_selects_an_end():
    assert anchor_date(date(2024, 6, 15), "annual", "end") == date(2024, 12, 31)
    assert anchor_date(date(2024, 6, 15), "annual", "start") == date(2024, 1, 1)


def test_from_year_defaults_to_the_pessimistic_end():
    """The default is not arbitrary; see test_duration.TestTheAnchorExample."""
    assert from_year(2024) == date(2024, 12, 31)
    assert from_year(2024, "start") == date(2024, 1, 1)


class TestBoundaryCheck:
    """Invariant 11.

    An annual group whose event times are not year ends means the anchor was
    declared wrong. A wrong anchor shifts every derived knowledge_time by up to
    a year, quietly, and half the time in the direction that invalidates
    results rather than merely understating them.
    """

    def test_accepts_a_correct_anchor(self):
        assert on_boundary(date(2024, 12, 31), "annual", "end")
        assert on_boundary(date(2024, 1, 1), "annual", "start")

    def test_rejects_the_opposite_anchor(self):
        assert not on_boundary(date(2024, 1, 1), "annual", "end")
        assert not on_boundary(date(2024, 12, 31), "annual", "start")

    def test_rejects_a_date_that_is_no_boundary_at_all(self):
        assert not on_boundary(date(2024, 6, 30), "annual", "end")

    def test_quarter_end_is_not_a_year_end(self):
        assert on_boundary(date(2024, 6, 30), "quarterly", "end")
        assert not on_boundary(date(2024, 6, 30), "annual", "end")


def test_unknown_period_is_refused():
    with pytest.raises(ValueError, match="unknown period"):
        period_bounds(date(2024, 1, 1), "fortnightly")


def test_unknown_anchor_is_refused():
    with pytest.raises(ValueError, match="anchor must be"):
        anchor_date(date(2024, 1, 1), "annual", "middle")


@pytest.mark.parametrize("period", PERIODS)
def test_every_declared_period_is_implemented(period):
    """Guards against a period being added to the list and nowhere else."""
    start, end = period_bounds(date(2024, 6, 15), period)
    assert start <= date(2024, 6, 15) <= end
