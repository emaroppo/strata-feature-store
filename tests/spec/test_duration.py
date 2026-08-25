"""Durations, and the arithmetic that decides what a lag actually means."""

from __future__ import annotations

from datetime import date

import pytest

from feature_store.spec.duration import (
    Duration,
    add_duration,
    format_duration,
    parse_duration,
)


@pytest.mark.parametrize(
    ("text", "months", "days"),
    [
        ("P18M", 18, 0),
        ("P3Y", 36, 0),
        ("P1Y6M", 18, 0),
        ("P2W", 0, 14),
        ("P7D", 0, 7),
        ("P1M15D", 1, 15),
    ],
)
def test_parses_calendar_parts(text, months, days):
    parsed = parse_duration(text)
    assert (parsed.months, parsed.days) == (months, days)


@pytest.mark.parametrize("text", ["PT1H", "P", "18M", "", "P-1M", "18", "P1H"])
def test_rejects_what_it_cannot_represent(text):
    """Time components are refused rather than dropped.

    Silently ignoring a component the author wrote is worse than refusing it:
    the config would then mean something other than it says, and nothing would
    indicate that.
    """
    with pytest.raises((ValueError, TypeError)):
        parse_duration(text)


@pytest.mark.parametrize("text", ["P18M", "P3Y", "P7D", "P1Y6M", "P1M15D"])
def test_format_round_trips_through_parse(text):
    duration = parse_duration(text)
    assert parse_duration(format_duration(duration)) == duration


def test_zero_is_refused():
    """A zero lag is a config mistake rather than an instruction."""
    with pytest.raises(ValueError):
        Duration()


def test_months_and_days_are_not_interchangeable():
    """Eighteen months is not a fixed number of days, and is not stored as one."""
    assert parse_duration("P1M") != parse_duration("P30D")


class TestTheAnchorExample:
    """The example the architecture doc turns on.

    An annual 2024 figure with an eighteen-month lag is knowable in mid-2025 if
    anchored at the start of the year, and mid-2026 if anchored at the end. The
    second withholds data that was available; the first hands a backtest data it
    could not have had. That asymmetry is why the anchor defaults to the end.
    """

    lag = staticmethod(lambda: parse_duration("P18M"))

    def test_anchored_at_period_start(self):
        assert add_duration(date(2024, 1, 1), self.lag()) == date(2025, 7, 1)

    def test_anchored_at_period_end(self):
        assert add_duration(date(2024, 12, 31), self.lag()) == date(2026, 6, 30)


def test_month_overflow_clamps_rather_than_spilling():
    """31 January plus one month is the end of February, not early March."""
    assert add_duration(date(2024, 1, 31), parse_duration("P1M")) == date(2024, 2, 29)
    assert add_duration(date(2023, 1, 31), parse_duration("P1M")) == date(2023, 2, 28)


def test_months_apply_before_days():
    """The order is load-bearing at month ends, and is the one config authors assume."""
    # months first: 31 Jan -> 29 Feb -> +1 day -> 1 Mar.
    # days first would be: 31 Jan -> 1 Feb -> +1 month -> 1 Mar. Same here, so
    # pick a case where they differ: 30 Jan + P1M1D.
    assert add_duration(date(2024, 1, 30), parse_duration("P1M1D")) == date(2024, 3, 1)


def test_leap_day_survives_a_year():
    assert add_duration(date(2024, 2, 29), parse_duration("P1Y")) == date(2025, 2, 28)
