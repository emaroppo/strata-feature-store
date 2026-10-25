"""Period arithmetic: what date an observation is stamped with, and whether it belongs.

The anchor defaults to the end of the period. See ``docs/adr/0001``.
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Final, Literal

Period = Literal["annual", "quarterly", "monthly", "weekly", "daily", "irregular"]
Anchor = Literal["start", "end"]

PERIODS: Final[tuple[Period, ...]] = (
    "annual",
    "quarterly",
    "monthly",
    "weekly",
    "daily",
    "irregular",
)

ANCHORS: Final[tuple[Anchor, ...]] = ("start", "end")

#: Periods with no boundary to check: any date is legitimate.
UNBOUNDED: Final[frozenset[str]] = frozenset({"irregular", "daily"})


def period_bounds(day: date, period: Period) -> tuple[date, date]:
    """The first and last day of the *period* containing *day*."""
    if period == "annual":
        return date(day.year, 1, 1), date(day.year, 12, 31)
    if period == "quarterly":
        first_month = 3 * ((day.month - 1) // 3) + 1
        last_month = first_month + 2
        return (
            date(day.year, first_month, 1),
            date(day.year, last_month, calendar.monthrange(day.year, last_month)[1]),
        )
    if period == "monthly":
        return date(day.year, day.month, 1), date(
            day.year, day.month, calendar.monthrange(day.year, day.month)[1]
        )
    if period == "weekly":
        # ISO weeks: Monday starts, Sunday ends. weekday() is 0 on Monday.
        start = day - timedelta(days=day.weekday())
        return start, start + timedelta(days=6)
    if period in UNBOUNDED:
        return day, day
    raise ValueError(f"unknown period {period!r}; expected one of {PERIODS}")


def anchor_date(day: date, period: Period, anchor: Anchor = "end") -> date:
    """Snap *day* to the *anchor* end of its *period*."""
    if anchor not in ANCHORS:
        raise ValueError(f"anchor must be 'start' or 'end', not {anchor!r}")
    start, end = period_bounds(day, period)
    return start if anchor == "start" else end


def on_boundary(day: date, period: Period, anchor: Anchor = "end") -> bool:
    """Whether *day* sits on the *anchor* boundary of its *period*; always true when unbounded.

    This is invariant 11. See ``docs/adr/0001``.
    """
    if period in UNBOUNDED:
        return True
    return day == anchor_date(day, period, anchor)


def from_year(year: int, anchor: Anchor = "end") -> date:
    """The date an annual observation for *year* is stamped with."""
    return anchor_date(date(year, 1, 1), "annual", anchor)
