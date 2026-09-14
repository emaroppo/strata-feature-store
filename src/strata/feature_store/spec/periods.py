"""Period arithmetic: what date an observation is stamped with, and whether it belongs.

A source publishes an annual figure for 2024. That fact has to become a single
``event_time``, and which date it becomes decides what every later as-of query
believes about when it was knowable. So the choice is recorded on the group
rather than left to whoever wrote the config.

**The anchor defaults to the end of the period, and that is a correctness
choice.** An annual 2024 figure anchored at 1 January with an eighteen-month
lag claims it was knowable on 2025-07-01; anchored at 31 December it claims
2026-06-30. The first is optimistic — it hands a backtest data it could not
have had. The second is pessimistic: it withholds data that was in fact
available, leaving results conservative rather than contaminated. Between an
error that invalidates prior work and one that merely understates it, the
default belongs on the second.
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

#: Periods with no boundary to check. A country-date panel emits a row whenever
#: something changed, so any date is legitimate and asserting otherwise would
#: reject the data for being shaped the way it is.
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
    """Snap *day* to the *anchor* end of its *period*.

    Used at standardise time to turn "2024" or "2024-Q3" into the single date
    the fact is stored under.
    """
    if anchor not in ANCHORS:
        raise ValueError(f"anchor must be 'start' or 'end', not {anchor!r}")
    start, end = period_bounds(day, period)
    return start if anchor == "start" else end


def on_boundary(day: date, period: Period, anchor: Anchor = "end") -> bool:
    """Whether *day* already sits on the *anchor* boundary of its *period*.

    This is invariant 11. An annual group whose event times are not year ends
    means the anchor was declared wrong, and a wrong anchor shifts every
    derived knowledge_time by up to a year — quietly, and in the direction that
    invalidates results if it shifts them earlier.
    """
    if period in UNBOUNDED:
        return True
    return day == anchor_date(day, period, anchor)


def from_year(year: int, anchor: Anchor = "end") -> date:
    """The date an annual observation for *year* is stamped with."""
    return anchor_date(date(year, 1, 1), "annual", anchor)
