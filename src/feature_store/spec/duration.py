"""ISO-8601 durations, restricted to the calendar parts.

Lags and staleness bounds are written as ``P18M``, ``P36M``, ``P2M`` rather
than as a number whose unit lives in a neighbouring field. One string carries
both, and it is the same string a human reads in a config and a reviewer reads
in a diff.

**Months and days are kept apart and never reconciled.** Eighteen months is not
548 days: added to different dates it lands on different intervals, and a lag
rule that drifts by a day or two across a decade of event times is a lag rule
that produces wrong knowledge_times at the boundaries. So a duration carries
both fields and applies months first, and no code here converts one into the
other.
"""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import date

_PATTERN = re.compile(
    r"^P(?!$)(?:(?P<years>\d+)Y)?(?:(?P<months>\d+)M)?(?:(?P<weeks>\d+)W)?(?:(?P<days>\d+)D)?$"
)


@dataclass(frozen=True, slots=True)
class Duration:
    """A calendar duration, as a number of months and a number of days."""

    months: int = 0
    days: int = 0

    def __post_init__(self) -> None:
        if self.months < 0 or self.days < 0:
            raise ValueError("durations here are non-negative; a lag never runs backwards")
        if self.months == 0 and self.days == 0:
            raise ValueError("a zero duration is almost always a config mistake; omit the field")

    def __str__(self) -> str:
        return format_duration(self)


def parse_duration(text: str) -> Duration:
    """Parse an ISO-8601 duration such as ``P18M`` or ``P2Y6M``.

    Time components (``PT1H``) are rejected rather than ignored. Nothing in
    this domain has a sub-day lag, and silently dropping a component the author
    meant is worse than refusing it.
    """
    if not isinstance(text, str):
        raise TypeError(f"duration must be a string, not {type(text).__name__}")
    match = _PATTERN.match(text)
    if match is None:
        raise ValueError(
            f"{text!r} is not an ISO-8601 calendar duration. "
            "Expected forms like 'P18M', 'P3Y', 'P2W', 'P1Y6M'."
        )
    parts = {k: int(v) if v else 0 for k, v in match.groupdict().items()}
    return Duration(
        months=parts["years"] * 12 + parts["months"],
        days=parts["weeks"] * 7 + parts["days"],
    )


def format_duration(duration: Duration) -> str:
    """Render back to ISO-8601. ``parse(format(d)) == d`` for every valid *d*."""
    out = "P"
    years, months = divmod(duration.months, 12)
    if years:
        out += f"{years}Y"
    if months:
        out += f"{months}M"
    if duration.days:
        out += f"{duration.days}D"
    return out


def add_duration(start: date, duration: Duration) -> date:
    """Add *duration* to *start*, months first, then days.

    Months-first is the convention every calendar library uses and the one a
    config author assumes. It matters at month ends: adding one month to
    31 January gives 28 February, and adding the day afterwards gives 1 March,
    where the other order would give 3 March.

    Overflowing a short month clamps to its last day rather than spilling into
    the next one, so ``31 January + P1M`` is 28 February and not 3 March.
    """
    month_index = start.month - 1 + duration.months
    year = start.year + month_index // 12
    month = month_index % 12 + 1
    day = min(start.day, calendar.monthrange(year, month)[1])
    shifted = date(year, month, day)
    if duration.days:
        shifted = date.fromordinal(shifted.toordinal() + duration.days)
    return shifted
