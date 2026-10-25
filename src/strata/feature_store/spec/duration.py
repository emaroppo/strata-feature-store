"""ISO-8601 durations, restricted to the calendar parts.

Months and days are kept apart and never converted into one another, because a
month is not a fixed number of days and folding them would drift at boundaries.
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
    """Parse an ISO-8601 calendar duration such as ``P18M``.

    Refuses time components such as ``PT1H``.
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
    """Add *duration* to *start*: months first, clamped to the month's last day, then days."""
    month_index = start.month - 1 + duration.months
    year = start.year + month_index // 12
    month = month_index % 12 + 1
    day = min(start.day, calendar.monthrange(year, month)[1])
    shifted = date(year, month, day)
    if duration.days:
        shifted = date.fromordinal(shifted.toordinal() + duration.days)
    return shifted
