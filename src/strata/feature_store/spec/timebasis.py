"""How a group's knowledge_time was determined, and how it is resolved at read.

Four bases in increasing fidelity, and fidelity only moves forward. See ``docs/adr/0003``.
"""

from __future__ import annotations

from typing import Final, Literal

from .duration import Duration, format_duration
from .schema import EVENT_TIME, KNOWLEDGE_TIME

TimeBasis = Literal["ingest_fallback", "modeled_lag", "file_release", "publisher_timestamp"]

#: Ordered worst to best. Comparing positions is how an upgrade is checked.
FIDELITY: Final[tuple[TimeBasis, ...]] = (
    "ingest_fallback",
    "modeled_lag",
    "file_release",
    "publisher_timestamp",
)

DESCRIPTIONS: Final[dict[TimeBasis, str]] = {
    "publisher_timestamp": "the source gave a per-row availability time",
    "file_release": "the vintage or release date of the file",
    "modeled_lag": "event_time plus a declared lag; a backfill estimate",
    "ingest_fallback": "download time — an upper bound, not a measurement",
}

#: The only basis that leaves knowledge_time null in parquet and derives it at read.
DERIVED: Final[frozenset[str]] = frozenset({"modeled_lag"})


def is_upgrade(old: TimeBasis, new: TimeBasis) -> bool:
    """Whether moving from *old* to *new* increases fidelity."""
    return FIDELITY.index(new) > FIDELITY.index(old)


#: DuckDB's date_trunc unit, and how long the truncated period runs, per period.
_TRUNC: Final[dict[str, tuple[str, str]]] = {
    "annual": ("year", "INTERVAL 1 YEAR - INTERVAL 1 DAY"),
    "quarterly": ("quarter", "INTERVAL 3 MONTH - INTERVAL 1 DAY"),
    "monthly": ("month", "INTERVAL 1 MONTH - INTERVAL 1 DAY"),
    "weekly": ("week", "INTERVAL 6 DAY"),
}


def period_end_sql(column: str, period: str) -> str:
    """SQL for the last day of the *period* containing *column*."""
    if period not in _TRUNC:
        raise ValueError(
            f"cannot anchor a lag to period {period!r}; expected one of {sorted(_TRUNC)}"
        )
    unit, span = _TRUNC[period]
    return f"(date_trunc('{unit}', {column}) + {span})"


def period_start_sql(column: str, period: str) -> str:
    """SQL for the first day of the *period* containing *column*."""
    if period not in _TRUNC:
        raise ValueError(f"cannot anchor to period {period!r}; expected one of {sorted(_TRUNC)}")
    unit, _ = _TRUNC[period]
    return f"date_trunc('{unit}', {column})"


def knowledge_time_sql(
    basis: TimeBasis,
    declared_lag: Duration | None = None,
    *,
    table: str | None = None,
    lag_anchor_period: str | None = None,
) -> str:
    """The SQL expression that resolves knowledge_time for a group.

    The stored column for every basis but ``modeled_lag``, which gets a COALESCE
    over the declared lag; refuses a lag on any other basis. See ``docs/adr/0003``.
    """
    prefix = f"{table}." if table else ""
    stored = f"{prefix}{KNOWLEDGE_TIME}"
    if basis not in FIDELITY:
        raise ValueError(f"unknown time basis {basis!r}; expected one of {FIDELITY}")
    if basis not in DERIVED:
        if declared_lag is not None:
            raise ValueError(
                f"a {basis!r} group must not declare a lag: knowledge_time is stored, "
                "so a lag here would be silently ignored"
            )
        return stored
    if declared_lag is None:
        raise ValueError("a 'modeled_lag' group must declare a lag; there is nothing to derive")

    # A cyclic publisher releases a whole period at once, so the lag runs from
    # the period end rather than each event date.
    if lag_anchor_period:
        from_point = period_end_sql(f"{prefix}{EVENT_TIME}", lag_anchor_period)
    else:
        from_point = f"{prefix}{EVENT_TIME}"
    return f"COALESCE({stored}, {from_point} + {interval_sql(declared_lag)})"


def interval_sql(duration: Duration) -> str:
    """*duration* as a DuckDB interval expression, months and days as separate terms."""
    terms = []
    if duration.months:
        terms.append(f"INTERVAL '{duration.months} months'")
    if duration.days:
        terms.append(f"INTERVAL '{duration.days} days'")
    if not terms:  # pragma: no cover - Duration forbids this at construction
        raise ValueError(f"empty duration {format_duration(duration)!r}")
    return " + ".join(terms)
