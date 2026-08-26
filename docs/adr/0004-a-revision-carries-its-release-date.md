# 4. A revision carries the date of the release that changed it

**Status:** accepted

## The decision

Standardising a subsequent release is differential. A value that has not changed
appends nothing. **A value that has changed appends one row whose
`knowledge_time` is that release's date** — not the lag-derived time the group
otherwise uses.

## Why

A modelled lag is a backfill estimate for an initial load, where nothing records
when each figure became knowable. It is not an estimate for a revision, because
a revision has a real date: the release that carried it.

This is what makes `modeled_lag` coherent rather than a convenient fiction. The
same group can hold guessed times for facts nobody recorded and measured times
for every change since, and the COALESCE resolves both without a schema change.

## What it dissolves

Two releases can no longer produce the same
`(entity, event_time, feature_name, knowledge_time)` with different values,
because the second one's knowledge time is its own release date. The duplicate
problem does not need a tie-break rule; it stops existing.

That matters most for `modeled_lag` groups, where the derived time is a function
of the event date alone and therefore carries no information about which release
a fact came from. Without this rule, every release of such a source would
collide with every other on every overlapping period.

## Cost

Standardisation is no longer a pure file-to-parquet transform: it reads the
group's current state to know what changed. At the volumes here that is one
DuckDB scan.

**It does not apply to high-volume sources.** Those are downloaded per period
with no overlap, so there is nothing to diff, and a correction to an already
ingested period would go unnoticed. That case is unsolved and recorded as such.
