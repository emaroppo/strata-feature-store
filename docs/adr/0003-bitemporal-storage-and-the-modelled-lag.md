# 3. Three timestamps, and what a modelled lag means

**Status:** accepted

## The decision

Every fact carries three times, and conflating any two is the bug this store
exists to prevent:

| column | meaning |
|---|---|
| `event_time` | when the thing happened, or the period it describes |
| `knowledge_time` | when the value became available |
| `ingest_time` | when it landed here |

A group declares how its knowledge time was determined, in increasing fidelity:
`ingest_fallback`, `modeled_lag`, `file_release`, `publisher_timestamp`.

**`modeled_lag` alone leaves the column null and derives it at read**, as
`COALESCE(knowledge_time, anchor(event_time) + declared_lag)`.

## Why derive rather than store

Because a lag is a *guess*, and guesses get corrected.

If the derived time were written into the parquet, correcting the rule would
mean rewriting every file in the group. Deriving it makes the correction a
Postgres update — and, because it produces a new `dataset_version`, an old pin
keeps resolving to what it resolved to before. A test asserts exactly this:
change the lag, the answer moves, and not one partition digest changes.

**The COALESCE is what lets a guess and a measurement share a group.** A
backfilled row is null and falls through to the estimate; a revision carries a
real publication date and wins. Without that, a group would have to be entirely
guessed or entirely measured, and a source is almost never either.

## What goes wrong under the alternatives

**Conflating knowledge time with ingest time** — stamping a 2024 archive with a
2026 download date — makes every as-of query before 2026 return nothing. It
reads as missing data, not as a mistake.

**Conflating knowledge time with event time** is worse: every backtest sees
every revision immediately, and looks better than the model is. Nothing errors.

**A null knowledge time is not neutral.** It never satisfies `kt <= cutoff`, so
a group carrying them ingests cleanly, reports its row count, and composes to an
empty panel. `ingest_fallback` did this until it was implemented, and a test now
walks every basis through the melt to make sure none can do it again.

## Consequences

- Fidelity only moves forward. Upgrading a group produces a new version with
  lineage to the old, never an in-place edit, or a frozen query version would
  start resolving to different data without its hash changing.
- `ingest_fallback` is honest about being an upper bound rather than a
  measurement, and is wrong in the direction that withholds rather than the one
  that invalidates.
