# 12. Every timestamp is naive UTC

**Status:** accepted

## The decision

All timestamps are naive UTC — in Postgres (`TIMESTAMP WITHOUT TIME ZONE`), in
parquet (`timestamp[us]`), and in memory. Conversion happens once, at the edge,
in standardisation. Nothing downstream ever sees an offset.

## Why

Mixing naive and aware values inside a comparison is a silent correctness bug,
and this store is almost entirely comparisons: `kt <= cutoff` is the question it
exists to answer.

One rule makes the mixture unrepresentable. Two rules would mean every
comparison depends on both sides having been tagged correctly, everywhere,
forever — and the failure would be an as-of query quietly off by a few hours at
exactly the boundaries that matter.

The canonical serialiser refuses an aware datetime outright rather than
converting it, because a value arriving with an offset means something upstream
skipped the edge, and converting it would hide that.

## What it gives up

Publishers that state a local release time. `2025-03-12 09:00 CET` becomes
`08:00` and the local wall clock is lost.

That is acceptable because nothing here reasons about local time: a knowledge
time answers "was this available yet", which is a question about instants, not
about what a clock in Vienna read. A source that genuinely needs the local time
would need it as a separate field, not as a tagged timestamp.
