# 7. A frozen version pins the ingest watermark too

**Status:** accepted
**Corrects:** the architecture's §8, which claimed a pinned cutoff was sufficient

## The decision

Freezing a composition stores its spec, its as-of cutoff, **and** the highest
`ingest_time` visible at that moment. Resolution filters on both.

Every read of a pinned version counts the facts it is withholding — those with
`knowledge_time <= cutoff` and `ingest_time > watermark` — and says so.

## Why one axis is not enough

A pinned as-of cutoff does not make a version reproducible. A fact ingested
*later*, whose knowledge time falls *before* the cutoff, changes the answer.

That is not an edge case; it is what a bitemporal store exists to represent. A
publisher issues a correction in August for a figure that was knowable in March.
Re-resolving a version pinned in June now returns something different, and its
content hash has not changed, because the spec has not changed.

There is a test: freeze a version, inject a correction knowable before the
cutoff, and assert the pinned answer does not move.

## Why the warning is not optional

Pinning silently withholds data, and the silence is the danger.

A pinned version resolving against a store that has since learned more is how a
backtest stays reproducible and wrong at the same time. So the read reports the
count, the groups affected, and the newest excluded knowledge time, and names the
command that freezes a superseding version. `read --latest` answers "what have we
learned since" without freezing anything.

This is the one place the store's usual error direction reverses. Elsewhere,
withholding available data is the safe choice because it leaves results
conservative. Here it is safe *only while somebody knows it happened*.

## Consequences

- The withheld count is bounded by the composition's latest cutoff. A fact that
  became knowable after every cutoff was never in scope, and reporting it would
  raise an alarm about nothing.
- A spine drawing its entities from a group is reproducible for the same reason
  the values are: the watermark fixes the entity set too.
