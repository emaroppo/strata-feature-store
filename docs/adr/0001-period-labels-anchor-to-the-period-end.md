# 1. A bare period label anchors to the end of its period

**Status:** accepted
**Context:** ingesting a panel whose columns are years

---

## The decision

When a source labels an observation with a period rather than a date — a column
headed `2018`, a cell reading `2024-Q3` — the stored `event_time` is the **last
day of that period**. `2018` becomes `2018-12-31`, not `2018-01-01`.

`feature_group.period_anchor` records which end was used, so a reader never has
to infer it, and a group may declare `start` where a publisher genuinely means
the beginning of a period.

## Why the end rather than the beginning

Because the two choices are not symmetric in what they cost when wrong.

An annual figure for 2018 becomes knowable at some point after 2018 is over.
Under a modelled lag, the derived knowledge time is the anchor plus the lag, so
the anchor decides what a backtest is allowed to see:

| anchor | event_time | + a 3-month lag | claim |
|---|---|---|---|
| start | 2018-01-01 | 2018-04-01 | the 2018 figure was knowable in April 2018 |
| end | 2018-12-31 | 2019-03-31 | the 2018 figure was knowable in March 2019 |

The first is false, and false in the dangerous direction: it hands a model data
that did not exist, and every result computed from it is inflated by an amount
nobody can measure afterwards. The second may be pessimistic — the figure might
have been available in February — and being pessimistic withholds something that
was there, leaving results conservative rather than contaminated.

Between an error that invalidates prior work and one that understates it, the
default belongs on the second. That asymmetry is the whole argument; the rest is
bookkeeping.

## What it rules out

**Anchoring to the middle.** Defensible for a flow measured over a period, and
rejected because it is neither honest about availability nor a real date the
publisher would recognise.

**Inferring the anchor per source.** A publisher that stamps observations with
period starts is real, and `period_anchor` exists for it — but the *default* is
not inferred, because an inference that is right nine times and silently wrong
the tenth is worse than a rule.

## Consequences

- A source with year columns needs no per-row date and still gets a real one.
- Invariant 11 checks that stored event times actually fall on the declared
  boundary, which is what catches an anchor declared wrong rather than merely
  chosen badly.
- A flow that genuinely occurred at the start of its period is stamped at the
  end, and `event_time` is therefore a label for the period rather than a claim
  about when within it something happened. Anything needing intra-period timing
  needs a finer granularity, not a different anchor.
