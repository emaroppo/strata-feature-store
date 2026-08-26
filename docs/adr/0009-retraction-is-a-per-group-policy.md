# 9. Whether a null withdraws a value is declared per group

**Status:** accepted

## The decision

A group declares `retraction_policy`: `honour` or `ignore`. Under `honour`, a
null value with a later knowledge time withdraws the earlier one. Under
`ignore`, it is filler and the previous figure stands.

Null-valued rows are always *stored* — the publisher emitted a null, and that is
a fact. The interpretation is applied at read, from the catalog.

## Why it cannot be inferred

Publishers mean opposite things by the same absence.

Some withdraw a figure by emitting a null: the value was published, then
retracted, and treating it as filler leaves a number in every panel that its own
publisher has disowned. Others emit nulls for "not reported this cycle": treating
those as withdrawals erases a perfectly good earlier figure on every quiet month.

The long format cannot distinguish them, and no amount of looking at the data
will: both look like a null arriving after a value. It has to be declared.

## Why at read rather than at ingest

Because the ruling gets revised, and re-standardising every release to change an
interpretation is the thing the store is built to avoid. Changing the policy is a
Postgres update, exactly as with a modelled lag.

## Consequences

- The seed panel and the World Bank panel both declare `ignore`, because their
  nulls mean "not coded" — which is also why 130M facts fit where 307M cells
  would not.
- Nothing currently writes null-valued rows, so the policy has nothing to act on
  today. That is an accident of the melt, not a guarantee, which is why the
  policy exists rather than a comment saying it does not matter yet.
