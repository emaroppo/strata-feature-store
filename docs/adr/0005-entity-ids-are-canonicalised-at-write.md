# 5. Entity ids are canonicalised at write, not resolved at read

**Status:** accepted
**Supersedes:** the architecture's original §6, which resolved crosswalks at composition

## The decision

Each source declares the vocabulary it publishes in. Standardisation converts to
the store's single canonical vocabulary, and **the stored parquet is always
canonical**. The crosswalk version used is recorded as a lineage input.

## Why this is more pinned, not less

The obvious objection is that baking a mapping into the bytes is less flexible
than resolving it per query. The opposite is true for reproducibility: the
mapping is frozen with its version recorded, and improving a crosswalk means
re-standardising from the retained release into a new partition that supersedes
the old — the same new-version-never-an-edit discipline used everywhere else.

Resolving at read means every historical query silently changes meaning the day
somebody improves the mapping. That is precisely the failure pinning exists to
prevent.

## What it buys

Composition has **no cross-namespace join at all**. Every group speaks one
vocabulary, so combining sources is a `UNION ALL` and a spine join. The seed
panel and the World Bank panel compose together in 0.11 seconds despite sharing
no vocabulary, cadence or publication schedule, because the reconciliation
already happened at ingest.

The namespace guard moves earlier as a result: standardising a non-canonical
source without a pinned crosswalk is an error **at ingest**, where it fails on
one file with an operator watching, rather than at query time as an empty result
set.

## Costs, stated plainly

- A crosswalk bug corrupts stored facts rather than one query's output. The
  retained release is what makes that recoverable, and is the second thing
  keeping raw bytes earns.
- The canonical vocabulary is configuration, not a constant. ISO3 is right for
  country panels and wrong for actors, firms or subnational units.
