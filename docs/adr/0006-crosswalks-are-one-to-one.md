# 6. Crosswalks are one-to-one, and unmapped entities are routed

**Status:** accepted

## The decision

A crosswalk maps a native id to **exactly one** canonical id, or to none. A
version containing any multi-target source id is rejected when it is registered,
not when it is used.

Entities with no canonical id are **written to a sibling group** in the source's
native vocabulary, never dropped. The run reports both counts.

## Why one-to-one

A source id with two live targets turns one fact into two. The row count grows,
every number stays plausible, and nothing reports it. That silent multiplication
is the exact failure the namespace rule exists to prevent, and permitting it here
would reintroduce it through the fix.

The one legitimate reason for a multi-target mapping is succession — a state
that split, a code reused across a boundary — and succession is deferred to a
knowledge layer that does not exist yet. Until it does, successors are separate
entities: `CSK`, `CZE` and `SVK` are three ids, not one that fans out.

Validating at registration rather than at use means a version that exists is a
version that is safe. The alternative is discovering a bad mapping halfway
through a long ingest, having already written half of it.

## Why route rather than drop

Because the unmapped rows are not errors.

Of the seed panel's 202 codes, 27 have no ISO3 — and four of those have data
through 2024: Kosovo, Somaliland, Palestine/Gaza, Zanzibar. Of the World Bank
panel's 266, 51 have none, almost all regional and income aggregates like
"Africa Eastern and Southern". Dropping them would silently lose live countries
and every aggregate the publisher computed.

The sibling group is not joinable to canonical data without a crosswalk of its
own, which the namespace guard already enforces. So the rows stay available and
stay out of any panel that did not ask for them.

## Consequences

- Two groups per source rather than one, and a composition that names the
  sibling is refused with an explanation rather than quietly comparing
  vocabularies.
- 1.5M of the World Bank panel's 9M facts live in the sibling. That is a lot of
  data that a `WHERE code IN (SELECT iso3 ...)` would have discarded without
  comment.
