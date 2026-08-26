# 11. Enum-like columns are TEXT with a CHECK

**Status:** accepted

## The decision

`kind`, `status`, `time_basis`, `resolution_mode`, `granularity` and the rest are
`TEXT` columns with a `CHECK` constraint, not native Postgres enums.

## Why

Widening a `CHECK` is a cheap migration. Widening a native enum is a
transactional one with its own rules about when new values become visible, and
these vocabularies are expected to widen: `dataset.kind` gained `crosswalk`,
`run.kind` has gained a value at nearly every milestone.

## The cost, which is real

**Alembic does not autogenerate CHECK constraint changes.** Adding a value to one
of these lists produces an *empty* migration, and the failure surfaces later as a
constraint violation on an insert the model says is fine.

So every vocabulary change needs its migration written by hand. And when writing
it, pass the **bare** constraint name: `MetaData`'s naming convention formats it
as `ck_%(table_name)s_%(constraint_name)s`, so passing the full name asks to drop
one that was never created.

Both of those cost time the first time. They are recorded here and in `TODO.md`
because the trade is still right — but only if the cost is known rather than
rediscovered.

## Consequences

- The constraint name appears in the error, which makes a violation
  self-explanatory: `ck_source_granularity` says exactly what was wrong.
- Tests assert on those names, so a rename shows up as a test failure rather than
  as a migration that silently drops nothing.
