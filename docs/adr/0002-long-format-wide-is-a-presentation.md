# 2. Facts are stored long; wide is a presentation

**Status:** accepted

## The decision

Every feature parquet has the same seven columns: entity, event time, knowledge
time, ingest time, feature name, and a numeric or string value. One row is one
fact. A wide table — observations as rows, features as columns — exists only as
an output orientation, produced by pivoting at read.

## Why

**A knowledge time attaches to a fact, not to a row.** That is the whole
argument. A wide layout forces every column in a row to share one revision
cadence, and that is simply false: a GDP figure is revised for years, a
population count is not, and an election date never is. Storing them in one row
means either a knowledge time per column — at which point the row is long with
extra steps — or one per row, which is a lie about most of the columns.

Three consequences follow, and they are the reason this is cheap rather than
merely correct:

- Adding a feature is an append. In wide it is a schema migration across every
  file ever registered.
- Combining sources is `UNION ALL`. In wide it is an N-way join with column
  collisions to reconcile.
- Nulls cost nothing. A wide file is mostly empty — the seed panel is about half
  — and long format simply does not store the gaps. The World Bank panel is
  7.5M facts out of 26M cells; the seed panel 130M out of 307M.

## What it rules out

**Wide as a registered layout.** Nothing writes wide parquet under `features/`.
A composition may emit it, and that output is a *presentation of the same
samples*: `n` observations by `f` features holds `n × f` samples either way.

## Consequences

- A query for three features out of four thousand reads almost nothing, because
  files are sorted by feature name and each part holds a disjoint range.
- The melt is the expensive step, and it happens once per release rather than on
  every read.
- Anything that is not a fact about an entity at a time has no home here.
  Embeddings, media, per-observation footnotes: all need a different shape, and
  saying so is easier than discovering it later.
