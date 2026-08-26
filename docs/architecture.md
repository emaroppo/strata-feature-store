# Point-in-Time Feature Store — Architecture

**Status:** implemented and in use; this document is the design it was built against
**Scope:** offline feature storage, standardisation and retrieval for revised panel data

---

## 1. What this is

A feature store for **panel data that gets revised** — official statistics, indicators,
derived event features — where the correct answer to "what was the value of X" depends
on *when you ask*.

The central capability is point-in-time correct composition: a feature query resolved
as-of April returns what was knowable in April, not what we know now. Naive joins on
event date leak future information into training sets and silently inflate backtest
performance.

**This store does exactly three things: store, standardise, retrieve.**

Everything downstream of retrieval is out of scope and lives in a separate repository:
transforms, imputation, normalisation, rescaling, z-scores, weighting, scoring, feature
engineering. The line is that this store resolves *what a value was*; it never computes a
new value from it. Pivoting to wide, carrying a value forward to a spine row, and nulling a
stale value are resolution. Anything with a coefficient in it is not.

That boundary needs saying because it erodes by gravity. A transform registry is one
convenient afternoon away at any moment, and the day it lands here this stops being a store.

**Explicitly out of scope:** online serving / low-latency feature retrieval. There is no
serving latency requirement, and an online store is most of the complexity in Feast or
Tecton. This is an offline store only.

---

## 2. Vocabulary

Named before any table, because two neighbouring systems use several of these words to mean
other things.

| Term | Means |
|---|---|
| **sample** | one stored long-format row — one feature's value, for one entity, at one time |
| **observation** | an `(entity, event_time)` pair; a spine row |
| **source** | a publisher's product. Registered once; holds licence, URL, native vocabulary, granularity |
| **release** | one concrete file downloaded from a source, content-addressed |
| **feature_group** | samples sharing uniform knowledge-time semantics |
| **feature** | the vocabulary registry — one row per feature name |
| **dataset** / **dataset_version** | a named, frozen collection of samples |

The chain: `source` → `release` → standardise run → `feature_group` + samples → composition
→ `dataset_version`.

**Pivoting to wide does not change what a sample is.** An output covering `n` observations and
`f` features holds `n × f` samples whether emitted long or wide. Wide is a presentation.

`source` and `release` are two tables because they answer different questions. A product's
licence and URL are true forever; a downloaded file has bytes, a hash, a publication date and
a retrieval date, and there is a new one every cycle. A standardise run reads exactly one file
and its lineage must point at that file.

---

## 3. Division of responsibility

| Layer | Holds | Answers |
|---|---|---|
| **Postgres** | catalog: descriptions, pointers, lineage | *what exists, where did it come from* |
| **Parquet** | feature values, long format | *what are the values* |
| **Garage (S3)** | bytes | storage and retrieval |
| **Object key path** | hive partition values | partition pruning |

Postgres holds **zero feature values**. It stays in the low megabytes even after bulk
ingest of every indicator collection on offer.

Storage is accessed over the S3 API throughout (`boto3` for object operations, DuckDB
`httpfs` for parquet reads). Garage; the same code runs against S3, R2, or Ceph by changing
`endpoint_url`. There is deliberately **no `StorageBackend` abstraction** — one code path,
one configurable endpoint.

The store owns **its own bucket**, not a prefix inside a shared one. Garage grants access
keys per bucket, so a separate bucket is what keeps this credential away from a neighbour's
data and vice versa. Inside that bucket, prefixes rather than further buckets.

---

## 4. Catalog schema

```
entity_namespace(name PK, description, external_ref, created_at)

source(id PK, name UNIQUE, kind, native_namespace FK, granularity,
       licence, source_url, description, created_at)

release(id PK, source_id FK, sha256 UNIQUE, object_key, size_bytes,
        original_filename, media_type, source_url,
        released_at, retrieved_at, ingested_at, created_by_run_id FK, meta)

feature_group(id PK, name UNIQUE, source_id FK, entity_namespace FK,
              time_basis, declared_lag, period_anchor, retraction_policy,
              storage_prefix, partition_spec, meta, created_by_run_id FK)

feature(id PK, feature_group_id FK, name, dtype, unit, description,
        created_by_run_id FK, deprecated_at NULL,
        UNIQUE(feature_group_id, name))

feature_partition(id PK, feature_group_id FK, object_key UNIQUE, content_sha256,
                  size_bytes, row_count, partition_values,
                  min_event_time, max_event_time, max_knowledge_time,
                  ingest_time, created_by_run_id FK, superseded_by_run_id NULL)

entity_list(id PK, name UNIQUE, entity_namespace FK, description)
entity_list_version(id PK, entity_list_id FK, version_num, frozen_at, created_by_run_id FK)
entity_list_member(entity_list_version_id FK, entity_id)

dataset(id PK, name UNIQUE, kind, created_at)
dataset_version(id PK, dataset_id FK, version_num, resolution_mode,
                spec JSONB, content_hash, ingest_watermark,
                frozen_at, created_by_run_id FK)

run(id PK, kind, name, config_hash, code_git_sha, code_dirty,
    params JSONB, env JSONB, status, manifest_key, max_knowledge_time,
    started_at, finished_at)

run_input(run_id FK, ref_type, ref_id, role)
run_output(run_id FK, ref_type, ref_id, role)
```

Notes:

- `feature` is a **vocabulary registry** — one row per feature name in the group — not a
  description of physical columns. It is written by the standardise run from what it actually
  emitted, so it cannot describe features that are not there. It is a cache of what the
  parquet contains, and `verify --deep` reconciles it.
- `feature_partition` exists so **readers never glob**. A `read_parquet('features/g/**')`
  reads the orphan objects that the write order deliberately leaves behind after a failed
  run. The file list comes from the catalog instead.
- Additive-only: `deprecated_at` rather than `DELETE`; a new name rather than a rename
  or retype; `superseded_by_run_id` rather than deleting a corrected partition.
- Enum-like columns (`resolution_mode`, `time_basis`, `kind`) are `TEXT` + `CHECK`, not
  native Postgres enums — cheaper to widen later and no transactional migration issues.
- `MetaData` carries a `naming_convention` from the first migration, so autogenerated
  downgrades can name their constraints. Retrofitting it is a schema-wide rename.
- All timestamps are **naive UTC**, in Postgres and parquet alike, converted once at the edge
  in standardisation. Mixing naive and aware values inside a DuckDB comparison is a silent
  correctness bug; one rule makes it unrepresentable.
- Crosswalks need no tables of their own: a crosswalk is a `source` with its own releases,
  frozen as a `dataset_version` with `kind='crosswalk'`, whose parquet holds five columns
  rather than the seven-column long schema.

---

## 5. Physical layout — long format

Every feature parquet has the identical schema:

```
entity_id       string          (dictionary-encoded)
event_time      date32
knowledge_time  timestamp[us]   nullable
ingest_time     timestamp[us]
feature_name    string          (dictionary-encoded)
value_num       double          nullable
value_str       string          nullable
```

**Why long, not wide:**

1. `knowledge_time` attaches to a *fact*, not a row. Wide format forces every column in
   a row to share a revision cadence, which is false — GDP is revised, population is not.
2. Adding a feature is an append, not a schema migration across every registered file.
3. Combining sources becomes `UNION ALL`, not an N-way join.

**Wide is a derived form only.** Composition pivots at freeze time and writes a wide
parquet as a `materialized` dataset_version. Wide never exists as a registered layout.

**Layout rules:**

- Path: `features/{group}/entity_namespace={ns}/[year={y}/]part-{sha[0:16]}.parquet`
- The filename is a content hash, not a sequence number. A retry producing identical bytes
  writes the identical key and is a no-op; a retry producing different bytes cannot clobber
  the first. A sequence number requires knowing the highest one, which means a LIST, which
  is a race.
- Sort by `(feature_name, entity_id, event_time)` before writing — makes row-group min/max
  statistics tight on `feature_name`, so a query for 3 indicators skips the row groups
  holding the other 900.
- Partition by year only when volume justifies it. Annual country panels are a few
  hundred rows per year; don't create 60 tiny files.
- Write `feature_group`, `entity_namespace`, `time_basis`, `declared_lag`, `period_anchor`,
  `run_id` and `schema_version` into parquet key-value metadata, so a bare file stays
  interpretable if the catalog is lost or rebuilt.

**Boundary:** features are long rows; per-record dense vectors (embeddings) and media are not
features and have no home here.

---

## 6. Time semantics

Three timestamps wherever facts live:

| column | meaning |
|---|---|
| `event_time` | when the thing happened / the period it describes |
| `knowledge_time` | when the value became available |
| `ingest_time` | when it landed in our store |

`knowledge_time` is **not** file download time. Stamping 2024 archives with a 2026
download date makes every as-of query before 2026 return nothing.

`feature_group.time_basis` records how the knowledge_time was determined:

| basis | meaning |
|---|---|
| `publisher_timestamp` | source gave a per-row availability time |
| `file_release` | vintage / release date of the file |
| `modeled_lag` | `event_time + declared_lag`, rule in the catalog |
| `ingest_fallback` | download time; an upper bound, not precise |

**`modeled_lag` knowledge_time is derived, not materialized** — the column is null in
parquet and resolved at read as
`COALESCE(knowledge_time, event_time + declared_lag)`. Correcting a lag rule is then a
Postgres metadata update, not a parquet rewrite.

**`period_anchor` defaults to `end`, and this is not cosmetic.** An annual 2024 figure
anchored at 2024-01-01 with an 18-month lag claims it was knowable 2025-07-01; anchored at
2024-12-31 it claims 2026-06-30. Being pessimistic withholds data that was available, which
leaves prior results conservative. Discovering data arrived *later* than assumed is the
dangerous direction — it invalidates them. `end` is the pessimistic direction.

**A null knowledge_time has no safe default, so it has none.** In a `publisher_timestamp`
group a null is indistinguishable at read from a `modeled_lag` group's deliberate null, so
the COALESCE would silently substitute a lag that group never declared. Nulls are an error
unless the standardisation config explicitly asks for a fallback or a drop.

Fidelity is monotonic: `ingest_fallback → modeled_lag → file_release →
publisher_timestamp`, never backwards. Upgrading fidelity produces a **new**
dataset_version with lineage to the old — never an in-place edit, or frozen query
versions would silently resolve to different data.

---

## 7. Entity namespaces, and canonicalisation at write

The catalog holds the **join key**, not entity knowledge. `entity_namespace` registers
vocabularies. No entity rows, no aliases, no succession logic.

Each `source` declares the vocabulary it publishes in. **Standardisation converts to the
store's canonical namespace, and the stored parquet is always canonical.** The crosswalk
version used is recorded as a `run_input`, so the mapping behind any partition is recoverable
from lineage.

Converting at write rather than at read is *more* pinned, not less: the mapping is frozen
into the bytes with its version recorded, and improving a crosswalk means re-standardising
from the retained release into a new partition that supersedes the old — the same
new-version-never-an-edit discipline applied everywhere else. What it buys is that
composition has no cross-namespace join at all.

The namespace guard therefore fires **at ingest**: standardising a source whose native
vocabulary is not canonical, without naming a crosswalk version, is an error. That is a
better place for it than inside a join, because it fails on one file with one operator
watching.

A **crosswalk** is an ordinary source whose frozen output holds
`(source_id, target_id, valid_from, valid_to, confidence)`.

**Crosswalks are one-to-one.** A native id maps to exactly one canonical id or to none, and a
version containing any multi-target source id is rejected when it is registered rather than
when it is used. The one legitimate reason for a multi-target mapping is succession — a state
that split, a code reused across a boundary — and succession is deferred to a knowledge layer
that does not exist yet. Until it does, successors are separate entities.

**Entities with no canonical id are written to a separate parquet, never dropped.**
Publishers mix aggregates into ordinary files — world totals, currency areas, income
groupings. These are real rows with no canonical id. Standardisation splits its output:
mapped entities to the group's canonical parquet, unmapped ones to a sibling group in the
source's native namespace, ids verbatim. The run reports both counts, so a file that is one
fifth aggregates says so. The sibling is not joinable to canonical data without a crosswalk,
which the namespace guard already enforces.

The canonical namespace is *configuration*, not a constant. It is right for country panels
and wrong for actors, firms or subnational units, so it is named once in `config.toml`.

---

## 8. Releases and revisions

A release usually republishes an entire history rather than an increment, so standardising
one wholesale would append a duplicate of everything. Standardisation of a subsequent release
is therefore **differential**: it reads the group's current resolved state and appends only
what differs.

- A value that has not changed appends nothing.
- **A value that has changed appends one row whose `knowledge_time` is the release's date.**
  Not the lag-derived time — the release is direct evidence of when the new figure became
  available, which is what `knowledge_time` means.
- A period not seen before appends a row under the group's declared basis.

**This is what makes `modeled_lag` coherent.** A derived `event_time + declared_lag` is a
backfill estimate for an initial load, where nothing records when each figure became
knowable. It is not an estimate for a revision, because a revision has a real date. So
`modeled_lag` groups write null on first load and a materialised release date on every
subsequent change, and the existing COALESCE resolves both without a schema change.

It also removes duplicate knowledge-times rather than needing a rule for them: two releases
cannot produce the same `(entity, event_time, feature_name, knowledge_time)` with different
values, because the second one's knowledge_time is its own release date.

The cost is that standardisation reads existing facts to know what changed. **For
high-volume sources this approach does not apply** — those are downloaded per period with no
overlap, so there is nothing to diff, and detecting corrections in that regime is unsolved
and deferred.

**Retraction is a per-group interpretation.** Some publishers withdraw a figure by emitting a
null; others emit nulls as filler for "not coded this cycle". These are opposite meanings and
the format cannot distinguish them, so the group declares which convention its publisher
follows in `retraction_policy`. Null-valued rows are always *stored* — the publisher emitted
a null, and that is a fact. Whether a null with a later knowledge_time withdraws the earlier
value is decided at read, from the catalog, so changing the ruling is a metadata update
rather than a re-standardisation of every release.

Raw releases are **kept**. An explicit prune command exists; nothing prunes on a schedule, on
a size threshold, or as a side effect of another command. A store that silently discards its
own sources cannot answer why a number changed.

---

## 9. Composition

Engine: DuckDB with `httpfs`, reading parquet directly from object storage.

```sql
WITH facts AS (
  SELECT entity_id, event_time,
         COALESCE(knowledge_time, event_time + INTERVAL 18 MONTH) AS kt,
         feature_name, value_num
  FROM read_parquet([ ...explicit key list from feature_partition... ])
  UNION ALL
  SELECT ... FROM read_parquet([ ... ])
)
SELECT s.entity_id, s.t AS spine_time, s.cutoff AS data_through,
       f.feature_name, f.value_num, f.kt AS knowledge_time,
       date_diff('day', f.kt, s.cutoff) AS age_days
FROM spine s
ASOF JOIN facts f
  ON s.entity_id = f.entity_id
     AND f.feature_name = s.feature_name
     AND f.kt <= s.cutoff
-- then pivot to wide at freeze
```

The `feature_name` equality in the ON clause is load-bearing. Without it a country's GDP
matches against its population's timeline.

Cross-source combination is a **UNION**, not a join — identical schemas mean no column
collisions and no schema reconciliation. With entity ids already canonical, the spine is the
only real join left.

**Granularity mismatch resolves for free.** Attaching country-year GDP to a country-week
spine needs no resampling logic: the as-of join carries the most recent known value forward
and updates when the next vintage lands.

**Which fact a spine row gets, stated plainly**, because the alternative reading is
reasonable and wrong. Resolution answers *what was the latest known value at the cutoff* —
not *what was the value for the period this row is labelled with*. The join constrains
knowledge_time against the cutoff and does not constrain event_time against the spine
timestamp at all. So under a fixed cutoff every spine row for an entity resolves to the same
figure, whatever period each row names.

That is what makes granularity mismatch free. Constraining event_time to match the spine row
would leave eleven months of every year empty for an annual indicator, and the carry-forward
is the entire point. `event_time` travels alongside the value so a reader can always see
which period the number describes.

**Staleness must be visible.** A value from 2019 and one from last week look identical in
the output. Carry the resolved `knowledge_time` and a derived `age_days` alongside each
feature, and support a per-feature `max_age` bound beyond which the value resolves to
null rather than carrying forward indefinitely. `max_age` is applied *before* the pivot, so a
nulled value takes its `age_days` with it.

`data_through` is the as-of cutoff **rendered as a column**, not a stored one. The store holds
knowledge_time per fact; a composition derives a cutoff per spine row and emits it on request.
The output's key is `(entity, spine_time, data_through)`, because two compositions of one
spine at different cutoffs are two legitimate rows.

---

## 10. Composition spec (the artifact)

Pydantic model, JSON Schema exported for editor validation. Fields:

- spine definition — entities × timestamps
- feature selection across groups (the pivot list)
- as-of policy: `spine_time`, `lag` with an offset, or `fixed`
- per-feature staleness bounds
- output orientation, freeze mode

Spine entities come from a literal list, from every entity present in a named group, or from
a stored versioned named list. The stored list is a deliberate and bounded exception to §7's
rule that the catalog holds no entity knowledge: a named list of ids is a *selection*, not
knowledge about entities. No aliases, no hierarchy, no succession, no attributes, and nothing
resolves through it at runtime.

**As-of exceptions are a schema field, not a code comment.** A feature that must be read at
spine time rather than the lagged cutoff — a calendar fact published in advance — declares
itself, with a required `reason`. It is part of the hashed spec and echoed into the manifest.
An undocumented exception is unrepresentable.

**Canonical serialization is defined before anything is hashed:** sorted keys, fixed
separators, defaults materialized explicitly, schema version included. Hash the canonical
form, never the raw file — otherwise reformatting rewrites the lineage graph.

`dataset_version.resolution_mode`:

| mode | meaning |
|---|---|
| `query` | predicate + pinned as-of cutoff + pinned ingest watermark; resolves identically without copying bytes |
| `materialized` | frozen parquet, content-addressed, immutable |

Both go through **one** `resolve()`. The materialized path is a cache of the query path,
never a second implementation, and a test asserts the two produce equal frames.

### The ingest watermark

A pinned as-of cutoff alone does **not** make a version reproducible. A fact ingested later,
whose knowledge_time is *before* the cutoff, changes the answer — which is precisely what a
bitemporal store exists to represent. So a frozen version pins an `ingest_watermark` too, and
resolution filters on it.

**Pinning silently withholds data, so it must not be silent.** Every read of a pinned version
counts the facts with `knowledge_time <= cutoff` and `ingest_time > watermark` — the late
arrivals the pin excludes, which were genuinely knowable at the as-of time — and warns with
the count, the groups affected, and the newest excluded knowledge_time.

This is the one place the error direction of §6 reverses. Withholding available data is the
safe direction only while somebody knows it happened. A pinned version quietly resolving
against a store that has since learned more is how a backtest stays reproducible and wrong at
the same time.

---

## 11. Invariants

1. **Knowledge-time monotonicity** — a run's output `knowledge_time` ≥ max
   `knowledge_time` of its inputs. Asserted in lineage write-back. Catches most lookahead
   bugs that survive the join logic.
2. **Namespace guard** — standardising a non-canonical source without a pinned crosswalk is
   an error, not a warning.
3. **Write order** — parquet first, verify, *then* insert the catalog row. A failure
   leaves an orphan file, never a catalog entry pointing at nothing. `verify` walks the
   catalog and checks every key resolves.
4. **Lineage is automatic, never opt-in.** Enforced by the type signature: every write takes
   a `Run`, and a `Run` is only constructible by the context manager that already inserted its
   row. Optional lineage gets skipped, and the catalog drifts into fiction within two projects.
5. **Frozen versions are immutable.** Enforced by a database trigger, not discipline — a
   "just fix this one row" is one `psql` session away. Corrections produce new versions; old
   pins keep resolving to old data.
6. **`knowledge_time <= ingest_time`**, per row. Catches a future-dated release, which would
   otherwise make facts visible to as-of queries that should not see them.
7. **Readers never glob.** The file list comes from `feature_partition`, so orphans stay
   invisible.
8. **One feature name, one dtype, forever.** Rejected at standardise, not discovered at read.
9. **Crosswalks are one-to-one**, checked at registration.
10. **Nothing is silently dropped.** Unmapped entities are routed and counted, not discarded.
11. **`event_time` matches the source's declared granularity.** Catches a wrong anchor, which
    shifts every derived knowledge_time by months.

---

## 12. Idempotence, integrity and undo

Running the same standardisation twice produces no second copy: if the output content hash
matches an existing partition, nothing is written and the run is recorded pointing at that
partition. Re-running a pipeline is free, and lineage still shows the attempt and that it
changed nothing.

After a failed run, `verify` says what is actually there — `--deep` re-downloads and re-hashes
and reconciles the `feature` registry against the parquet; `--run ID` scopes to one run's
outputs.

`run undo ID` deletes a run's outputs and marks it rolled back, leaving the run row in place.
It **refuses** when anything depends on those outputs, naming what does rather than offering a
force flag: the alternative to refusing is a dangling pin, and a frozen version that no longer
resolves is worse than a partition that will not delete. Undo is what makes the differential
standardise safe to attempt.

---

## 13. Testing

Integration tests against a **live Postgres and a live Garage container** (testcontainers
or `pytest-postgresql`), not mocks. Mocking S3 defeats the point of having chosen an
S3-compatible store, and this is the highest-signal thing in the repo. Tests use a throwaway
container and never a shared instance — they create and wipe buckets, and pointing that at
real data is one config typo away.

CI additionally runs `alembic upgrade head → downgrade base → upgrade head` against a
clean database, catching missing downgrades and dependency-order bugs while the schema is
small.

**Point-in-time correctness is proven by a truncation property, not by examples:**

> For any spec `S` and cutoff `T`, resolving `S` as-of `T` over the full store must equal
> resolving `S` as-of `T` over a store from which every fact with `knowledge_time > T` has
> been physically deleted.

It does not know or care how the answer is produced. Any path that reads a row it should not —
the join, the pivot, a carry-forward, a retraction filter, a future addition — diverges
between the two stores. An example-based test guards the join you thought about; this guards
the ones you did not.

---

## 14. Deferred, and not excluded

Additive later, unwinding nothing here:

- revision detection for high-volume incremental sources
- a knowledge layer owning aliases, hierarchy and succession, interfacing by *emitting*
  crosswalk versions, never queried at runtime
- blobs, records, labels, model registry, processing-step registry

Ordering principle throughout: **a real consumer forces each abstraction into existence.**
Plugin seams are designed at the third implementation, not the first.

---

## 15. Decisions worth recording as ADRs

Recorded in `docs/adr/`:

| | |
|---|---|
| 0001 | Period labels anchor to the period end |
| 0002 | Facts are stored long; wide is a presentation |
| 0003 | Three timestamps, and what a modelled lag means |
| 0004 | A revision carries the date of the release that changed it |
| 0005 | Entity ids are canonicalised at write, not resolved at read |
| 0006 | Crosswalks are one-to-one, and unmapped entities are routed |
| 0007 | A frozen version pins the ingest watermark too |
| 0008 | Frozen versions are immutable, enforced by the database |
| 0009 | Whether a null withdraws a value is declared per group |
| 0010 | One storage code path, and a bucket of its own |
| 0011 | Enum-like columns are TEXT with a CHECK |
| 0012 | Every timestamp is naive UTC |

Two decisions from earlier drafts are deliberately *not* recorded, because they
turned out to restate an implementation rather than choose between defensible
alternatives: melting batched by column (a memory limit, not a design), and the
wide pivot being written out rather than generated (one behaviour of `PIVOT`,
documented where it is used).

Dormancy of several months is the normal case for a background project. Re-deriving these
from scratch is what turns a pause into an abandonment.
