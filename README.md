# feature-store

Point-in-time correct storage, standardisation and retrieval for panel data that
gets revised.

The question this exists to answer honestly is *what was the value of X* — where
the correct answer depends on when you ask. An indicator published in April is
revised in September; a naive join on event date hands your April backtest the
September figure, and the backtest looks better than the model is. Nothing errors,
nothing logs, and the number is simply wrong.

## What it does

Three things, and deliberately no more:

1. **Store** a source file exactly as downloaded, content-addressed and kept.
2. **Standardise** it into a long-format parquet where every fact carries when it
   happened, when it became knowable, and when it landed here.
3. **Retrieve** it as of any point in time, resolving each value to what was
   knowable then.

Everything downstream — transforms, imputation, normalisation, scoring, feature
engineering — lives in a separate repository. This store resolves what a value
*was*; it never computes a new value from one. Anything with a coefficient in it
belongs elsewhere.

## The shape of it

```
source            a publisher's product, registered once
  └── release     one downloaded file, content-addressed, kept
        └── standardise run
              └── feature_group + samples   long-format parquet in the bucket
                    └── composition
                          └── dataset_version   frozen, query or materialised
```

Postgres holds the catalog — descriptions, pointers, lineage — and zero feature
values. Parquet in an S3-compatible bucket holds the values. DuckDB reads them
straight out of the bucket to compose.

## Three timestamps

Every fact carries all three, and conflating any two of them is the bug this
design exists to prevent.

| column | meaning |
|---|---|
| `event_time` | when the thing happened, or the period it describes |
| `knowledge_time` | when the value became available |
| `ingest_time` | when it landed here |

`knowledge_time` is not the download date. Stamping a 2024 archive with a 2026
download date makes every as-of query before 2026 return nothing — which looks
like missing data rather than like a mistake.

## Getting started

Requires Docker and [uv](https://docs.astral.sh/uv/).

```bash
./scripts/dev-up.sh          # postgres + a throwaway Garage, bootstrapped
cp config.example.toml config.toml
cp .env.example .env         # paste the credentials dev-up.sh printed
uv run feature-store --help
```

The stack in `docker-compose.yml` is for development and tests only — the test
suite creates and wipes buckets, so it must never point at a real instance.
Production use points `config.toml` at a Garage instance elsewhere.

## Documentation

- `docs/architecture.md` — what this is and why, including the decisions that are
  load-bearing and the reasoning behind them.
- `docs/adr/` — one file per decision worth being able to revisit.

## Status

Early. The architecture is settled and written down; the implementation is being
built against it in order, starting from the catalog.
