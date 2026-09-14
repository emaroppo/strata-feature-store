"""Everything that touches the outside world: Postgres, the bucket, DuckDB, the CLI.

Imports freely from :mod:`strata.feature_store.spec`. The reverse never happens, and
``tests/spec/test_purity.py`` is what keeps that true rather than merely
intended.
"""
