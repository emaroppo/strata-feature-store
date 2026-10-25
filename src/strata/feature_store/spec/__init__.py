"""Pure definitions: no I/O, and no imports that perform any.

``tests/spec/test_purity.py`` fails if ``sqlalchemy``, ``boto3``, ``duckdb``,
``psycopg``, ``alembic`` or ``strata.feature_store.store`` enters the import graph.
"""
