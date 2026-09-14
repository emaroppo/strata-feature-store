"""Pure definitions. No I/O, and no imports that perform any.

``tests/spec/test_purity.py`` walks this package's import graph and fails if
``sqlalchemy``, ``boto3``, ``duckdb``, ``psycopg``, ``alembic`` or anything
under ``strata.feature_store.store`` appears in it. Shared kernels are where coupling
hides, which is why the rule is tested rather than described.
"""
