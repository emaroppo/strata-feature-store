"""DuckDB, configured for the object store and for work larger than memory.

Every S3 setting here prevents a failure that reports something other than its
cause. See ``docs/adr/0010``.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from strata.feature_store.store.config import StorageSettings

#: Above this DuckDB spills rather than fails; left well under total RAM.
DEFAULT_MEMORY_LIMIT = "8GB"


def connect(
    storage: StorageSettings | None = None,
    *,
    memory_limit: str = DEFAULT_MEMORY_LIMIT,
    temp_directory: str | Path | None = None,
    threads: int | None = None,
) -> duckdb.DuckDBPyConnection:
    """An in-process DuckDB, optionally wired to the bucket.

    ``temp_directory`` is where a larger-than-memory sort spills; it needs room
    comparable to the data.
    """
    connection = duckdb.connect()
    connection.execute(f"SET memory_limit='{memory_limit}'")
    # Every parquet written is explicitly sorted; preserving insertion order
    # would make DuckDB buffer a whole pipeline instead of streaming it.
    connection.execute("SET preserve_insertion_order=false")
    if threads:
        connection.execute(f"SET threads={threads}")
    if temp_directory:
        Path(temp_directory).mkdir(parents=True, exist_ok=True)
        connection.execute(f"SET temp_directory='{temp_directory}'")

    if storage is not None:
        connection.execute("INSTALL httpfs")
        connection.execute("LOAD httpfs")
        connection.execute(f"SET s3_endpoint='{storage.duckdb_endpoint}'")
        connection.execute(f"SET s3_access_key_id='{storage.access_key}'")
        connection.execute(f"SET s3_secret_access_key='{storage.secret_key}'")
        connection.execute(f"SET s3_region='{storage.region or 'us-east-1'}'")
        connection.execute(f"SET s3_url_style='{storage.addressing_style}'")
        connection.execute(f"SET s3_use_ssl={'true' if storage.use_ssl else 'false'}")
    return connection


def s3_url(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"
