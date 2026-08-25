"""DuckDB, configured for the object store and for work larger than memory.

Every setting here corresponds to a specific failure, and each of those failures
reports something other than the setting that caused it. They are collected in
one place so nobody has to rediscover them.

**The endpoint has no scheme.** boto3 wants ``http://host:3900``; DuckDB wants
``host:3900`` and rejects the other form. Passing the URL straight through fails
with an error about the request rather than about the setting.

**Path-style addressing is required.** Virtual-host style needs a wildcard DNS
record that a self-hosted store on a private network does not have, and the
failure surfaces as name resolution.

**A region must be set even when it means nothing.** The signing algorithm
includes it, so an empty one produces a signature mismatch, which reads as a
credentials problem.

**SSL follows the endpoint** rather than being configured separately, so the two
cannot disagree.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:  # pragma: no cover
    from feature_store.store.config import StorageSettings

#: Sorting a few hundred million rows does not fit in memory, and the default
#: limit makes DuckDB fail rather than spill. Left well under total RAM so the
#: database, the writer and the operating system are not competing for the last
#: of it.
DEFAULT_MEMORY_LIMIT = "8GB"


def connect(
    storage: StorageSettings | None = None,
    *,
    memory_limit: str = DEFAULT_MEMORY_LIMIT,
    temp_directory: str | Path | None = None,
    threads: int | None = None,
) -> duckdb.DuckDBPyConnection:
    """An in-process DuckDB, optionally wired to the bucket.

    ``temp_directory`` is where a larger-than-memory sort spills. It needs room
    comparable to the data being sorted, and a sort that runs out of space fails
    late — after the expensive part.
    """
    connection = duckdb.connect()
    connection.execute(f"SET memory_limit='{memory_limit}'")
    # Insertion order is meaningless to this store: every parquet it writes is
    # explicitly sorted, and nothing reads a file expecting the order it arrived
    # in. Preserving it forces DuckDB to buffer a whole pipeline in memory
    # rather than stream it, which turns a melt of a few hundred million facts
    # into an out-of-memory error instead of a spill.
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
