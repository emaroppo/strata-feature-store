"""Point-in-time correct storage, standardisation and retrieval for revised panel data.

Two subpackages, and the boundary between them is enforced by a test:

``spec``
    What a thing *is*. The long-format schema, the standardisation and
    composition config models, canonical hashing, calendar arithmetic. Pure
    functions and pure types: no I/O, and no import of anything that does any.

``store``
    Everything that touches the outside world — Postgres, the object store,
    DuckDB, the CLI.

The split is not decoration. Post-processing lives in a separate repository by
design, and it needs to know what a composed output looks like without
acquiring a database driver and an S3 client to find out.
"""
