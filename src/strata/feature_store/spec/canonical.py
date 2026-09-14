"""Canonical serialisation, defined before anything is hashed.

Every config and every spec in this system is identified by the hash of its
canonical form. That only works if the canonical form is pinned down first —
otherwise reformatting a file, reordering two keys, or writing out a default
someone had omitted silently rewrites the lineage graph, and runs that used
identical settings stop looking identical.

**The hash is over the effective config, never the file on disk.** Two runs of
one release under different ``--set`` overrides are two different configs, and
hashing the bytes of the shared file would make them look the same. Conversely
a file reindented overnight is the same config, and hashing its bytes would
make it look different.

The rules, all of which exist because the alternative changes a hash without
changing a meaning:

- keys sorted, so authoring order is not significant
- no incidental whitespace
- defaults materialised by the caller before hashing, so an omitted field and
  an explicitly-written default agree
- a schema version inside the payload, so a change to what a field *means*
  changes the hash even when the text is identical
- ``NaN`` and infinities refused, because they are not JSON and every reader
  disagrees about them
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any

#: Everything hashed here is small — a config, a spec. Reading it whole is fine.
_SEPARATORS = (",", ":")


def _default(value: Any) -> Any:
    """Render types JSON does not cover, deterministically."""
    if isinstance(value, datetime):
        # Naive UTC throughout the store, so an offset here means something
        # upstream did not convert at the edge.
        if value.tzinfo is not None:
            raise ValueError(
                f"{value!r} carries a timezone; timestamps are naive UTC and are "
                "converted at ingest, not at hash time"
            )
        return value.isoformat(sep="T", timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (set, frozenset)):
        raise TypeError(
            "sets have no order and cannot be hashed reproducibly; use a sorted list"
        )
    if hasattr(value, "model_dump"):  # a pydantic model that was not dumped first
        return value.model_dump(mode="json")
    raise TypeError(f"no canonical form defined for {type(value).__name__}")


def canonical_json(payload: Any) -> str:
    """The one textual form of *payload* that this system hashes."""
    return json.dumps(
        payload,
        sort_keys=True,
        separators=_SEPARATORS,
        ensure_ascii=False,
        allow_nan=False,
        default=_default,
    )


def canonical_bytes(payload: Any) -> bytes:
    """:func:`canonical_json` as UTF-8, which is what actually gets hashed."""
    return canonical_json(payload).encode("utf-8")


def to_jsonable(payload: Any) -> Any:
    """*payload* reduced to plain JSON types, via the canonical form.

    Use this before handing anything to a JSONB column. A database driver
    serialises with its own ``json.dumps``, which knows nothing about dates and
    raises on the first one — and a config that carries a release date is the
    normal case, not an edge case.

    Going through the canonical form rather than a bespoke converter buys a
    property worth having: what gets stored is byte-for-byte what got hashed, so
    a ``config_hash`` can always be recomputed from the row it describes.
    """
    return json.loads(canonical_json(payload))


def content_hash(payload: Any) -> str:
    """The sha256 of *payload*'s canonical form, hex, full length.

    Used for ``run.config_hash`` and ``dataset_version.content_hash``. Object
    keys truncate it for readability; identity here does not.
    """
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()


def short_hash(payload: Any, length: int = 16) -> str:
    """A truncated :func:`content_hash`, for object key names."""
    if not 8 <= length <= 64:
        raise ValueError("truncating below 8 hex characters invites collisions")
    return content_hash(payload)[:length]


def hash_file(path: str, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    """The sha256 and byte length of a file on disk, read in chunks.

    This is the one hash in the module that is over raw bytes rather than a
    canonical form, because a release *is* its bytes: it is the file exactly as
    the publisher served it, and nothing here has decided yet what any of it
    means. Chunked because a source file is routinely hundreds of megabytes.
    """
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size
