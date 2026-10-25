"""The canonical form: ``strata.common.canonical``, re-exported.

The digest of ``strata.common.contract``'s payload is pinned in
``tests/spec/test_canonical.py``. See ``strata/docs/adr/0017``.
"""

from strata.common.canonical import (
    CANONICAL_VERSION,
    canonical_bytes,
    canonical_json,
    content_hash,
    hash_file,
    short_hash,
    to_jsonable,
)

__all__ = [
    "CANONICAL_VERSION",
    "canonical_bytes",
    "canonical_json",
    "content_hash",
    "hash_file",
    "short_hash",
    "to_jsonable",
]
