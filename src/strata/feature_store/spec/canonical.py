"""The canonical form: ``strata.common.canonical``, re-exported.

The rules were re-declared here once, so that no release of this package
had to be coordinated with another. They are one implementation now,
because two copies of a hashing rule drift and the fault is a lineage
that quietly stops matching. The digest of ``strata.common.contract``'s
payload is pinned in ``tests/spec/test_canonical.py``, so a change to the
rules upstream fails here on upgrade rather than moving an identity.
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
