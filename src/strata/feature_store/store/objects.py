"""Bytes in and out of the object store.

**Content addressing.** A release's key is derived from a sha256 of its bytes,
never from the name the publisher gave it. Publishers reuse filenames across
vintages constantly — the same ``indicators.csv`` in April and again in
September — and a name-derived key would let the second silently overwrite the
first. After that no earlier standardisation is reproducible, and nothing
anywhere records that it happened. Under content addressing they are simply two
objects, and re-uploading identical bytes is a no-op rather than a duplicate.

**Write order, which is invariant 3.** Upload, verify the upload landed, and
only then let the caller write a catalog row. A crash in the middle therefore
leaves an orphan object and never a catalog row pointing at nothing — a
reconcilable mess rather than a corrupt one. :func:`put_file` returns only after
the verification passes, so a caller that writes its row afterwards gets the
ordering for free.

There is deliberately **no storage backend abstraction**. One code path, one
configurable endpoint: Garage locally, and the same code against S3, R2 or Ceph
by changing ``endpoint_url``. An abstraction here would mean the thing that runs
in tests is not the thing that runs in production, which is the gap that makes
S3 mocks worthless.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from strata.feature_store.spec.canonical import hash_file

if TYPE_CHECKING:  # pragma: no cover
    from strata.feature_store.store.config import StorageSettings

#: Where raw release bytes live. Fanned out two levels so no single prefix holds
#: every object, which keeps listing usable on stores that paginate by prefix.
SOURCES_PREFIX = "sources"

#: Our own checksum, carried as object metadata. S3's ETag is only an MD5 for
#: single-part uploads and something else entirely for multipart, so it cannot
#: be relied on to mean anything; this can.
SHA256_METADATA_KEY = "sha256"


class StorageError(RuntimeError):
    """Raised when the store did not end up holding what we asked it to."""


@dataclass(frozen=True, slots=True)
class StoredObject:
    key: str
    sha256: str
    size_bytes: int
    #: False when the identical bytes were already present under this key.
    uploaded: bool


def client(settings: StorageSettings):
    """A boto3 S3 client pointed at the configured endpoint.

    Path-style addressing is not optional for a self-hosted store: virtual-host
    style needs a wildcard DNS record that a Garage instance on a private
    network does not have, and the failure looks like a name-resolution error
    rather than a configuration one.
    """
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name=settings.region,
        config=BotoConfig(
            s3={"addressing_style": settings.addressing_style},
            retries={"max_attempts": 5, "mode": "standard"},
            # botocore 1.36+ validates a flexible checksum on every response by
            # default, and computes it over multipart downloads in a way
            # S3-compatible stores do not reproduce. The result is a
            # FlexibleChecksumError on a file that arrived perfectly intact, and
            # only on files large enough to be fetched in parts — so it passes
            # every small test and fails on the first real one.
            #
            # Turning it off costs nothing here: every object this store reads
            # back is verified against the sha256 in its own key, which is a
            # stronger check than the one being skipped.
            response_checksum_validation="when_required",
            request_checksum_calculation="when_required",
        ),
    )


def source_key(sha256: str, suffix: str = "") -> str:
    """The key a release with this digest lives under.

    The suffix is cosmetic — enough that downloading the object by hand gives
    you something openable. Identity is the digest, and nothing reads the
    extension back.
    """
    if len(sha256) != 64:
        raise ValueError(f"expected a 64-character sha256, got {len(sha256)}")
    return f"{SOURCES_PREFIX}/{sha256[:2]}/{sha256[2:4]}/{sha256}{suffix}"


def exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True


def head(s3, bucket: str, key: str) -> dict:
    return s3.head_object(Bucket=bucket, Key=key)


def put_file(s3, bucket: str, path: str | Path, *, suffix: str | None = None) -> StoredObject:
    """Upload *path* under its content address, and verify it arrived.

    Returns without uploading when an object of the same size already sits at
    that key: identical bytes hash to the same digest, so the key already holds
    exactly what would have been written.
    """
    path = Path(path)
    digest, size = hash_file(str(path))
    key = source_key(digest, suffix if suffix is not None else "".join(path.suffixes[-1:]))

    try:
        existing = head(s3, bucket, key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
            raise
    else:
        if existing["ContentLength"] != size:
            # Two different byte strings hashing to one sha256 would be news.
            # Far likelier is a truncated earlier upload, which must not be
            # mistaken for the file being present.
            raise StorageError(
                f"{key} already exists with {existing['ContentLength']} bytes but the local "
                f"file is {size}. Refusing to treat it as present; investigate before retrying."
            )
        return StoredObject(key=key, sha256=digest, size_bytes=size, uploaded=False)

    # upload_file switches to multipart above a threshold on its own, which the
    # seed panel at three quarters of a gigabyte comfortably exceeds.
    s3.upload_file(
        Filename=str(path),
        Bucket=bucket,
        Key=key,
        ExtraArgs={"Metadata": {SHA256_METADATA_KEY: digest}},
    )

    landed = head(s3, bucket, key)
    if landed["ContentLength"] != size:
        raise StorageError(
            f"uploaded {size} bytes to {key} but the store reports "
            f"{landed['ContentLength']}. The catalog has not been written."
        )
    recorded = landed.get("Metadata", {}).get(SHA256_METADATA_KEY)
    if recorded != digest:
        raise StorageError(
            f"{key} came back with sha256 metadata {recorded!r}, expected {digest!r}. "
            "The catalog has not been written."
        )
    return StoredObject(key=key, sha256=digest, size_bytes=size, uploaded=True)


def get_to_file(s3, bucket: str, key: str, destination: str | Path) -> Path:
    """Download *key* to *destination*, verifying the digest when the key carries one."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(Bucket=bucket, Key=key, Filename=str(destination))

    expected = Path(key).stem
    if len(expected) == 64:
        digest, _ = hash_file(str(destination))
        if digest != expected:
            destination.unlink(missing_ok=True)
            raise StorageError(
                f"{key} downloaded with digest {digest}, but its key claims {expected}. "
                "The local copy has been removed."
            )
    return destination


def delete(s3, bucket: str, key: str) -> None:
    """Remove one object. Used by ``release prune`` and ``run undo``, never automatically."""
    s3.delete_object(Bucket=bucket, Key=key)


def iter_keys(s3, bucket: str, prefix: str = "") -> Iterator[str]:
    """Every key under *prefix*, paginated.

    Used by ``verify`` to find orphans — objects the store holds that no catalog
    row points at. Note that nothing in the read path lists: composition builds
    its file list from ``feature_partition``, precisely so the orphans a failed
    run leaves behind stay invisible to it.
    """
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            yield item["Key"]


def sha256_bytes(data: bytes) -> str:
    """Digest of an in-memory payload, for small artifacts like a run manifest."""
    return hashlib.sha256(data).hexdigest()


def put_bytes(s3, bucket: str, key: str, data: bytes, content_type: str | None = None) -> str:
    """Write *data* to *key* and verify the length. Returns the digest."""
    digest = sha256_bytes(data)
    extra = {"Metadata": {SHA256_METADATA_KEY: digest}}
    if content_type:
        extra["ContentType"] = content_type
    s3.put_object(Bucket=bucket, Key=key, Body=data, **extra)
    landed = head(s3, bucket, key)
    if landed["ContentLength"] != len(data):
        raise StorageError(
            f"wrote {len(data)} bytes to {key}, store reports {landed['ContentLength']}"
        )
    return digest
