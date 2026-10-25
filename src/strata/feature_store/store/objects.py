"""Bytes in and out of the object store, over one code path with no backend abstraction.

A release is keyed by the sha256 of its bytes, never by the publisher's filename,
so reused names cannot overwrite a vintage. Upload and verify before any catalog
row is written (invariant 3), so a crash leaves an orphan, never a dangling row.
See ``docs/adr/0010``.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from strata.feature_store.spec.canonical import hash_file
from strata.feature_store.store.config import StorageSettings

#: Where raw release bytes live, fanned out two levels so listing stays usable.
SOURCES_PREFIX = "sources"

#: Our own checksum as object metadata; S3's ETag means nothing for multipart uploads.
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
    """A boto3 S3 client pointed at the configured endpoint. See ``docs/adr/0010``."""
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key,
        aws_secret_access_key=settings.secret_key,
        region_name=settings.region,
        config=BotoConfig(
            s3={"addressing_style": settings.addressing_style},
            retries={"max_attempts": 5, "mode": "standard"},
            # botocore's flexible checksums fail on multipart transfers against
            # S3-compatible stores; the sha256 in the key is the real check. docs/adr/0010
            response_checksum_validation="when_required",
            request_checksum_calculation="when_required",
        ),
    )


def source_key(sha256: str, suffix: str = "") -> str:
    """The key a release with this digest lives under; the suffix is cosmetic, never read back."""
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
    """Upload *path* under its content address and verify it arrived.

    Returns ``uploaded=False`` without writing when an object of the same size is
    already at that key.
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
            # A size mismatch under one digest is a truncated earlier upload, not a collision.
            raise StorageError(
                f"{key} already exists with {existing['ContentLength']} bytes but the local "
                f"file is {size}. Refusing to treat it as present; investigate before retrying."
            )
        return StoredObject(key=key, sha256=digest, size_bytes=size, uploaded=False)

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
    """Remove one object; never called automatically."""
    s3.delete_object(Bucket=bucket, Key=key)


def iter_keys(s3, bucket: str, prefix: str = "") -> Iterator[str]:
    """Every key under *prefix*, paginated; nothing in the read path lists, only ``verify`` does."""
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
