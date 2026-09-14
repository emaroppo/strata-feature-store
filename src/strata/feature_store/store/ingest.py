"""Workflow steps 1 and 2: submit a file, keep it exactly as it arrived.

Nothing here parses, unzips, sniffs or normalises. The file goes into the store
byte-identical to what the publisher served, and the only thing derived from it
is its digest. That restraint is what makes a mapping mistake recoverable: when
a standardisation config turns out to have named a column wrong, the fix is to
re-standardise from these bytes rather than to re-download and hope the
publisher still serves the same vintage. Publishers routinely do not.

**Order of operations, which is invariant 3.** Hash, upload, verify the upload,
then write the catalog row. A crash anywhere before the last step leaves an
object nothing points at — findable by ``verify`` and removable — rather than a
catalog row pointing at bytes that are not there.
"""

from __future__ import annotations

import mimetypes
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from strata.feature_store.store import catalog, objects
from strata.feature_store.store.config import Settings
from strata.feature_store.store.runs import run as open_run


@dataclass(frozen=True, slots=True)
class IngestResult:
    release_id: int
    run_id: int
    sha256: str
    object_key: str
    size_bytes: int
    #: False when these exact bytes were already in the store.
    uploaded: bool


def _naive_utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def register_source(
    engine: sa.Engine,
    settings: Settings,
    *,
    name: str,
    native_namespace: str,
    granularity: str,
    kind: str = "panel",
    licence: str | None = None,
    source_url: str | None = None,
    description: str | None = None,
) -> int:
    """Register a publisher's product, and its vocabulary if it is new."""
    with open_run(
        engine,
        "ingest_release",
        f"register source {name}",
        {
            "source": name,
            "native_namespace": native_namespace,
            "granularity": granularity,
            "kind": kind,
        },
    ) as current:
        # The vocabulary is NOT created here. Auto-creating it would make
        # the guard in add_source unreachable, and a typo in the namespace
        # would silently mint a new vocabulary that nothing can join to.
        # Registering a vocabulary is its own deliberate act.
        source_id = catalog.add_source(
            current,
            name=name,
            native_namespace=native_namespace,
            granularity=granularity,
            kind=kind,
            licence=licence,
            source_url=source_url,
            description=description,
        )
        current.note(source_id=source_id)
    return source_id


def add_release(
    engine: sa.Engine,
    settings: Settings,
    *,
    path: str | Path,
    source_name: str,
    released_at: date | None = None,
    retrieved_at: datetime | None = None,
    source_url: str | None = None,
    meta: dict[str, Any] | None = None,
    s3=None,
) -> IngestResult:
    """Put one downloaded file into the store and record it.

    ``released_at`` is the publisher's vintage and ``retrieved_at`` is when we
    fetched it. They are separate on purpose: conflating them, and stamping a
    2024 archive with today's date, makes every as-of query before today return
    nothing — which reads as missing data rather than as a mistake.
    """
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(path)

    s3 = s3 or objects.client(settings.storage)
    bucket = settings.storage.bucket

    with engine.begin() as connection:
        source = catalog.get_source(connection, source_name)
        if source is None:
            raise catalog.CatalogError(
                f"no source named {source_name!r}. Register it first: a release belongs to a "
                "product, and the product carries the licence, vocabulary and granularity."
            )

    params = {
        "source": source_name,
        "original_filename": path.name,
        "released_at": released_at,
        "retrieved_at": retrieved_at or _naive_utc_now(),
        "source_url": source_url,
    }

    with open_run(
        engine,
        "ingest_release",
        f"ingest {path.name} into {source_name}",
        params,
        s3=s3,
        bucket=bucket,
    ) as current:
        # Bytes first, verified, and only then the row.
        stored = objects.put_file(s3, bucket, path)

        release_id = catalog.add_release(
            current,
            source_id=source["id"],
            sha256=stored.sha256,
            object_key=stored.key,
            size_bytes=stored.size_bytes,
            original_filename=path.name,
            media_type=mimetypes.guess_type(path.name)[0],
            source_url=source_url,
            released_at=released_at,
            retrieved_at=params["retrieved_at"],
            meta=meta,
        )
        current.note(
            release_id=release_id,
            sha256=stored.sha256,
            size_bytes=stored.size_bytes,
            uploaded=stored.uploaded,
        )
        # A release is a fact that became knowable when it was published.
        # Recording it here is what lets invariant 1 compare a later
        # standardise run against the release it read.
        if released_at is not None:
            current.observed_knowledge_time(
                datetime(released_at.year, released_at.month, released_at.day)
            )
        run_id = current.id

    return IngestResult(
        release_id=release_id,
        run_id=run_id,
        sha256=stored.sha256,
        object_key=stored.key,
        size_bytes=stored.size_bytes,
        uploaded=stored.uploaded,
    )


def fetch_release(
    engine: sa.Engine,
    settings: Settings,
    *,
    digest_prefix: str,
    destination: str | Path,
    s3=None,
) -> Path:
    """Pull an original file back out, digest-verified on arrival."""
    with engine.begin() as connection:
        found = catalog.find_release(connection, digest_prefix)
    if found is None:
        raise catalog.CatalogError(f"no release whose digest starts with {digest_prefix!r}")
    if found["pruned_at"] is not None:
        raise catalog.CatalogError(
            f"release {found['sha256'][:12]} was pruned on {found['pruned_at']:%Y-%m-%d}; "
            "the catalog row remains as a tombstone but the bytes are gone."
        )
    s3 = s3 or objects.client(settings.storage)
    return objects.get_to_file(s3, settings.storage.bucket, found["object_key"], destination)
