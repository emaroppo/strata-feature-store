"""Catalog writes. Every one of them takes a :class:`~feature_store.store.runs.Run`.

That first parameter is invariant 4. A ``Run`` cannot be constructed directly —
only :func:`feature_store.store.runs.run` yields one, and it has already inserted
the run row before it does. So there is no way to write a catalog row without
lineage, and none of these functions has to remember to record it.

Reads live here too, but take a plain connection: reading changes nothing and
owes no account of itself.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import sqlalchemy as sa

from feature_store.store.runs import Run
from feature_store.store.tables import entity_namespace, release, source


class CatalogError(RuntimeError):
    """A write would have contradicted something the catalog already holds."""


# --------------------------------------------------------------------------
# entity namespaces


def ensure_namespace(
    run: Run,
    name: str,
    *,
    description: str | None = None,
    external_ref: str | None = None,
) -> str:
    """Register a vocabulary if it is not already known. Idempotent."""
    with run.transaction() as connection:
        existing = connection.execute(
            sa.select(entity_namespace.c.name).where(entity_namespace.c.name == name)
        ).scalar_one_or_none()
        if existing is not None:
            return existing
        connection.execute(
            sa.insert(entity_namespace).values(
                name=name, description=description, external_ref=external_ref
            )
        )
    return name


def namespace_exists(connection: sa.Connection, name: str) -> bool:
    return (
        connection.execute(
            sa.select(entity_namespace.c.name).where(entity_namespace.c.name == name)
        ).scalar_one_or_none()
        is not None
    )


# --------------------------------------------------------------------------
# sources


def add_source(
    run: Run,
    *,
    name: str,
    native_namespace: str,
    granularity: str,
    kind: str = "panel",
    licence: str | None = None,
    source_url: str | None = None,
    description: str | None = None,
) -> int:
    """Register a publisher's product.

    The native namespace and the granularity are declared once, here, rather
    than repeated in every standardisation config. Both are read back at
    standardise: the first decides whether a crosswalk is required, the second
    is what event times are checked against.
    """
    with run.transaction() as connection:
        # Name first: when the source already exists, saying so is more use than
        # a complaint about a namespace the caller was never going to reach.
        existing = get_source(connection, name)
        if existing is not None:
            raise CatalogError(
                f"source {name!r} already exists (id {existing['id']}). Sources are registered "
                "once; a new file from the same publisher is a release, not a source."
            )
        if not namespace_exists(connection, native_namespace):
            raise CatalogError(
                f"entity namespace {native_namespace!r} is not registered. "
                "Register the vocabulary before the source that publishes in it."
            )
        source_id = connection.execute(
            sa.insert(source)
            .values(
                name=name,
                kind=kind,
                native_namespace=native_namespace,
                granularity=granularity,
                licence=licence,
                source_url=source_url,
                description=description,
            )
            .returning(source.c.id)
        ).scalar_one()
    return source_id


def get_source(connection: sa.Connection, name: str) -> dict[str, Any] | None:
    row = connection.execute(sa.select(source).where(source.c.name == name)).mappings().first()
    return dict(row) if row else None


def get_source_by_id(connection: sa.Connection, source_id: int) -> dict[str, Any] | None:
    row = connection.execute(sa.select(source).where(source.c.id == source_id)).mappings().first()
    return dict(row) if row else None


def list_sources(connection: sa.Connection) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in connection.execute(sa.select(source).order_by(source.c.name)).mappings().all()
    ]


# --------------------------------------------------------------------------
# releases


def add_release(
    run: Run,
    *,
    source_id: int,
    sha256: str,
    object_key: str,
    size_bytes: int,
    original_filename: str,
    media_type: str | None = None,
    source_url: str | None = None,
    released_at: date | None = None,
    retrieved_at: datetime | None = None,
    meta: dict[str, Any] | None = None,
) -> int:
    """Record a downloaded file.

    Called only after the bytes are in the store and verified, which is
    invariant 3 — parquet first, verify, then the catalog row. A crash before
    this point leaves an orphan object; a crash after it leaves nothing wrong.
    """
    with run.transaction() as connection:
        existing = get_release(connection, sha256, source_id=source_id)
        if existing is not None:
            # Same bytes, same product: this file is already here. Re-registering
            # it is a no-op, and the run still records that it was attempted.
            return existing["id"]

        release_id = connection.execute(
            sa.insert(release)
            .values(
                source_id=source_id,
                sha256=sha256,
                object_key=object_key,
                size_bytes=size_bytes,
                original_filename=original_filename,
                media_type=media_type,
                source_url=source_url,
                released_at=released_at,
                retrieved_at=retrieved_at,
                created_by_run_id=run.id,
                meta=meta,
            )
            .returning(release.c.id)
        ).scalar_one()
        run.wrote("release", release_id, role="release")
    return release_id


def get_release(
    connection: sa.Connection, sha256: str, *, source_id: int | None = None
) -> dict[str, Any] | None:
    query = sa.select(release).where(release.c.sha256 == sha256)
    if source_id is not None:
        query = query.where(release.c.source_id == source_id)
    row = connection.execute(query).mappings().first()
    return dict(row) if row else None


def find_release(connection: sa.Connection, prefix: str) -> dict[str, Any] | None:
    """Look a release up by a digest prefix, so a human can type eight characters."""
    rows = (
        connection.execute(sa.select(release).where(release.c.sha256.like(f"{prefix}%")))
        .mappings()
        .all()
    )
    if not rows:
        return None
    digests = {row["sha256"] for row in rows}
    if len(digests) > 1:
        raise CatalogError(
            f"{prefix!r} matches {len(digests)} distinct digests; give more characters"
        )
    # One digest across several rows means two sources registered identical
    # bytes. They resolve to the same object, so fetching is unambiguous even
    # though the catalog rows are not.
    return dict(rows[0])


def list_releases(connection: sa.Connection, source_id: int | None = None) -> list[dict[str, Any]]:
    query = sa.select(release).order_by(release.c.released_at, release.c.ingested_at)
    if source_id is not None:
        query = query.where(release.c.source_id == source_id)
    return [dict(r) for r in connection.execute(query).mappings().all()]


# --------------------------------------------------------------------------
# partitions


def live_partitions(
    connection: sa.Connection, feature_group_id: int | None = None
) -> list[dict[str, Any]]:
    """The parquet objects a reader is allowed to see.

    Two filters, and both matter:

    **The creating run must have finished.** A standardise commits each part as
    it writes it, so a run still in progress — or one that died at the last
    batch — has real, complete parquet files in the store that describe only
    part of a group. Reading them would return a plausible answer computed from
    a quarter of the data, with nothing indicating that. Visibility therefore
    follows the run's status rather than a transaction boundary.

    **A superseded partition is not live.** Re-standardising a release under a
    corrected config supersedes the old part rather than deleting it, so old
    pins keep resolving while new reads do not see both.

    This is also invariant 7: the file list comes from here, never from listing
    a prefix, so the orphans a failed run leaves behind stay invisible.
    """
    from feature_store.store.tables import feature_partition
    from feature_store.store.tables import run as run_table

    query = (
        sa.select(feature_partition)
        .join(run_table, run_table.c.id == feature_partition.c.created_by_run_id)
        .where(
            run_table.c.status == "ok",
            feature_partition.c.superseded_by_run_id.is_(None),
        )
        .order_by(feature_partition.c.id)
    )
    if feature_group_id is not None:
        query = query.where(feature_partition.c.feature_group_id == feature_group_id)
    return [dict(r) for r in connection.execute(query).mappings().all()]
