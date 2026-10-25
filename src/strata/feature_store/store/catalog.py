"""Catalog writes, each taking a :class:`~strata.feature_store.store.runs.Run`, and reads.

Only :func:`strata.feature_store.store.runs.run` yields a ``Run``, so no catalog
row can be written without lineage (invariant 4).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from typing import Any

import sqlalchemy as sa

from strata.feature_store.store.runs import Run
from strata.feature_store.store.tables import (
    dataset,
    dataset_version,
    entity_list,
    entity_list_member,
    entity_list_version,
    entity_namespace,
    feature_partition,
    release,
    source,
)
from strata.feature_store.store.tables import run as run_table


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
    """Register a publisher's product; refuses a known name or an unregistered namespace."""
    with run.transaction() as connection:
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
    """Record a downloaded file whose bytes are already in the store and verified (invariant 3).

    Returns the existing id when the same bytes are already registered under this source.
    """
    with run.transaction() as connection:
        existing = get_release(connection, sha256, source_id=source_id)
        if existing is not None:
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
    """Look a release up by a digest prefix; refuses a prefix matching more than one digest."""
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
    # Several rows under one digest share one object, so fetching is unambiguous.
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
    """The parquet objects a reader may see: written by a finished run and not superseded.

    The file list comes from here, never from listing a prefix (invariant 7), so
    a partial run's parts stay invisible.
    """
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


# --------------------------------------------------------------------------
# dataset versions


def version_row(
    connection: sa.Connection,
    name: str,
    version: int | None,
    *,
    error: Callable[[list[int]], Exception],
) -> dict[str, Any]:
    """One ``dataset_version`` row by dataset name; ``version=None`` means the newest.

    When nothing matches, raises ``error(available)`` with the version numbers the
    dataset does have, so each caller words its own refusal.
    """
    query = (
        sa.select(dataset_version)
        .join(dataset, dataset.c.id == dataset_version.c.dataset_id)
        .where(dataset.c.name == name)
        .order_by(dataset_version.c.version_num.desc())
    )
    if version is not None:
        query = query.where(dataset_version.c.version_num == version)
    row = connection.execute(query).mappings().first()
    if row is None:
        available = (
            connection.execute(
                sa.select(dataset_version.c.version_num)
                .join(dataset, dataset.c.id == dataset_version.c.dataset_id)
                .where(dataset.c.name == name)
                .order_by(dataset_version.c.version_num)
            )
            .scalars()
            .all()
        )
        raise error(list(available))
    return dict(row)


# --------------------------------------------------------------------------
# entity lists


def add_entity_list(
    run: Run,
    *,
    name: str,
    entity_namespace: str,
    members: list[str],
    description: str | None = None,
) -> tuple[int, int]:
    """Freeze a named set of entity ids as a new version; refuses an empty list.

    A list is a selection, not knowledge about entities, and is versioned
    because a composition pins it.
    """
    unique = sorted(set(members))
    if not unique:
        raise CatalogError("an entity list with no members selects nothing")

    with run.transaction() as connection:
        if not namespace_exists(connection, entity_namespace):
            raise CatalogError(f"entity namespace {entity_namespace!r} is not registered")
        list_id = connection.execute(
            sa.select(entity_list.c.id).where(entity_list.c.name == name)
        ).scalar_one_or_none()
        if list_id is None:
            list_id = connection.execute(
                sa.insert(entity_list)
                .values(name=name, entity_namespace=entity_namespace, description=description)
                .returning(entity_list.c.id)
            ).scalar_one()
        version_num = (
            connection.execute(
                sa.select(
                    sa.func.coalesce(sa.func.max(entity_list_version.c.version_num), 0)
                ).where(entity_list_version.c.entity_list_id == list_id)
            ).scalar_one()
            + 1
        )
        version_id = connection.execute(
            sa.insert(entity_list_version)
            .values(
                entity_list_id=list_id,
                version_num=version_num,
                created_by_run_id=run.id,
            )
            .returning(entity_list_version.c.id)
        ).scalar_one()
        connection.execute(
            sa.insert(entity_list_member),
            [{"entity_list_version_id": version_id, "entity_id": e} for e in unique],
        )
    run.wrote("entity_list_version", version_id, role="entity_list")
    return version_id, version_num
