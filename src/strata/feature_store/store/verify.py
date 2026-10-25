"""Reconciling what the catalog claims against what the store holds, and undoing runs.

Shallow checks presence and size, deep checks digests and the feature registry,
orphans checks that every object is pointed at by a row.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from strata.feature_store.spec.canonical import hash_file
from strata.feature_store.store import catalog, duck, objects
from strata.feature_store.store.config import Settings
from strata.feature_store.store.tables import (
    dataset_version,
    feature,
    feature_group,
    feature_partition,
    release,
    run_input,
    run_output,
)
from strata.feature_store.store.tables import run as run_table


@dataclass
class Findings:
    """What did not line up. Empty is the only good answer."""

    missing: list[str] = field(default_factory=list)
    wrong_size: list[str] = field(default_factory=list)
    wrong_digest: list[str] = field(default_factory=list)
    orphans: list[str] = field(default_factory=list)
    registry_drift: list[str] = field(default_factory=list)
    checked: int = 0
    #: Objects of runs that never finished: not orphans, not readable either.
    unfinished: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.missing or self.wrong_size or self.wrong_digest or self.registry_drift)

    def as_dict(self) -> dict[str, Any]:
        return {
            "checked": self.checked,
            "missing": self.missing,
            "wrong_size": self.wrong_size,
            "wrong_digest": self.wrong_digest,
            "orphans": self.orphans,
            "registry_drift": self.registry_drift,
            "unfinished": self.unfinished,
        }


def _catalogued(connection: sa.Connection, run_id: int | None) -> list[dict[str, Any]]:
    """Every object the catalog claims exists.

    Every table that names an object key must appear here: a key absent from
    this list is an orphan to ``--prune-orphans``, so omitting a table loses data.
    """
    partitions = sa.select(
        feature_partition.c.object_key.label("key"),
        feature_partition.c.size_bytes.label("size"),
        feature_partition.c.content_sha256.label("digest"),
        run_table.c.status.label("status"),
        sa.literal("feature_partition").label("kind"),
    ).join(run_table, run_table.c.id == feature_partition.c.created_by_run_id)
    releases = (
        sa.select(
            release.c.object_key.label("key"),
            release.c.size_bytes.label("size"),
            release.c.sha256.label("digest"),
            run_table.c.status.label("status"),
            sa.literal("release").label("kind"),
        )
        .join(run_table, run_table.c.id == release.c.created_by_run_id)
        .where(release.c.pruned_at.is_(None))
    )
    versions = (
        sa.select(
            dataset_version.c.object_prefix.label("key"),
            sa.literal(None).label("size"),
            dataset_version.c.content_hash.label("digest"),
            run_table.c.status.label("status"),
            sa.literal("dataset_version").label("kind"),
        )
        .join(run_table, run_table.c.id == dataset_version.c.created_by_run_id)
        .where(dataset_version.c.object_prefix.isnot(None))
    )
    if run_id is not None:
        partitions = partitions.where(feature_partition.c.created_by_run_id == run_id)
        releases = releases.where(release.c.created_by_run_id == run_id)
        versions = versions.where(dataset_version.c.created_by_run_id == run_id)
    return [
        dict(r)
        for r in connection.execute(sa.union_all(partitions, releases, versions)).mappings().all()
    ]


def verify(
    engine: sa.Engine,
    settings: Settings,
    *,
    deep: bool = False,
    orphans: bool = False,
    run_id: int | None = None,
    s3=None,
) -> Findings:
    """Check the catalog against the store."""
    s3 = s3 or objects.client(settings.storage)
    bucket = settings.storage.bucket
    found = Findings()

    with engine.connect() as connection:
        rows = _catalogued(connection, run_id)

    known: set[str] = set()
    for row in rows:
        known.add(row["key"])
        found.checked += 1
        if row["status"] != "ok":
            found.unfinished.append(row["key"])
        try:
            head = objects.head(s3, bucket, row["key"])
        except Exception:
            found.missing.append(row["key"])
            continue
        if row["size"] is not None and head["ContentLength"] != row["size"]:
            found.wrong_size.append(
                f"{row['key']} ({head['ContentLength']} bytes, catalog says {row['size']})"
            )
        if deep and row["kind"] != "dataset_version":
            # A dataset_version's content_hash is over the spec, not the bytes.
            with tempfile.TemporaryDirectory() as work:
                local = Path(work) / "object"
                s3.download_file(Bucket=bucket, Key=row["key"], Filename=str(local))
                digest, _ = hash_file(str(local))
            if digest != row["digest"]:
                found.wrong_digest.append(f"{row['key']} hashes to {digest[:12]}")

    if orphans:
        for prefix in ("sources/", "features/", "crosswalks/", "composed/"):
            for key in objects.iter_keys(s3, bucket, prefix):
                if key.endswith(".json"):
                    continue  # specs and manifests are not catalogued objects
                if key not in known:
                    found.orphans.append(key)

    if deep:
        found.registry_drift = _registry_drift(engine, settings, run_id=run_id)
    return found


def _registry_drift(engine: sa.Engine, settings: Settings, *, run_id: int | None) -> list[str]:
    """Feature names the parquet holds that the registry does not know about.

    One direction only: a registry row without facts is the ordinary state of an
    unpublished feature, not drift.
    """
    drift: list[str] = []
    with engine.connect() as connection:
        groups = (
            connection.execute(
                sa.select(feature_group.c.id, feature_group.c.name).order_by(feature_group.c.id)
            )
            .mappings()
            .all()
        )
        for group in groups:
            partitions = catalog.live_partitions(connection, group["id"])
            if not partitions:
                continue
            claimed = set(
                connection.execute(
                    sa.select(feature.c.name).where(
                        feature.c.feature_group_id == group["id"],
                        feature.c.deprecated_at.is_(None),
                    )
                )
                .scalars()
                .all()
            )
            con = duck.connect(settings.storage)
            urls = ", ".join(
                f"'{duck.s3_url(settings.storage.bucket, p['object_key'])}'" for p in partitions
            )
            present = {
                r[0]
                for r in con.execute(
                    f"SELECT DISTINCT feature_name FROM read_parquet([{urls}])"
                ).fetchall()
            }
            for name in sorted(present - claimed):
                drift.append(f"{group['name']}: parquet holds {name!r}, registry has no row")
    return drift


# --------------------------------------------------------------------------
# undo


class UndoError(RuntimeError):
    """Rolling the run back would have broken something that depends on it."""


def undo(engine: sa.Engine, settings: Settings, run_id: int, *, s3=None) -> dict[str, Any]:
    """Delete a run's outputs and mark it rolled back; the run row itself stays.

    Refuses, naming the dependant, when a later run read the outputs or the run
    froze a dataset version. See ``docs/adr/0008``.
    """
    s3 = s3 or objects.client(settings.storage)
    bucket = settings.storage.bucket

    with engine.connect() as connection:
        run_row = (
            connection.execute(sa.select(run_table).where(run_table.c.id == run_id))
            .mappings()
            .first()
        )
        if run_row is None:
            raise UndoError(f"no run {run_id}")
        if run_row["status"] == "rolled_back":
            raise UndoError(f"run {run_id} was already rolled back")

        outputs = connection.execute(
            sa.select(run_output.c.ref_type, run_output.c.ref_id).where(
                run_output.c.run_id == run_id
            )
        ).all()
        if not outputs:
            raise UndoError(f"run {run_id} produced nothing to undo")

        dependants = (
            connection.execute(
                sa.select(run_input.c.run_id)
                .where(
                    sa.tuple_(run_input.c.ref_type, run_input.c.ref_id).in_(
                        [(t, i) for t, i in outputs]
                    ),
                    run_input.c.run_id != run_id,
                )
                .distinct()
            )
            .scalars()
            .all()
        )
        if dependants:
            raise UndoError(
                f"run {run_id}'s outputs were read by run(s) {sorted(dependants)}. "
                "Undo those first, or leave this one in place: the alternative is a "
                "lineage edge pointing at something that no longer exists."
            )

        version_ids = [i for t, i in outputs if t == "dataset_version"]
        pinned = (
            connection.execute(
                sa.select(dataset_version.c.id).where(
                    dataset_version.c.id.in_(version_ids) if version_ids else sa.false()
                )
            )
            .scalars()
            .all()
        )
        if pinned:
            raise UndoError(
                f"run {run_id} froze dataset version(s) {sorted(pinned)}. Frozen versions "
                "are immutable; undoing this would leave every pin to them unresolvable."
            )

        partition_ids = [i for t, i in outputs if t == "feature_partition"]
        release_ids = [i for t, i in outputs if t == "release"]
        keys = (
            connection.execute(
                sa.select(feature_partition.c.object_key).where(
                    feature_partition.c.id.in_(partition_ids)
                )
            )
            .scalars()
            .all()
            if partition_ids
            else []
        )
        keys += (
            connection.execute(sa.select(release.c.object_key).where(release.c.id.in_(release_ids)))
            .scalars()
            .all()
            if release_ids
            else []
        )

    # Catalog first, inverting the write order: a crash here leaves an orphan, never a dangling row.
    with engine.begin() as connection:
        if partition_ids:
            connection.execute(
                sa.delete(feature_partition).where(feature_partition.c.id.in_(partition_ids))
            )
        if release_ids:
            connection.execute(sa.delete(release).where(release.c.id.in_(release_ids)))
        connection.execute(sa.delete(run_output).where(run_output.c.run_id == run_id))
        connection.execute(
            sa.update(run_table).where(run_table.c.id == run_id).values(status="rolled_back")
        )

    removed = []
    for key in keys:
        objects.delete(s3, bucket, key)
        removed.append(key)
    return {"run_id": run_id, "objects_removed": removed, "partitions": len(partition_ids)}


# --------------------------------------------------------------------------
# prune


def prune_release(engine: sa.Engine, settings: Settings, digest_prefix: str, *, s3=None) -> dict:
    """Drop a release's bytes, keeping the row as a tombstone; never called automatically."""
    s3 = s3 or objects.client(settings.storage)
    with engine.connect() as connection:
        found = catalog.find_release(connection, digest_prefix)
    if found is None:
        raise UndoError(f"no release whose digest starts with {digest_prefix!r}")
    if found["pruned_at"] is not None:
        raise UndoError(f"release {found['sha256'][:12]} is already pruned")

    with engine.begin() as connection:
        connection.execute(
            sa.update(release).where(release.c.id == found["id"]).values(pruned_at=sa.func.now())
        )
    objects.delete(s3, settings.storage.bucket, found["object_key"])
    return {"sha256": found["sha256"], "object_key": found["object_key"]}


def prune_orphans(
    engine: sa.Engine,
    settings: Settings,
    *,
    s3=None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Delete exactly the objects :func:`verify` lists as orphans.

    Refuses while any run is ``running``, because invariant 3 makes an uploaded
    but unrecorded part look like an orphan, and refuses when the catalog and
    store otherwise disagree.
    """
    s3 = s3 or objects.client(settings.storage)

    with engine.connect() as connection:
        in_flight = (
            connection.execute(
                sa.select(run_table.c.id, run_table.c.kind, run_table.c.name)
                .where(run_table.c.status == "running")
                .order_by(run_table.c.id)
            )
            .mappings()
            .all()
        )
    if in_flight:
        raise UndoError(
            f"{len(in_flight)} run(s) still in progress "
            f"({', '.join(str(r['id']) for r in in_flight[:5])}). Bytes are written before "
            "their catalog row, so a part that has landed and not yet been recorded looks "
            "exactly like an orphan. Wait for them to finish, or mark them failed if they "
            "died, and run this again."
        )

    found = verify(engine, settings, orphans=True, s3=s3)
    if not found.ok:
        raise UndoError(
            "the catalog and the store disagree "
            f"({len(found.missing)} missing, {len(found.wrong_size)} wrong size, "
            f"{len(found.wrong_digest)} wrong digest, {len(found.registry_drift)} drift). "
            "Refusing to delete anything until that is understood: an orphan list computed "
            "against a store in this state is not one to act on."
        )

    if dry_run:
        return {"would_remove": list(found.orphans), "removed": [], "checked": found.checked}

    removed = []
    for key in found.orphans:
        objects.delete(s3, settings.storage.bucket, key)
        removed.append(key)
    return {"would_remove": [], "removed": removed, "checked": found.checked}
