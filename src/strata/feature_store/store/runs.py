"""Runs: the unit every catalog write attaches to, so lineage is never opt-in (invariant 4).

A run is many short transactions, not one; visibility follows the run's status,
so a half-finished run's work survives a crash and stays invisible to readers.
Invariant 1, outputs never knowing more than inputs, is checked at close.
"""

from __future__ import annotations

import contextlib
import json
import os
import platform
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from strata.feature_store.spec.canonical import canonical_bytes, content_hash, to_jsonable
from strata.feature_store.store.tables import run as run_table
from strata.feature_store.store.tables import run_input, run_output

MANIFEST_PREFIX = "runs"


class LineageError(RuntimeError):
    """An invariant about what a run read and produced did not hold."""


@dataclass(slots=True)
class Run:
    """A unit of work, already recorded, that catalog writes attach themselves to.

    Constructed only by :func:`run`; ``connection`` is set only inside :meth:`transaction`.
    """

    id: int
    kind: str
    name: str
    engine: sa.Engine
    params: dict[str, Any]
    connection: sa.Connection | None = None
    _inputs: list[tuple[str, int, str]] = field(default_factory=list)
    _outputs: list[tuple[str, int, str]] = field(default_factory=list)
    _max_knowledge_time: datetime | None = None
    _notes: dict[str, Any] = field(default_factory=dict)

    @contextmanager
    def transaction(self) -> Iterator[sa.Connection]:
        """A short transaction, reentrant so nested writes share one."""
        if self.connection is not None:
            yield self.connection
            return
        with self.engine.begin() as connection:
            self.connection = connection
            try:
                yield connection
            finally:
                self.connection = None

    def read(self, ref_type: str, ref_id: int, role: str = "input") -> None:
        """Record that this run read something."""
        self._inputs.append((ref_type, ref_id, role))
        with self.transaction() as connection:
            connection.execute(
                pg_insert(run_input)
                .values(run_id=self.id, ref_type=ref_type, ref_id=ref_id, role=role)
                .on_conflict_do_nothing()
            )

    def wrote(self, ref_type: str, ref_id: int, role: str = "output") -> None:
        """Record that this run produced something."""
        self._outputs.append((ref_type, ref_id, role))
        with self.transaction() as connection:
            connection.execute(
                pg_insert(run_output)
                .values(run_id=self.id, ref_type=ref_type, ref_id=ref_id, role=role)
                .on_conflict_do_nothing()
            )

    def observed_knowledge_time(self, when: datetime | None) -> None:
        """Raise this run's high-water mark, used by invariant 1."""
        if when is None:
            return
        if self._max_knowledge_time is None or when > self._max_knowledge_time:
            self._max_knowledge_time = when

    def note(self, **values: Any) -> None:
        """Attach counts and decisions to the manifest. See ``strata/docs/adr/0036``."""
        self._notes.update(values)

    @property
    def notes(self) -> dict[str, Any]:
        return dict(self._notes)


def git_state(root: str | None = None) -> tuple[str | None, bool]:
    """The commit this ran from and whether the tree was dirty; ``(None, False)`` outside git."""
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=root,
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain"],
                capture_output=True,
                text=True,
                check=True,
                cwd=root,
            ).stdout.strip()
        )
        return sha, dirty
    except (subprocess.CalledProcessError, FileNotFoundError):  # pragma: no cover
        return None, False


def environment() -> dict[str, Any]:
    """What produced this, to the precision that matters for reproducing it."""
    versions: dict[str, str] = {}
    for module in ("duckdb", "pyarrow", "polars", "sqlalchemy"):
        with contextlib.suppress(Exception):  # absent is a fine answer
            versions[module] = __import__(module).__version__
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "host": platform.node(),
        "user": os.environ.get("USER"),
        "packages": versions,
    }


def _naive_utc_now() -> datetime:
    """Naive UTC, matching every timestamp in the store."""
    return datetime.now(UTC).replace(tzinfo=None)


def _inputs_max_knowledge_time(connection: sa.Connection, run_id: int) -> datetime | None:
    """The greatest knowledge_time reached by the runs that produced this run's inputs."""
    # A reference matches on both halves: ids are only unique within a ref_type.
    producing = (
        sa.select(run_output.c.run_id)
        .join(
            run_input,
            sa.and_(
                run_input.c.ref_type == run_output.c.ref_type,
                run_input.c.ref_id == run_output.c.ref_id,
            ),
        )
        .where(run_input.c.run_id == run_id)
    )
    return connection.execute(
        sa.select(sa.func.max(run_table.c.max_knowledge_time)).where(run_table.c.id.in_(producing))
    ).scalar_one_or_none()


@contextmanager
def run(
    engine: sa.Engine,
    kind: str,
    name: str,
    params: dict[str, Any] | None = None,
    *,
    s3=None,
    bucket: str | None = None,
    check_monotonic: bool = True,
) -> Iterator[Run]:
    """Open a run, yield it, and close it with its status and lineage recorded.

    The row is committed before the body runs, so a killed process leaves a
    ``running`` row for ``verify --run`` to reconcile. Raises ``LineageError``
    and marks the run failed when its outputs know more than its inputs.
    """
    params = params or {}
    sha, dirty = git_state()
    started = _naive_utc_now()
    with engine.begin() as connection:
        run_id = connection.execute(
            sa.insert(run_table)
            .values(
                kind=kind,
                name=name,
                config_hash=content_hash(params),
                code_git_sha=sha,
                code_dirty=int(dirty),
                # Through the canonical form, so what is stored is exactly what config_hash covers.
                params=to_jsonable(params),
                env=to_jsonable(environment()),
                status="running",
                started_at=started,
            )
            .returning(run_table.c.id)
        ).scalar_one()

    current = Run(id=run_id, kind=kind, name=name, engine=engine, params=params)
    try:
        yield current
    except Exception:
        with engine.begin() as connection:
            connection.execute(
                sa.update(run_table)
                .where(run_table.c.id == run_id)
                .values(status="failed", finished_at=_naive_utc_now())
            )
        raise

    if check_monotonic and current._max_knowledge_time is not None:
        with engine.begin() as connection:
            inputs_max = _inputs_max_knowledge_time(connection, run_id)
        if inputs_max is not None and current._max_knowledge_time < inputs_max:
            with engine.begin() as connection:
                connection.execute(
                    sa.update(run_table)
                    .where(run_table.c.id == run_id)
                    .values(status="failed", finished_at=_naive_utc_now())
                )
            raise LineageError(
                f"run {run_id} ({kind}) produced output knowing only up to "
                f"{current._max_knowledge_time}, but its inputs already knew up to "
                f"{inputs_max}. Something read a value it should not have been able to see."
            )

    manifest_key = None
    if s3 is not None and bucket is not None:
        manifest_key = write_manifest(s3, bucket, current, started)

    # The last write is what makes the run's outputs visible to a reader.
    with engine.begin() as connection:
        connection.execute(
            sa.update(run_table)
            .where(run_table.c.id == run_id)
            .values(
                status="ok",
                finished_at=_naive_utc_now(),
                max_knowledge_time=current._max_knowledge_time,
                manifest_key=manifest_key,
            )
        )


def manifest(current: Run, started: datetime) -> dict[str, Any]:
    """The self-describing record of one run, written beside the bytes to outlive the catalog."""
    sha, dirty = git_state()
    return {
        "schema_version": 1,
        "run_id": current.id,
        "kind": current.kind,
        "name": current.name,
        "started_at": started,
        "config_hash": content_hash(current.params),
        "params": current.params,
        "code": {"git_sha": sha, "dirty": dirty},
        "env": environment(),
        "inputs": [{"ref_type": t, "ref_id": i, "role": r} for t, i, r in sorted(current._inputs)],
        "outputs": [
            {"ref_type": t, "ref_id": i, "role": r} for t, i, r in sorted(current._outputs)
        ],
        "max_knowledge_time": current._max_knowledge_time,
        "notes": current.notes,
    }


def manifest_key_for(current: Run, started: datetime) -> str:
    return f"{MANIFEST_PREFIX}/{started:%Y/%m}/{current.id:08d}/manifest.json"


def write_manifest(s3, bucket: str, current: Run, started: datetime) -> str:
    # Deferred: objects imports boto3, which catalog-only CLI commands never need.
    from strata.feature_store.store.objects import put_bytes

    key = manifest_key_for(current, started)
    payload = canonical_bytes(manifest(current, started))
    put_bytes(s3, bucket, key, payload, content_type="application/json")
    return key


def read_manifest(s3, bucket: str, key: str) -> dict[str, Any]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)
