"""Runs, and why lineage cannot be skipped.

Invariant 4 says lineage is automatic and never opt-in, and the enforcement is
the type signature rather than a convention. Every function that writes to the
catalog takes a :class:`Run` as its first argument, and a ``Run`` can only be
obtained from :func:`run`, which has already inserted the row before it yields.
There is therefore no code path that writes a catalog row without lineage — not
because writing one would be wrong, but because there is no way to express it.

The alternative, a ``record_lineage()`` call that writers are expected to make,
gets skipped exactly once and then always, and a catalog whose lineage is
partial is worse than one with none: it looks trustworthy.

**Invariant 1 is checked at the close of the run.** A run whose outputs claim a
``knowledge_time`` earlier than its inputs has looked into the future somewhere,
and the arithmetic catches most lookahead bugs that survive the join logic.

**A run is many transactions, not one.** Each write commits as it happens, and
the run row stays ``running`` until the whole thing finishes. Visibility comes
from the run's status rather than from an uncommitted transaction: a reader
filters to partitions whose creating run reached ``ok``, so a half-finished
ingest is invisible for the same reason it was before — but its work survives a
crash, its progress is observable, and the retry is cheap because an identical
part is already a no-op.

Holding one transaction open for the whole run bought that same visibility more
bluntly, at the cost of a twenty-minute snapshot, no progress, and discarding
every catalog row when the last batch failed.
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from feature_store.spec.canonical import canonical_bytes, content_hash, to_jsonable
from feature_store.store.tables import run as run_table
from feature_store.store.tables import run_input, run_output

MANIFEST_PREFIX = "runs"


class LineageError(RuntimeError):
    """An invariant about what a run read and produced did not hold."""


@dataclass(slots=True)
class Run:
    """A unit of work, already recorded, that catalog writes attach themselves to.

    Constructed only by :func:`run`. The ``id`` exists before any output does,
    which is what lets a write reference it without a second pass.

    Holds an engine rather than a connection, and opens a short transaction per
    write. ``connection`` is set only while inside :meth:`transaction`, so a
    catalog write attempted outside one fails immediately rather than silently
    joining somebody else's transaction.
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
        """A short transaction, reentrant so nested writes share one.

        Reentrancy matters: a caller that wraps several catalog writes in one
        transaction gets atomicity across them, while a lone write still commits
        on its own without the caller arranging anything.
        """
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
        # Recording the same read twice is harmless and happens naturally when a
        # run loops; the lineage edge is the fact, not how often it was noticed.
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
        """Attach counts and decisions to the manifest.

        This is where a standardise run says how many rows it wrote, how many
        entities it could not map, and how many values a release changed —
        the numbers that make an ingest reviewable after the fact rather than
        only at the moment it ran.
        """
        self._notes.update(values)

    @property
    def notes(self) -> dict[str, Any]:
        return dict(self._notes)


def git_state(root: str | None = None) -> tuple[str | None, bool]:
    """The commit this ran from, and whether the tree was dirty.

    A dirty tree means the recorded sha does not fully describe the code, so it
    is recorded as a fact rather than quietly ignored.
    """
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
        try:
            versions[module] = __import__(module).__version__
        except Exception:  # pragma: no cover - absent is a fine answer
            pass
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "host": platform.node(),
        "user": os.environ.get("USER"),
        "packages": versions,
    }


def _naive_utc_now() -> datetime:
    """Naive UTC, matching every timestamp in the store."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _inputs_max_knowledge_time(connection: sa.Connection, run_id: int) -> datetime | None:
    """The greatest knowledge_time reached by the runs that produced this run's inputs."""
    # Every run that produced something this run read. A reference matches on
    # both halves: ref_type alone would join a release id against a partition id
    # that happens to share a number.
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

    The row is committed before the body executes, so a process killed mid-run
    leaves a ``running`` row naming what it was doing, alongside whatever work it
    had already committed. ``verify --run`` is what reconciles those afterwards.
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
                # Through the canonical form, so a JSONB column never meets a
                # date its driver cannot serialise, and so what is stored is
                # exactly what config_hash was computed over.
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

    # The last write, and the one that makes everything this run produced
    # visible to a reader. Until it lands, the partitions exist and are ignored.
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
    """The self-describing record of one run.

    Written next to the bytes so an artifact stays interpretable when the
    catalog is gone: it carries the effective config, its hash, the code state,
    and what the run read and produced.
    """
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
    from feature_store.store.objects import put_bytes

    key = manifest_key_for(current, started)
    payload = canonical_bytes(manifest(current, started))
    put_bytes(s3, bucket, key, payload, content_type="application/json")
    return key


def read_manifest(s3, bucket: str, key: str) -> dict[str, Any]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)
