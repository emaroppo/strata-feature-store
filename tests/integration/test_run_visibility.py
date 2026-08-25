"""A run commits as it goes, and its work is invisible until it finishes.

The property being tested is the one that replaced a single long transaction.
Before, a standardise held one transaction for its whole duration, so nothing
was visible until it committed — atomic, but it discarded every catalog row when
the last batch failed, gave no progress, and held a snapshot open for twenty
minutes.

Now each partition commits as it is written, and a reader filters on the
creating run's status instead. The guarantee is the same and the failure mode is
better: the work survives, and the retry is cheap.
"""

from __future__ import annotations

import uuid
from datetime import datetime

import pytest
import sqlalchemy as sa

from feature_store.store import catalog
from feature_store.store.runs import LineageError
from feature_store.store.runs import run as open_run
from feature_store.store.tables import feature_group, feature_partition
from feature_store.store.tables import run as run_table

pytestmark = pytest.mark.integration

NAMESPACE = "vis-vocab"


@pytest.fixture
def group(engine: sa.Engine) -> int:
    """A feature group to hang partitions off."""
    suffix = uuid.uuid4().hex[:8]
    with open_run(engine, "ingest_release", "fixture", {}) as current:
        catalog.ensure_namespace(current, f"{NAMESPACE}-{suffix}")
        catalog.add_source(
            current,
            name=f"src_{suffix}",
            native_namespace=f"{NAMESPACE}-{suffix}",
            granularity="annual",
        )
        with current.transaction() as connection:
            source_id = catalog.get_source(connection, f"src_{suffix}")["id"]
            return connection.execute(
                sa.insert(feature_group)
                .values(
                    name=f"g_{suffix}",
                    source_id=source_id,
                    entity_namespace=f"{NAMESPACE}-{suffix}",
                    time_basis="file_release",
                    storage_prefix=f"features/g_{suffix}",
                    created_by_run_id=current.id,
                )
                .returning(feature_group.c.id)
            ).scalar_one()


def _add_partition(run, group_id: int, tag: str) -> int:
    with run.transaction() as connection:
        partition_id = connection.execute(
            sa.insert(feature_partition)
            .values(
                feature_group_id=group_id,
                object_key=f"features/x/{tag}-{uuid.uuid4().hex[:8]}.parquet",
                content_sha256="a" * 64,
                size_bytes=1,
                row_count=10,
                ingest_time=datetime(2026, 1, 1),
                created_by_run_id=run.id,
            )
            .returning(feature_partition.c.id)
        ).scalar_one()
    run.wrote("feature_partition", partition_id)
    return partition_id


def test_partitions_commit_before_the_run_finishes(engine, group):
    """Written work survives a crash, which the long transaction did not allow."""
    with open_run(engine, "standardise", "incremental", {}) as current:
        _add_partition(current, group, "one")
        # A separate connection cannot see uncommitted work. That it does see
        # this row is the point: the batch is durable already.
        with engine.connect() as observer:
            count = observer.execute(
                sa.select(sa.func.count())
                .select_from(feature_partition)
                .where(feature_partition.c.feature_group_id == group)
            ).scalar_one()
        assert count == 1, "the partition should be committed while the run is still open"


def test_a_running_run_has_no_live_partitions(engine, group):
    """Committed but not finished is not readable.

    This is what the single transaction used to provide. A group holding three
    of thirteen batches would otherwise answer queries from a quarter of the
    data, with nothing indicating it.
    """
    with open_run(engine, "standardise", "still going", {}) as current:
        _add_partition(current, group, "a")
        _add_partition(current, group, "b")
        with engine.connect() as observer:
            assert catalog.live_partitions(observer, group) == []

    # ...and visible the moment the run reaches ok.
    with engine.connect() as observer:
        assert len(catalog.live_partitions(observer, group)) == 2


def test_a_failed_runs_partitions_stay_invisible(engine, group):
    """The rows remain, for verify and undo to reconcile. Readers never see them."""
    with pytest.raises(ValueError):
        with open_run(engine, "standardise", "dies midway", {}) as current:
            _add_partition(current, group, "doomed")
            raise ValueError("batch 12 of 13")

    with engine.connect() as observer:
        assert catalog.live_partitions(observer, group) == []
        total = observer.execute(
            sa.select(sa.func.count())
            .select_from(feature_partition)
            .where(feature_partition.c.feature_group_id == group)
        ).scalar_one()
        assert total == 1, "the row survives; it is filtered, not deleted"
        status = observer.execute(
            sa.select(run_table.c.status).order_by(run_table.c.id.desc()).limit(1)
        ).scalar_one()
        assert status == "failed"


def test_a_superseded_partition_is_not_live(engine, group):
    """Corrections supersede rather than delete, so old pins keep resolving."""
    with open_run(engine, "standardise", "first", {}) as first:
        original = _add_partition(first, group, "v1")

    with open_run(engine, "standardise", "corrected", {}) as second:
        _add_partition(second, group, "v2")
        with second.transaction() as connection:
            connection.execute(
                sa.update(feature_partition)
                .where(feature_partition.c.id == original)
                .values(superseded_by_run_id=second.id)
            )

    with engine.connect() as observer:
        live = catalog.live_partitions(observer, group)
        assert len(live) == 1
        assert live[0]["id"] != original


def test_invariant_one_still_fails_the_run(engine, group):
    """Multiple transactions must not have loosened the monotonicity check."""
    with open_run(engine, "standardise", "upstream", {}) as up:
        up.observed_knowledge_time(datetime(2025, 3, 12))
        up.wrote("feature_partition", 999_001)

    with pytest.raises(LineageError, match="should not have been able to see"):
        with open_run(engine, "compose", "downstream", {}) as down:
            down.read("feature_partition", 999_001)
            down.observed_knowledge_time(datetime(2020, 1, 1))

    with engine.connect() as observer:
        status = observer.execute(
            sa.select(run_table.c.status).order_by(run_table.c.id.desc()).limit(1)
        ).scalar_one()
        assert status == "failed"
