"""verify, undo and prune: the operations that delete things.

These are the only commands in the store that destroy data, which makes their
*refusals* the part worth testing hardest. A wrong answer is recoverable by
re-running; a wrong deletion is not.

`verify` is here too because it is the other half of invariant 3. Writing bytes
before the catalog row is only the recoverable direction if something actually
recovers — an orphan nobody finds is a slow leak, and "findable in principle"
is how a bucket accumulates years of debris.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

import pytest
import sqlalchemy as sa

from feature_store.store import catalog, compose, ingest, objects, verify
from feature_store.store.runs import run as open_run
from feature_store.store.tables import feature_partition
from feature_store.store.tables import release as release_table
from feature_store.store.tables import run as run_table

from ..pit import panel

pytestmark = pytest.mark.integration


def _facts(feature: str = "gdp") -> list[panel.Fact]:
    return [
        panel.Fact("AAA", date(2020, 12, 31), datetime(2021, 3, 1), feature, 1.0),
        panel.Fact("AAA", date(2021, 12, 31), datetime(2022, 3, 1), feature, 2.0),
    ]


@pytest.fixture
def a_release(engine, settings, s3, tmp_path):
    """A registered source with one stored release."""
    tag = uuid.uuid4().hex[:8]
    with open_run(engine, "ingest_release", "fixture", {}) as run:
        catalog.ensure_namespace(run, panel.NAMESPACE)
    ingest.register_source(
        engine,
        settings,
        name=f"src_{tag}",
        native_namespace=panel.NAMESPACE,
        granularity="annual",
    )
    path = tmp_path / "panel.csv"
    path.write_text(f"code,date,gdp\n# {tag}\nAAA,2020-12-31,1.0\n")
    return ingest.add_release(
        engine, settings, path=path, source_name=f"src_{tag}", released_at=date(2025, 3, 12), s3=s3
    )


class TestVerify:
    def test_a_clean_store_agrees(self, engine, settings, s3, a_release):
        found = verify.verify(engine, settings, s3=s3)
        assert found.ok
        assert found.checked > 0, "and actually looked at something"

    def test_a_deleted_object_is_reported_missing(self, engine, settings, s3, a_release):
        """The catalog claiming bytes that are not there is the bad direction."""
        objects.delete(s3, settings.storage.bucket, a_release.object_key)
        found = verify.verify(engine, settings, s3=s3)
        assert a_release.object_key in found.missing
        assert not found.ok

    def test_an_orphan_is_found(self, engine, settings, s3):
        """Invariant 3's recovery story, exercised rather than assumed.

        A run that dies between uploading and writing its row leaves exactly
        this: an object under a store prefix that no catalog row points at.
        """
        key = f"features/orphan_{uuid.uuid4().hex[:8]}/entity_namespace=x/part-dead.parquet"
        objects.put_bytes(s3, settings.storage.bucket, key, b"not really parquet")
        try:
            found = verify.verify(engine, settings, orphans=True, s3=s3)
            assert key in found.orphans
        finally:
            objects.delete(s3, settings.storage.bucket, key)

    def test_a_truncated_object_is_reported(self, engine, settings, s3, a_release):
        """Present but wrong is worse than absent, because it reads fine."""
        objects.put_bytes(s3, settings.storage.bucket, a_release.object_key, b"short")
        found = verify.verify(engine, settings, s3=s3)
        assert any(a_release.object_key in item for item in found.wrong_size)

    def test_deep_catches_bytes_that_do_not_hash(self, engine, settings, s3, a_release):
        """Same length, different content: only a re-hash notices."""
        original = objects.head(s3, settings.storage.bucket, a_release.object_key)
        objects.put_bytes(
            s3,
            settings.storage.bucket,
            a_release.object_key,
            b"x" * original["ContentLength"],
        )
        found = verify.verify(engine, settings, deep=True, s3=s3)
        assert any(a_release.object_key in item for item in found.wrong_digest)

    def test_registry_drift_is_reported(self, engine, settings, s3):
        """The feature registry is a cache of what the files hold, and caches lie."""
        from feature_store.store.tables import feature as feature_table
        from feature_store.store.tables import feature_group

        name = panel.build_store(engine, settings, s3, _facts())
        with engine.begin() as connection:
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == name)
            ).scalar_one()
            connection.execute(
                sa.insert(feature_table).values(
                    feature_group_id=group_id,
                    name="never_written",
                    dtype="num",
                    created_by_run_id=connection.execute(
                        sa.select(sa.func.max(run_table.c.id))
                    ).scalar_one(),
                )
            )
        found = verify.verify(engine, settings, deep=True, s3=s3)
        assert any("never_written" in item for item in found.registry_drift)

    def test_scoping_to_one_run_checks_only_its_outputs(self, engine, settings, s3, a_release):
        whole = verify.verify(engine, settings, s3=s3)
        scoped = verify.verify(engine, settings, run_id=a_release.run_id, s3=s3)
        assert scoped.checked == 1
        assert scoped.checked < whole.checked


class TestUndo:
    def test_it_removes_the_partition_and_its_object(self, engine, settings, s3):
        name = panel.build_store(engine, settings, s3, _facts())
        with engine.connect() as connection:
            from feature_store.store.tables import feature_group

            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == name)
            ).scalar_one()
            partitions = catalog.live_partitions(connection, group_id)
            run_id = partitions[0]["created_by_run_id"]
            key = partitions[0]["object_key"]

        verify.undo(engine, settings, run_id, s3=s3)

        assert not objects.exists(s3, settings.storage.bucket, key)
        with engine.connect() as connection:
            assert catalog.live_partitions(connection, group_id) == []
            status = connection.execute(
                sa.select(run_table.c.status).where(run_table.c.id == run_id)
            ).scalar_one()
        assert status == "rolled_back"

    def test_the_run_row_survives(self, engine, settings, s3):
        """That it happened is a fact. A catalog that erases its own history is
        exactly the fiction lineage exists to prevent."""
        name = panel.build_store(engine, settings, s3, _facts())
        with engine.connect() as connection:
            run_id = connection.execute(
                sa.select(sa.func.max(run_table.c.id))
            ).scalar_one()
        verify.undo(engine, settings, run_id, s3=s3)
        with engine.connect() as connection:
            row = connection.execute(
                sa.select(run_table).where(run_table.c.id == run_id)
            ).mappings().one()
        assert row["name"].startswith("pit fixture")

    def test_it_refuses_when_something_read_the_outputs(self, engine, settings, s3):
        """A dangling lineage edge is worse than an undeleted partition."""
        name = panel.build_store(engine, settings, s3, _facts())
        with engine.connect() as connection:
            from feature_store.store.tables import feature_group

            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == name)
            ).scalar_one()
            partition = catalog.live_partitions(connection, group_id)[0]

        with open_run(engine, "compose", "downstream reader", {}) as reader:
            reader.read("feature_partition", partition["id"])

        with pytest.raises(verify.UndoError, match="were read by run"):
            verify.undo(engine, settings, partition["created_by_run_id"], s3=s3)

    def test_it_refuses_to_unmake_a_frozen_version(self, engine, settings, s3):
        """Undoing this would leave every pin to it unresolvable."""
        from feature_store.spec.compose import ComposeSpec

        name = panel.build_store(engine, settings, s3, _facts())
        spec = ComposeSpec.model_validate(
            {
                "name": f"frozen_{uuid.uuid4().hex[:8]}",
                "spine": {
                    "entities": {"ids": ["AAA"]},
                    "times": {"dates": ["2022-06-01"]},
                },
                "features": [{"group": name, "name": "gdp"}],
                "as_of": {"policy": "spine_time"},
            }
        )
        frozen = compose.freeze(engine, settings, spec, s3=s3)
        with engine.connect() as connection:
            run_id = connection.execute(
                sa.select(run_table.c.id)
                .where(run_table.c.name.like(f"freeze {spec.name}%"))
            ).scalar_one()
        with pytest.raises(verify.UndoError, match="[Ff]rozen"):
            verify.undo(engine, settings, run_id, s3=s3)
        assert frozen.version_num == 1

    def test_a_run_that_produced_nothing_cannot_be_undone(self, engine, settings, s3):
        with open_run(engine, "compose", "did nothing", {}) as run:
            run_id = run.id
        with pytest.raises(verify.UndoError, match="produced nothing"):
            verify.undo(engine, settings, run_id, s3=s3)

    def test_undoing_twice_is_refused(self, engine, settings, s3):
        name = panel.build_store(engine, settings, s3, _facts())
        with engine.connect() as connection:
            run_id = connection.execute(sa.select(sa.func.max(run_table.c.id))).scalar_one()
        verify.undo(engine, settings, run_id, s3=s3)
        with pytest.raises(verify.UndoError, match="already rolled back"):
            verify.undo(engine, settings, run_id, s3=s3)

    def test_an_unknown_run_says_so(self, engine, settings, s3):
        with pytest.raises(verify.UndoError, match="no run"):
            verify.undo(engine, settings, 999_999, s3=s3)


class TestPrune:
    def test_it_removes_the_bytes_and_keeps_the_row(self, engine, settings, s3, a_release):
        """A tombstone, so lineage still resolves and the file stays identifiable."""
        verify.prune_release(engine, settings, a_release.sha256[:12], s3=s3)

        assert not objects.exists(s3, settings.storage.bucket, a_release.object_key)
        with engine.connect() as connection:
            row = connection.execute(
                sa.select(release_table).where(release_table.c.id == a_release.release_id)
            ).mappings().one()
        assert row["pruned_at"] is not None
        assert row["sha256"] == a_release.sha256

    def test_a_pruned_release_cannot_be_fetched(self, engine, settings, s3, a_release, tmp_path):
        """And says why, rather than failing on a missing object."""
        verify.prune_release(engine, settings, a_release.sha256[:12], s3=s3)
        with pytest.raises(catalog.CatalogError, match="pruned"):
            ingest.fetch_release(
                engine,
                settings,
                digest_prefix=a_release.sha256[:12],
                destination=tmp_path / "back.csv",
                s3=s3,
            )

    def test_pruning_twice_is_refused(self, engine, settings, s3, a_release):
        verify.prune_release(engine, settings, a_release.sha256[:12], s3=s3)
        with pytest.raises(verify.UndoError, match="already pruned"):
            verify.prune_release(engine, settings, a_release.sha256[:12], s3=s3)

    def test_a_pruned_release_is_not_reported_missing(self, engine, settings, s3, a_release):
        """The bytes are gone on purpose; verify must not call that a fault."""
        verify.prune_release(engine, settings, a_release.sha256[:12], s3=s3)
        found = verify.verify(engine, settings, s3=s3)
        assert a_release.object_key not in found.missing

    def test_an_unknown_digest_says_so(self, engine, settings, s3):
        with pytest.raises(verify.UndoError, match="no release"):
            verify.prune_release(engine, settings, "deadbeef", s3=s3)


def test_a_partition_orphaned_by_undo_is_found_by_verify(engine, settings, s3):
    """The two halves of the recovery story, meeting.

    Undo deletes the catalog row before the object, deliberately inverting the
    write order: a crash between them leaves an orphan, which is the same
    recoverable state a failed write leaves. This checks that verify sees it.
    """
    name = panel.build_store(engine, settings, s3, _facts())
    with engine.connect() as connection:
        from feature_store.store.tables import feature_group

        group_id = connection.execute(
            sa.select(feature_group.c.id).where(feature_group.c.name == name)
        ).scalar_one()
        partition = catalog.live_partitions(connection, group_id)[0]

    # Simulate the crash: drop the row, leave the object.
    with engine.begin() as connection:
        connection.execute(
            sa.delete(feature_partition).where(feature_partition.c.id == partition["id"])
        )
    found = verify.verify(engine, settings, orphans=True, s3=s3)
    assert partition["object_key"] in found.orphans
    objects.delete(s3, settings.storage.bucket, partition["object_key"])
