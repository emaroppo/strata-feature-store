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
def forget(engine, s3, settings):
    """Drop a release row and its object, so a damaged one stops being damage.

    Several tests below deliberately break an object to prove verify notices.
    Left in place, that damage persists for the rest of the session — and
    `prune_orphans` then refuses, correctly, because an orphan list computed
    against a store nobody understands is not one to act on. A test that breaks
    something on purpose has to put it back.
    """

    def clean(release_id: int, key: str) -> None:
        with engine.begin() as connection:
            connection.execute(
                sa.delete(release_table).where(release_table.c.id == release_id)
            )
        objects.delete(s3, settings.storage.bucket, key)

    return clean


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

    def test_a_deleted_object_is_reported_missing(self, engine, settings, s3, a_release, forget):
        """The catalog claiming bytes that are not there is the bad direction."""
        objects.delete(s3, settings.storage.bucket, a_release.object_key)
        try:
            found = verify.verify(engine, settings, s3=s3)
            assert a_release.object_key in found.missing
            assert not found.ok
        finally:
            forget(a_release.release_id, a_release.object_key)

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

    def test_a_truncated_object_is_reported(self, engine, settings, s3, a_release, forget):
        """Present but wrong is worse than absent, because it reads fine."""
        objects.put_bytes(s3, settings.storage.bucket, a_release.object_key, b"short")
        try:
            found = verify.verify(engine, settings, s3=s3)
            assert any(a_release.object_key in item for item in found.wrong_size)
        finally:
            forget(a_release.release_id, a_release.object_key)

    def test_deep_catches_bytes_that_do_not_hash(self, engine, settings, s3, a_release, forget):
        """Same length, different content: only a re-hash notices."""
        original = objects.head(s3, settings.storage.bucket, a_release.object_key)
        objects.put_bytes(
            s3,
            settings.storage.bucket,
            a_release.object_key,
            b"x" * original["ContentLength"],
        )
        try:
            found = verify.verify(engine, settings, deep=True, s3=s3)
            assert any(a_release.object_key in item for item in found.wrong_digest)
        finally:
            forget(a_release.release_id, a_release.object_key)

    def test_a_fact_the_registry_does_not_know_is_drift(self, engine, settings, s3):
        # Leaves drift behind on purpose; see TestPruneOrphans, which runs after
        # and asserts that prune refuses while the catalog disagrees.
        """The direction that matters: data the vocabulary has lost track of.

        Nothing can discover the feature, and a spec naming it is refused even
        though the facts are sitting there.
        """
        from feature_store.store.tables import feature as feature_table
        from feature_store.store.tables import feature_group

        name = panel.build_store(engine, settings, s3, _facts("orphaned_feature"))
        with engine.begin() as connection:
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == name)
            ).scalar_one()
            run_id = connection.execute(sa.select(sa.func.max(run_table.c.id))).scalar_one()
            connection.execute(
                sa.delete(feature_table).where(
                    feature_table.c.feature_group_id == group_id,
                    feature_table.c.name == "orphaned_feature",
                )
            )
        try:
            found = verify.verify(engine, settings, deep=True, s3=s3)
            assert any("orphaned_feature" in item for item in found.registry_drift)
            assert not found.ok
        finally:
            # Put the vocabulary back. Drift left behind is drift every later
            # test sees, and prune_orphans refuses while the catalog disagrees.
            with engine.begin() as connection:
                connection.execute(
                    sa.insert(feature_table).values(
                        feature_group_id=group_id,
                        name="orphaned_feature",
                        dtype="num",
                        created_by_run_id=run_id,
                    )
                )

    def test_a_registry_row_without_facts_is_not_drift(self, engine, settings, s3):
        """The ordinary state of a feature nobody has published yet.

        Standardise registers a column from the source's schema whether or not
        this release carried values for it, so a wide panel's unmapped sibling
        is almost entirely rows like this. Reporting them would flag correct
        behaviour as a fault, on a scale that buries the real findings — which
        is exactly what it did the first time verify ran against the real store.
        """
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
                    name="not_yet_published",
                    dtype="num",
                    created_by_run_id=connection.execute(
                        sa.select(sa.func.max(run_table.c.id))
                    ).scalar_one(),
                )
            )
        found = verify.verify(engine, settings, deep=True, s3=s3)
        assert not any("not_yet_published" in item for item in found.registry_drift)

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


class TestPruneOrphans:
    """Deleting debris, and refusing to when the picture is not trustworthy."""

    def _plant(self, s3, settings, suffix: str = "") -> str:
        key = (
            f"features/orphan_{uuid.uuid4().hex[:8]}{suffix}/"
            f"entity_namespace=x/part-dead.parquet"
        )
        objects.put_bytes(s3, settings.storage.bucket, key, b"debris")
        return key

    def test_it_removes_what_verify_listed(self, engine, settings, s3, a_release):
        """Every planted orphan goes. Other tests leave their own debris in the
        shared bucket, so this checks containment rather than equality — the
        claim is that nothing planted survives, not that nothing else existed.
        """
        planted = [self._plant(s3, settings) for _ in range(3)]
        result = verify.prune_orphans(engine, settings, s3=s3)

        assert set(planted) <= set(result["removed"])
        for key in planted:
            assert not objects.exists(s3, settings.storage.bucket, key)

    def test_catalogued_objects_are_untouched(self, engine, settings, s3, a_release):
        """The failure that would matter: deleting something live."""
        self._plant(s3, settings)
        verify.prune_orphans(engine, settings, s3=s3)
        assert objects.exists(s3, settings.storage.bucket, a_release.object_key)
        assert verify.verify(engine, settings, s3=s3).ok

    def test_a_dry_run_deletes_nothing(self, engine, settings, s3, a_release):
        key = self._plant(s3, settings)
        result = verify.prune_orphans(engine, settings, s3=s3, dry_run=True)
        assert key in result["would_remove"]
        assert result["removed"] == []
        assert objects.exists(s3, settings.storage.bucket, key)

    def test_it_refuses_while_a_run_is_in_progress(self, engine, settings, s3, a_release):
        """The refusal that matters most.

        Bytes are written before their catalog row, so a part that has landed
        and not yet been recorded is indistinguishable from an orphan by
        inspection. Pruning during a standardise would delete the work it is in
        the middle of doing.
        """
        self._plant(s3, settings)
        with open_run(engine, "standardise", "still going", {}) as run:
            run.wrote("feature_partition", 999_123)
            with pytest.raises(verify.UndoError, match="still in progress"):
                verify.prune_orphans(engine, settings, s3=s3)

    def test_a_finished_run_does_not_block_it(self, engine, settings, s3, a_release):
        key = self._plant(s3, settings)
        with open_run(engine, "standardise", "done", {}) as run:
            run.wrote("feature_partition", 999_124)
        result = verify.prune_orphans(engine, settings, s3=s3)
        assert key in result["removed"]

    def test_it_refuses_when_the_catalog_disagrees(
        self, engine, settings, s3, a_release, forget
    ):
        """An orphan list computed against a broken store is not one to act on."""
        self._plant(s3, settings)
        objects.delete(s3, settings.storage.bucket, a_release.object_key)
        try:
            with pytest.raises(verify.UndoError, match="disagree"):
                verify.prune_orphans(engine, settings, s3=s3)
        finally:
            # The damage was the point of the test, so it has to go with it.
            forget(a_release.release_id, a_release.object_key)

    def test_debris_from_a_failed_run_is_removed(self, engine, settings, s3, a_release):
        """Those runs are over, and what they left is what this exists to clear."""
        key = self._plant(s3, settings)
        with pytest.raises(ValueError):
            with open_run(engine, "standardise", "died", {}) as run:
                run.wrote("feature_partition", 999_125)
                raise ValueError("mid-write")
        result = verify.prune_orphans(engine, settings, s3=s3)
        assert key in result["removed"]

    def test_nothing_to_do_is_not_an_error(self, engine, settings, s3, a_release):
        verify.prune_orphans(engine, settings, s3=s3)
        assert verify.prune_orphans(engine, settings, s3=s3)["removed"] == []


class TestEveryCatalogueTableIsCovered:
    """A table that stores objects and is missing from `_catalogued` loses data.

    Anything absent from that list is invisible to the consistency checks *and*
    is classified as an orphan, because an orphan is defined as a key nothing in
    the list points at. So the consequence of forgetting a table is not reduced
    coverage — it is `--prune-orphans` deleting live data.

    `dataset_version.object_prefix` was missing from the first version, which
    made every crosswalk parquet and every materialised composition look like
    debris. Three were deleted before anything noticed, and the store then
    failed to resolve a crosswalk it still held a pinned, immutable version row
    for.
    """

    def test_a_crosswalk_parquet_is_not_an_orphan(self, engine, settings, s3, tmp_path):
        from feature_store.store import crosswalk

        tag = uuid.uuid4().hex[:8]
        with open_run(engine, "ingest_release", "fixture", {}) as run:
            catalog.ensure_namespace(run, panel.NAMESPACE)
        ingest.register_source(
            engine,
            settings,
            name=f"cw_src_{tag}",
            native_namespace=panel.NAMESPACE,
            granularity="irregular",
            kind="crosswalk",
        )
        path = tmp_path / "cw.csv"
        path.write_text(
            "source_id,target_id,valid_from,valid_to,confidence\n"
            f"AAA,AAA,1900-01-01,9999-12-31,1.0\n# {tag}\n"
        )
        release = ingest.add_release(
            engine, settings, path=path, source_name=f"cw_src_{tag}", s3=s3
        )
        version = crosswalk.register(
            engine, settings, name=f"cw_{tag}", digest_prefix=release.sha256[:12], s3=s3
        )

        found = verify.verify(engine, settings, orphans=True, s3=s3)
        assert version.object_key not in found.orphans, (
            "a catalogued crosswalk classified as debris would be deleted by --prune-orphans"
        )
        assert version.object_key not in found.missing

    def test_a_materialised_composition_is_not_an_orphan(self, engine, settings, s3):
        from feature_store.spec.compose import ComposeSpec

        name = panel.build_store(engine, settings, s3, _facts())
        spec = ComposeSpec.model_validate(
            {
                "name": f"mat_{uuid.uuid4().hex[:8]}",
                "spine": {
                    "entities": {"ids": ["AAA"]},
                    "times": {"dates": ["2022-06-01"]},
                },
                "features": [{"group": name, "name": "gdp"}],
                "as_of": {"policy": "spine_time"},
                "freeze": {"mode": "materialized"},
            }
        )
        frozen = compose.freeze(engine, settings, spec, s3=s3)
        found = verify.verify(engine, settings, orphans=True, s3=s3)
        assert frozen.object_prefix not in found.orphans
        assert frozen.object_prefix not in found.missing

    def test_prune_would_not_remove_them(self, engine, settings, s3, a_release):
        """The consequence, stated directly rather than inferred."""
        from feature_store.store.tables import dataset_version

        with engine.connect() as connection:
            catalogued = set(
                connection.execute(
                    sa.select(dataset_version.c.object_prefix).where(
                        dataset_version.c.object_prefix.isnot(None)
                    )
                ).scalars().all()
            )
        assert catalogued, "nothing to check; the fixture should have frozen something"
        preview = verify.prune_orphans(engine, settings, s3=s3, dry_run=True)
        assert not (catalogued & set(preview["would_remove"]))
