"""The catalog's constraints, exercised against a real database.

A CHECK constraint that is never tested is a comment with extra steps. These
tests are about what the schema *refuses*, because every refusal here stands in
for a way the catalog could otherwise drift into describing data that is not
there.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
import sqlalchemy as sa

from feature_store.store.tables import (
    dataset,
    dataset_version,
    entity_namespace,
    feature,
    feature_group,
    release,
    run,
    source,
)

pytestmark = pytest.mark.integration

NAMESPACE = "iso3166-1-alpha3"


@pytest.fixture
def a_run(connection: sa.Connection) -> int:
    """Every write happens inside a run, so most tests need one first."""
    return connection.execute(
        sa.insert(run)
        .values(
            kind="standardise",
            name="test",
            config_hash="0" * 64,
            params={},
            env={},
            status="running",
        )
        .returning(run.c.id)
    ).scalar_one()


@pytest.fixture
def a_source(connection: sa.Connection) -> int:
    connection.execute(sa.insert(entity_namespace).values(name=NAMESPACE))
    return connection.execute(
        sa.insert(source)
        .values(
            name="source_a",
            kind="panel",
            native_namespace=NAMESPACE,
            granularity="annual",
        )
        .returning(source.c.id)
    ).scalar_one()


def _group_values(source_id: int, run_id: int, **overrides):
    values = {
        "name": "group_a",
        "source_id": source_id,
        "entity_namespace": NAMESPACE,
        "time_basis": "file_release",
        "period_anchor": "end",
        "retraction_policy": "honour",
        "storage_prefix": "features/group_a",
        "created_by_run_id": run_id,
    }
    return values | overrides


class TestVocabularies:
    """TEXT plus CHECK, so widening later is a cheap migration."""

    def test_granularity_must_be_known(self, connection, a_source):
        with pytest.raises(sa.exc.IntegrityError, match="ck_source_granularity"):
            connection.execute(
                sa.insert(source).values(
                    name="bad",
                    kind="panel",
                    native_namespace=NAMESPACE,
                    granularity="fortnightly",
                )
            )

    def test_time_basis_must_be_known(self, connection, a_source, a_run):
        with pytest.raises(sa.exc.IntegrityError, match="ck_feature_group_time_basis"):
            connection.execute(
                sa.insert(feature_group).values(
                    _group_values(a_source, a_run, time_basis="vibes")
                )
            )

    def test_retraction_policy_must_be_known(self, connection, a_source, a_run):
        with pytest.raises(sa.exc.IntegrityError, match="ck_feature_group_retraction_policy"):
            connection.execute(
                sa.insert(feature_group).values(
                    _group_values(a_source, a_run, retraction_policy="maybe")
                )
            )

    def test_dtype_must_be_num_or_str(self, connection, a_source, a_run):
        group_id = connection.execute(
            sa.insert(feature_group)
            .values(_group_values(a_source, a_run))
            .returning(feature_group.c.id)
        ).scalar_one()
        with pytest.raises(sa.exc.IntegrityError, match="ck_feature_dtype"):
            connection.execute(
                sa.insert(feature).values(
                    feature_group_id=group_id,
                    name="gdp_growth",
                    dtype="float64",
                    created_by_run_id=a_run,
                )
            )


class TestLagIffModeled:
    """A declared lag is meaningful for exactly one time basis.

    Declared on any other, it would be silently ignored, and the group would
    claim a rule it does not apply. Missing on ``modeled_lag``, there would be
    nothing to derive and the COALESCE would have no second argument.
    """

    def test_modeled_lag_requires_a_lag(self, connection, a_source, a_run):
        with pytest.raises(sa.exc.IntegrityError, match="lag_iff_modeled"):
            connection.execute(
                sa.insert(feature_group).values(
                    _group_values(a_source, a_run, time_basis="modeled_lag", declared_lag=None)
                )
            )

    def test_other_bases_refuse_a_lag(self, connection, a_source, a_run):
        with pytest.raises(sa.exc.IntegrityError, match="lag_iff_modeled"):
            connection.execute(
                sa.insert(feature_group).values(
                    _group_values(a_source, a_run, time_basis="file_release", declared_lag="P18M")
                )
            )

    def test_the_valid_combination_is_accepted(self, connection, a_source, a_run):
        connection.execute(
            sa.insert(feature_group).values(
                _group_values(a_source, a_run, time_basis="modeled_lag", declared_lag="P18M")
            )
        )


class TestContentAddressing:
    """The same bytes are the same release, whatever the publisher called the file."""

    def test_sha256_is_unique_across_the_store(self, connection, a_source, a_run):
        digest = "a" * 64
        for filename in ("indicators_2024-04.csv", "indicators.csv"):
            values = {
                "source_id": a_source,
                "sha256": digest,
                "object_key": f"sources/aa/aa/{digest}.csv",
                "size_bytes": 10,
                "original_filename": filename,
                "created_by_run_id": a_run,
            }
            if filename == "indicators.csv":
                with pytest.raises(sa.exc.IntegrityError, match="sha256"):
                    connection.execute(sa.insert(release).values(values))
            else:
                connection.execute(sa.insert(release).values(values))

    def test_released_at_and_retrieved_at_are_separate(self, connection, a_source, a_run):
        """Conflating them is the bug that makes every historical query empty."""
        connection.execute(
            sa.insert(release).values(
                source_id=a_source,
                sha256="b" * 64,
                object_key="sources/bb/bb/x.csv",
                size_bytes=10,
                original_filename="x.csv",
                released_at=date(2024, 4, 15),
                retrieved_at=datetime(2026, 8, 25, 12, 0),
                created_by_run_id=a_run,
            )
        )
        row = connection.execute(
            sa.select(release.c.released_at, release.c.retrieved_at).where(
                release.c.sha256 == "b" * 64
            )
        ).one()
        assert row.released_at == date(2024, 4, 15)
        assert row.retrieved_at.year == 2026


class TestDatasetVersions:
    def test_materialized_versions_must_name_their_bytes(self, connection, a_run):
        dataset_id = connection.execute(
            sa.insert(dataset).values(name="panel_a", kind="composed").returning(dataset.c.id)
        ).scalar_one()
        values = {
            "dataset_id": dataset_id,
            "version_num": 1,
            "resolution_mode": "materialized",
            "spec": {},
            "content_hash": "c" * 64,
            "ingest_watermark": datetime(2026, 1, 1),
            "object_prefix": None,
            "created_by_run_id": a_run,
        }
        with pytest.raises(sa.exc.IntegrityError, match="materialized_has_bytes"):
            connection.execute(sa.insert(dataset_version).values(values))

    def test_query_versions_need_no_bytes(self, connection, a_run):
        dataset_id = connection.execute(
            sa.insert(dataset).values(name="panel_b", kind="composed").returning(dataset.c.id)
        ).scalar_one()
        connection.execute(
            sa.insert(dataset_version).values(
                dataset_id=dataset_id,
                version_num=1,
                resolution_mode="query",
                spec={"name": "panel_b"},
                content_hash="d" * 64,
                ingest_watermark=datetime(2026, 1, 1),
                created_by_run_id=a_run,
            )
        )

    def test_version_numbers_are_unique_per_dataset(self, connection, a_run):
        dataset_id = connection.execute(
            sa.insert(dataset).values(name="panel_c", kind="composed").returning(dataset.c.id)
        ).scalar_one()
        values = {
            "dataset_id": dataset_id,
            "version_num": 1,
            "resolution_mode": "query",
            "spec": {},
            "content_hash": "e" * 64,
            "ingest_watermark": datetime(2026, 1, 1),
            "created_by_run_id": a_run,
        }
        connection.execute(sa.insert(dataset_version).values(values))
        with pytest.raises(sa.exc.IntegrityError):
            connection.execute(sa.insert(dataset_version).values(values))


class TestFeatureVocabulary:
    def test_a_name_is_unique_within_its_group(self, connection, a_source, a_run):
        group_id = connection.execute(
            sa.insert(feature_group)
            .values(_group_values(a_source, a_run))
            .returning(feature_group.c.id)
        ).scalar_one()
        values = {
            "feature_group_id": group_id,
            "name": "gdp_growth",
            "dtype": "num",
            "created_by_run_id": a_run,
        }
        connection.execute(sa.insert(feature).values(values))
        with pytest.raises(sa.exc.IntegrityError):
            connection.execute(sa.insert(feature).values(values))


def test_jsonb_round_trips(connection, a_run):
    """The catalog stores specs and params as JSONB, not as text."""
    stored = connection.execute(sa.select(run.c.params).where(run.c.id == a_run)).scalar_one()
    assert stored == {}
    connection.execute(
        sa.update(run).where(run.c.id == a_run).values(params={"nested": {"a": [1, 2]}})
    )
    reread = connection.execute(sa.select(run.c.params).where(run.c.id == a_run)).scalar_one()
    assert reread["nested"]["a"] == [1, 2]
