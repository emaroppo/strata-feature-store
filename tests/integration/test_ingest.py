"""Workflow steps 1 and 2, against a live Postgres and a live object store.

The properties under test are the ones that make a mapping mistake survivable:
the same bytes are one object however they were named, different bytes never
overwrite each other, and a catalog row is only written after the bytes are
known to have landed.
"""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
import sqlalchemy as sa

from feature_store.store import catalog, ingest, objects
from feature_store.store.config import Settings, StorageSettings
from feature_store.store.runs import run as open_run
from feature_store.store.tables import release as release_table
from feature_store.store.tables import run as run_table

pytestmark = pytest.mark.integration

NAMESPACE = "test-vocab"


@pytest.fixture(scope="session")
def storage_settings() -> StorageSettings:
    """Points at the throwaway container, never at a shared instance.

    These tests write objects and would happily delete them. Aiming that at a
    store holding real sources is one environment variable away, so the default
    is local and the override is explicit.
    """
    settings = StorageSettings(
        endpoint_url=os.environ.get("FEATURE_STORE_TEST_S3_ENDPOINT", "http://localhost:3900"),
        bucket=os.environ.get("FEATURE_STORE_TEST_BUCKET", "feature-store"),
        region="garage",
        addressing_style="path",
    )
    if not os.environ.get("FEATURE_STORE_S3_ACCESS_KEY"):
        pytest.skip("FEATURE_STORE_S3_ACCESS_KEY is not set; no object store to test against")
    try:
        client = objects.client(settings)
        client.head_bucket(Bucket=settings.bucket)
    except Exception as exc:  # pragma: no cover - depends on the host
        pytest.skip(f"no object store at {settings.endpoint_url}: {exc}")
    return settings


@pytest.fixture
def s3(storage_settings: StorageSettings):
    return objects.client(storage_settings)


@pytest.fixture
def settings(migrated_database_url: str, storage_settings: StorageSettings) -> Settings:
    from feature_store.store.config import CatalogSettings

    return Settings(
        catalog=CatalogSettings(url=migrated_database_url),
        storage=storage_settings,
        canonical_namespace="iso3166-1-alpha3",
    )


@pytest.fixture
def prepared(engine: sa.Engine, settings: Settings):
    """A registered source, on its own vocabulary, isolated per test."""
    name = f"src_{uuid.uuid4().hex[:8]}"
    namespace = f"{NAMESPACE}-{uuid.uuid4().hex[:8]}"
    with engine.begin() as connection:
        with open_run(connection, "ingest_release", "fixture", {}) as current:
            catalog.ensure_namespace(current, namespace)
            catalog.add_source(
                current, name=name, native_namespace=namespace, granularity="annual"
            )
    return name


@pytest.fixture
def a_file(tmp_path):
    """Files whose bytes are unique to this test.

    The object store is shared and content-addressed, so two tests writing the
    same content would land on one object and see each other's upload flags.
    A per-test marker keeps them independent without weakening what is asserted.
    """
    marker = uuid.uuid4().hex

    def make(content: str, filename: str = "indicators.csv"):
        path = tmp_path / filename
        path.write_text(f"# {marker}\n{content}")
        return path

    return make


class TestContentAddressing:
    def test_the_same_bytes_upload_once(self, engine, settings, prepared, a_file, s3):
        path = a_file("code,year,gdp\nAAA,2024,2.1\n")
        first = ingest.add_release(engine, settings, path=path, source_name=prepared, s3=s3)
        second = ingest.add_release(engine, settings, path=path, source_name=prepared, s3=s3)

        assert first.sha256 == second.sha256
        assert first.release_id == second.release_id
        assert first.uploaded is True
        assert second.uploaded is False, "identical bytes should not be re-uploaded"
        assert first.run_id != second.run_id, "the attempt is still recorded"

    def test_a_revised_file_of_the_same_name_does_not_overwrite(
        self, engine, settings, prepared, a_file, s3
    ):
        """The failure this whole scheme exists to prevent.

        Publishers reuse filenames across vintages. Under a name-derived key the
        September download would replace April's, and every standardisation that
        read April would become unreproducible with nothing recording it.
        """
        april = a_file("code,year,gdp\nAAA,2024,2.1\n", "indicators.csv")
        first = ingest.add_release(
            engine, settings, path=april, source_name=prepared, released_at=date(2024, 4, 15), s3=s3
        )
        september = a_file("code,year,gdp\nAAA,2024,2.3\n", "indicators.csv")
        second = ingest.add_release(
            engine, settings, path=september, source_name=prepared,
            released_at=date(2024, 9, 30), s3=s3,
        )

        assert first.sha256 != second.sha256
        assert first.object_key != second.object_key
        assert first.release_id != second.release_id
        assert objects.exists(s3, settings.storage.bucket, first.object_key)
        assert objects.exists(s3, settings.storage.bucket, second.object_key)

    def test_both_vintages_are_retrievable_afterwards(
        self, engine, settings, prepared, a_file, s3, tmp_path
    ):
        april = a_file("code,year,gdp\nAAA,2024,2.1\n", "indicators.csv")
        first = ingest.add_release(engine, settings, path=april, source_name=prepared, s3=s3)
        september = a_file("code,year,gdp\nAAA,2024,2.3\n", "indicators.csv")
        ingest.add_release(engine, settings, path=september, source_name=prepared, s3=s3)

        out = ingest.fetch_release(
            engine, settings, digest_prefix=first.sha256[:10],
            destination=tmp_path / "back.csv", s3=s3,
        )
        assert out.read_text().endswith("2.1\n"), "April's bytes, not September's"


class TestTheReleaseRow:
    def test_records_vintage_and_retrieval_separately(
        self, engine, settings, prepared, a_file, s3
    ):
        """Conflating them makes every as-of query before today return nothing."""
        path = a_file("code,year,gdp\nAAA,2024,2.1\n")
        result = ingest.add_release(
            engine, settings, path=path, source_name=prepared,
            released_at=date(2024, 4, 15), s3=s3,
        )
        with engine.begin() as connection:
            row = (
                connection.execute(
                    sa.select(release_table).where(release_table.c.id == result.release_id)
                )
                .mappings()
                .one()
            )
        assert row["released_at"] == date(2024, 4, 15)
        assert row["retrieved_at"].year >= 2026
        assert row["released_at"] != row["retrieved_at"].date()

    def test_the_run_knows_up_to_the_release_date(self, engine, settings, prepared, a_file, s3):
        """What invariant 1 compares a later standardise run against."""
        path = a_file("code,year,gdp\nAAA,2024,2.1\n")
        result = ingest.add_release(
            engine, settings, path=path, source_name=prepared,
            released_at=date(2024, 4, 15), s3=s3,
        )
        with engine.begin() as connection:
            known = connection.execute(
                sa.select(run_table.c.max_knowledge_time).where(run_table.c.id == result.run_id)
            ).scalar_one()
        assert known.date() == date(2024, 4, 15)

    def test_lineage_records_the_release_as_an_output(
        self, engine, settings, prepared, a_file, s3
    ):
        path = a_file("code,year,gdp\nAAA,2024,2.1\n")
        result = ingest.add_release(engine, settings, path=path, source_name=prepared, s3=s3)
        with engine.begin() as connection:
            rows = connection.execute(
                sa.text(
                    "select ref_type, ref_id from run_output where run_id = :r"
                ),
                {"r": result.run_id},
            ).all()
        assert ("release", result.release_id) in rows

    def test_a_manifest_is_written_and_readable(self, engine, settings, prepared, a_file, s3):
        from feature_store.store.runs import read_manifest

        path = a_file("code,year,gdp\nAAA,2024,2.1\n")
        result = ingest.add_release(
            engine, settings, path=path, source_name=prepared,
            released_at=date(2024, 4, 15), s3=s3,
        )
        with engine.begin() as connection:
            key = connection.execute(
                sa.select(run_table.c.manifest_key).where(run_table.c.id == result.run_id)
            ).scalar_one()
        payload = read_manifest(s3, settings.storage.bucket, key)
        assert payload["notes"]["sha256"] == result.sha256
        assert payload["params"]["released_at"] == "2024-04-15", "dates survive as ISO strings"
        assert payload["outputs"][0]["ref_type"] == "release"


class TestRefusals:
    def test_a_release_needs_a_registered_source(self, engine, settings, a_file, s3):
        path = a_file("code,year,gdp\nAAA,2024,2.1\n")
        with pytest.raises(catalog.CatalogError, match="no source named"):
            ingest.add_release(engine, settings, path=path, source_name="nonexistent", s3=s3)

    def test_a_source_cannot_be_registered_twice(self, engine, settings, prepared):
        with pytest.raises(catalog.CatalogError, match="already exists"):
            ingest.register_source(
                engine, settings, name=prepared,
                native_namespace="whatever", granularity="annual",
            )

    def test_a_source_needs_a_registered_vocabulary(self, engine, settings):
        with pytest.raises(catalog.CatalogError, match="not registered"):
            ingest.register_source(
                engine, settings, name=f"s_{uuid.uuid4().hex[:6]}",
                native_namespace="never-registered", granularity="annual",
            )

    def test_fetching_an_unknown_digest_says_so(self, engine, settings, tmp_path):
        with pytest.raises(catalog.CatalogError, match="no release"):
            ingest.fetch_release(
                engine, settings, digest_prefix="deadbeef", destination=tmp_path / "x"
            )
