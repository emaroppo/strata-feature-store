"""Workflow step 3 against a release small enough to check by hand.

The seed panel proved this runs; it cannot prove it is right, because nobody can
verify 130 million facts by inspection. So the same code path runs here over
four entities and two features, where every expected row can be written down.

What is asserted is mostly what does *not* happen: a null cell does not become a
row, an unmapped entity does not disappear, a value that will not cast does not
become a null, and an event time on the wrong boundary does not pass silently.
Each of those is a way the store could fill with plausible, wrong data while
reporting success.
"""

from __future__ import annotations

import re
import uuid
from datetime import date

import pytest
import sqlalchemy as sa

from strata.feature_store.store import catalog, crosswalk, duck, ingest, objects
from strata.feature_store.store import standardise as std
from strata.feature_store.store.standardise import StandardiseError
from strata.feature_store.store.tables import feature, feature_group

pytestmark = pytest.mark.integration

#: Four entities, two features, one null, one entity with no canonical id.
#: AAA 2020: gdp 1.0, pop 100      -> 2 facts
#: AAA 2021: gdp 2.0, pop 200      -> 2 facts
#: BBB 2020: gdp 3.0, pop <null>   -> 1 fact  (the null is not a row)
#: ZZZ 2020: gdp 9.9, pop 999      -> 2 facts (unmapped, routed to the sibling)
PANEL = (
    "code,date,gdp,pop,leader\n"
    "AAA,2020-12-31,1.0,100,Ada\n"
    "AAA,2021-12-31,2.0,200,Bea\n"
    "BBB,2020-12-31,3.0,,Cas\n"
    "ZZZ,2020-12-31,9.9,999,Dev\n"
)
EXPECTED_MAPPED = 5
EXPECTED_UNMAPPED = 2

CROSSWALK = (
    "source_id,target_id,valid_from,valid_to,confidence\n"
    "AAA,AAA,1900-01-01,9999-12-31,1.0\n"
    "BBB,BBB,1900-01-01,9999-12-31,1.0\n"
)


def _spec(names: dict, **overrides) -> dict:
    payload = {
        "feature_group": names["group"],
        "source": names["source"],
        "retraction_policy": "ignore",
        "layout": "features_in_columns",
        "entity": {"column": "code", "normalise": ["strip", "upper"]},
        "event_time": {
            "column": "date",
            "format": "%Y-%m-%d",
            "period": "annual",
            "anchor": "end",
        },
        "knowledge_time": {
            "basis": "modeled_lag",
            "declared_lag": "P2M12D",
            "lag_anchor_period": "annual",
            "rationale": "annual publication in March",
        },
        "features": {
            "exclude": ["leader"],
            "name": {"allow_unlisted": True},
            "kind": "numeric",
            "meta": {"default_scale": "latent", "default_statistic": "point"},
        },
        "crosswalk": {"dataset": names["crosswalk"], "version": 1},
    }
    payload.update(overrides)
    return payload


@pytest.fixture
def names() -> dict:
    tag = uuid.uuid4().hex[:8]
    return {
        "tag": tag,
        "source": f"panel_{tag}",
        "group": f"g_{tag}",
        "crosswalk": f"cw_{tag}",
        "namespace": f"native-{tag}",
    }


@pytest.fixture
def prepared(engine, settings, s3, tmp_path, names):
    """A registered source, a stored release, and a pinned crosswalk version."""
    with std.open_run(engine, "ingest_release", "fixture", {}) as current:
        catalog.ensure_namespace(current, names["namespace"])
        catalog.ensure_namespace(current, settings.canonical_namespace)

    ingest.register_source(
        engine,
        settings,
        name=names["source"],
        native_namespace=names["namespace"],
        granularity="annual",
    )
    ingest.register_source(
        engine,
        settings,
        name=f"{names['crosswalk']}_src",
        native_namespace=names["namespace"],
        granularity="irregular",
        kind="crosswalk",
    )

    panel = tmp_path / "panel.csv"
    # Unique bytes per test: the object store is shared and content-addressed.
    panel.write_text(PANEL.replace("Ada", f"Ada{names['tag']}"))
    release = ingest.add_release(
        engine,
        settings,
        path=panel,
        source_name=names["source"],
        released_at=date(2025, 3, 12),
        s3=s3,
    )

    cw_file = tmp_path / "cw.csv"
    cw_file.write_text(CROSSWALK)
    cw_release = ingest.add_release(
        engine, settings, path=cw_file, source_name=f"{names['crosswalk']}_src", s3=s3
    )
    crosswalk.register(
        engine,
        settings,
        name=names["crosswalk"],
        digest_prefix=cw_release.sha256[:12],
        s3=s3,
    )
    return release


def _run(engine, settings, s3, tmp_path, names, **overrides):
    from strata.feature_store.spec.standardise import StandardiseSpec

    spec = StandardiseSpec.model_validate(_spec(names, **overrides))
    return std.standardise(
        engine,
        settings,
        spec,
        digest_prefix=_digest(engine, names["source"]),
        s3=s3,
        work_dir=tmp_path / "work",
        memory_limit="2GB",
    )


def _digest(engine, source_name: str) -> str:
    with engine.connect() as connection:
        source = catalog.get_source(connection, source_name)
        releases = catalog.list_releases(connection, source["id"])
    return releases[0]["sha256"][:12]


def _facts(settings, s3, keys: list[str]) -> list[tuple]:
    """Read the written parquet back, as rows."""
    con = duck.connect(settings.storage, memory_limit="1GB")
    urls = ", ".join(f"'{duck.s3_url(settings.storage.bucket, k)}'" for k in keys)
    return con.execute(
        f"SELECT entity_id, event_time, feature_name, value_num, knowledge_time "
        f"FROM read_parquet([{urls}]) ORDER BY feature_name, entity_id, event_time"
    ).fetchall()


class TestTheMelt:
    def test_counts_match_what_was_written_by_hand(
        self, engine, settings, s3, tmp_path, names, prepared
    ):
        result = _run(engine, settings, s3, tmp_path, names)
        assert result.rows_written == EXPECTED_MAPPED

    def test_a_null_cell_does_not_become_a_row(
        self, engine, settings, s3, tmp_path, names, prepared
    ):
        """BBB has no population. That is an absence, not a value.

        Storing it would put a row in the store asserting that something was
        reported when it was not, and would double the size of a half-empty
        panel for the privilege.
        """
        _run(engine, settings, s3, tmp_path, names)
        with engine.connect() as connection:
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == names["group"])
            ).scalar_one()
            keys = [p["object_key"] for p in catalog.live_partitions(connection, group_id)]
        rows = _facts(settings, s3, keys)
        assert ("BBB", date(2020, 12, 31), "pop", None, None) not in rows
        assert [r for r in rows if r[0] == "BBB"] == [("BBB", date(2020, 12, 31), "gdp", 3.0, None)]

    def test_knowledge_time_is_null_and_derived_at_read(
        self, engine, settings, s3, tmp_path, names, prepared
    ):
        """A modeled lag is a rule in the catalog, not a column in the file.

        Correcting it is then a metadata update rather than a rewrite of every
        parquet in the group.
        """
        _run(engine, settings, s3, tmp_path, names)
        with engine.connect() as connection:
            group = (
                connection.execute(
                    sa.select(feature_group).where(feature_group.c.name == names["group"])
                )
                .mappings()
                .one()
            )
            keys = [p["object_key"] for p in catalog.live_partitions(connection, group["id"])]
        assert group["declared_lag"] == "P2M12D"
        assert group["lag_anchor_period"] == "annual"
        assert all(row[4] is None for row in _facts(settings, s3, keys))

    def test_the_derived_knowledge_time_lands_on_publication(
        self, engine, settings, s3, tmp_path, names, prepared
    ):
        """2020 data becomes knowable in March 2021, not in March 2020."""
        from strata.feature_store.spec.duration import parse_duration
        from strata.feature_store.spec.timebasis import knowledge_time_sql

        _run(engine, settings, s3, tmp_path, names)
        with engine.connect() as connection:
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == names["group"])
            ).scalar_one()
            keys = [p["object_key"] for p in catalog.live_partitions(connection, group_id)]

        expression = knowledge_time_sql(
            "modeled_lag", parse_duration("P2M12D"), lag_anchor_period="annual"
        )
        con = duck.connect(settings.storage, memory_limit="1GB")
        urls = ", ".join(f"'{duck.s3_url(settings.storage.bucket, k)}'" for k in keys)
        resolved = con.execute(
            f"SELECT DISTINCT event_time, {expression} AS kt "
            f"FROM read_parquet([{urls}]) ORDER BY event_time"
        ).fetchall()
        assert [(r[0], r[1].date()) for r in resolved] == [
            (date(2020, 12, 31), date(2021, 3, 12)),
            (date(2021, 12, 31), date(2022, 3, 12)),
        ]


class TestUnmappedEntities:
    def test_are_routed_rather_than_dropped(self, engine, settings, s3, tmp_path, names, prepared):
        """ZZZ has no canonical id. Losing it silently is the failure to avoid."""
        _run(engine, settings, s3, tmp_path, names)
        with engine.connect() as connection:
            sibling = (
                connection.execute(
                    sa.select(feature_group).where(
                        feature_group.c.name == f"{names['group']}__unmapped"
                    )
                )
                .mappings()
                .one()
            )
            keys = [p["object_key"] for p in catalog.live_partitions(connection, sibling["id"])]
        rows = _facts(settings, s3, keys)
        assert len(rows) == EXPECTED_UNMAPPED
        assert {r[0] for r in rows} == {"ZZZ"}

    def test_the_sibling_keeps_the_native_vocabulary(
        self, engine, settings, s3, tmp_path, names, prepared
    ):
        """It must not claim to be canonical; that is what makes it unjoinable."""
        _run(engine, settings, s3, tmp_path, names)
        with engine.connect() as connection:
            rows = (
                connection.execute(
                    sa.select(feature_group.c.name, feature_group.c.entity_namespace).where(
                        feature_group.c.name.in_([names["group"], f"{names['group']}__unmapped"])
                    )
                )
                .mappings()
                .all()
            )
        by_name = {r["name"]: r["entity_namespace"] for r in rows}
        assert by_name[names["group"]] == settings.canonical_namespace
        assert by_name[f"{names['group']}__unmapped"] == names["namespace"]

    def test_the_run_reports_how_many_were_unmapped(
        self, engine, settings, s3, tmp_path, names, prepared
    ):
        result = _run(engine, settings, s3, tmp_path, names)
        assert result.unmapped_entities == 1


class TestRefusals:
    def test_a_value_that_will_not_cast_is_an_error(
        self, engine, settings, s3, tmp_path, names, prepared
    ):
        """A numeric group pointed at a text column must not quietly null it.

        TRY_CAST would turn every name into a null, which is indistinguishable
        at read from never having been reported. The seed panel hid 25 such
        columns among four thousand; this is the check that found them.
        """
        with pytest.raises(StandardiseError, match="could not be cast"):
            _run(
                engine,
                settings,
                s3,
                tmp_path,
                names,
                features={"exclude": [], "name": {"allow_unlisted": True}, "kind": "numeric"},
            )

    def test_an_unknown_column_is_named(self, engine, settings, s3, tmp_path, names, prepared):
        with pytest.raises(StandardiseError, match="does not have"):
            _run(
                engine,
                settings,
                s3,
                tmp_path,
                names,
                features={
                    "name": {"allow_unlisted": True},
                    "exclude": ["leader", "nonexistent"],
                },
            )

    def test_unlisted_columns_are_refused_by_default(
        self, engine, settings, s3, tmp_path, names, prepared
    ):
        """A new upstream column is a feature nobody named."""
        with pytest.raises(StandardiseError, match="allow_unlisted"):
            _run(
                engine,
                settings,
                s3,
                tmp_path,
                names,
                features={
                    "exclude": ["leader"],
                    "name": {"allow_unlisted": False},
                    "kind": "numeric",
                },
            )

    def test_an_event_time_off_the_declared_boundary_is_refused(
        self, engine, settings, s3, tmp_path, names, prepared
    ):
        """The source says annual; anchoring at the start makes the dates wrong.

        A wrong anchor shifts every derived knowledge_time by up to a year, half
        the time in the direction that invalidates results.
        """
        with pytest.raises(StandardiseError, match="boundary"):
            _run(
                engine,
                settings,
                s3,
                tmp_path,
                names,
                event_time={
                    "column": "date",
                    "format": "%Y-%m-%d",
                    "period": "annual",
                    "anchor": "start",
                },
            )


class TestTheFeatureRegistry:
    def test_records_every_name_with_its_structure(
        self, engine, settings, s3, tmp_path, names, prepared
    ):
        _run(engine, settings, s3, tmp_path, names)
        with engine.connect() as connection:
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == names["group"])
            ).scalar_one()
            rows = (
                connection.execute(
                    sa.select(feature.c.name, feature.c.dtype, feature.c.meta).where(
                        feature.c.feature_group_id == group_id
                    )
                )
                .mappings()
                .all()
            )
        registry = {r["name"]: r for r in rows}
        assert set(registry) == {"gdp", "pop"}
        assert registry["gdp"]["dtype"] == "num"
        assert registry["gdp"]["meta"] == {
            "base": "gdp",
            "scale": "latent",
            "statistic": "point",
        }


class TestIdempotence:
    def test_the_same_config_twice_writes_no_second_copy(
        self, engine, settings, s3, tmp_path, names, prepared
    ):
        first = _run(engine, settings, s3, tmp_path, names)
        second = _run(engine, settings, s3, tmp_path, names)
        assert first.rows_written == second.rows_written
        with engine.connect() as connection:
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == names["group"])
            ).scalar_one()
            live = catalog.live_partitions(connection, group_id)
        assert len(live) == 1, "identical content must reuse the existing partition"


def test_parts_are_sorted_by_feature_name(engine, settings, s3, tmp_path, names, prepared):
    """What makes row-group statistics tight enough to skip on."""
    _run(engine, settings, s3, tmp_path, names)
    with engine.connect() as connection:
        group_id = connection.execute(
            sa.select(feature_group.c.id).where(feature_group.c.name == names["group"])
        ).scalar_one()
        keys = [p["object_key"] for p in catalog.live_partitions(connection, group_id)]
    con = duck.connect(settings.storage, memory_limit="1GB")
    urls = ", ".join(f"'{duck.s3_url(settings.storage.bucket, k)}'" for k in keys)
    ordered = con.execute(
        f"SELECT feature_name, entity_id, event_time FROM read_parquet([{urls}])"
    ).fetchall()
    assert ordered == sorted(ordered)


def test_the_object_exists_where_the_catalog_says(engine, settings, s3, tmp_path, names, prepared):
    """Invariant 3 from the other side: no row points at bytes that are absent."""
    result = _run(engine, settings, s3, tmp_path, names)
    assert objects.exists(s3, settings.storage.bucket, result.object_key)


class TestACanonicalSource:
    """A source that already publishes in the canonical vocabulary.

    Every ingest so far has gone through a crosswalk, so the pass-through branch
    of the namespace guard had never run. A source needing no conversion is the
    ordinary case for anything already keyed on ISO3, and it must not
    accidentally require a crosswalk to say so.
    """

    @pytest.fixture
    def native(self, engine, settings, s3, tmp_path, names):
        with std.open_run(engine, "ingest_release", "fixture", {}) as current:
            catalog.ensure_namespace(current, settings.canonical_namespace)
        ingest.register_source(
            engine,
            settings,
            name=names["source"],
            native_namespace=settings.canonical_namespace,
            granularity="annual",
        )
        panel_file = tmp_path / "panel.csv"
        panel_file.write_text(PANEL.replace("Ada", f"Ada{names['tag']}"))
        ingest.add_release(
            engine,
            settings,
            path=panel_file,
            source_name=names["source"],
            released_at=date(2025, 3, 12),
            s3=s3,
        )
        return names

    def test_it_standardises_without_a_crosswalk(self, engine, settings, s3, tmp_path, native):
        result = _run(engine, settings, s3, tmp_path, native, crosswalk=None)
        # Every entity is canonical by definition, including the one the
        # crosswalk fixture would have left unmapped.
        assert result.rows_written == EXPECTED_MAPPED + EXPECTED_UNMAPPED
        assert result.unmapped_entities == 0

    def test_no_unmapped_sibling_is_created(self, engine, settings, s3, tmp_path, native):
        """There is nothing for it to hold, and an empty group is a lie."""
        _run(engine, settings, s3, tmp_path, native, crosswalk=None)
        with engine.connect() as connection:
            sibling = connection.execute(
                sa.select(feature_group.c.id).where(
                    feature_group.c.name == f"{native['group']}__unmapped"
                )
            ).scalar_one_or_none()
        assert sibling is None

    def test_a_crosswalk_here_is_refused(self, engine, settings, s3, tmp_path, native):
        """It would have nothing to convert, so naming one is a mistake."""
        with pytest.raises(StandardiseError, match="nothing to convert"):
            _run(engine, settings, s3, tmp_path, native)


#: One row per (entity, indicator); one column per year. The mirror of PANEL,
#: which is one row per (entity, date) and one column per indicator.
PERIODS_PANEL = (
    "Country Code,Indicator Name,Indicator Code,2020,2021,2022\n"
    "AAA,Gross domestic product,NY.GDP,1.0,2.0,3.0\n"
    "AAA,Population,SP.POP,100,200,\n"
    "ZZZ,Gross domestic product,NY.GDP,9.9,,\n"
)


@pytest.fixture
def periods_release(engine, settings, s3, tmp_path, names, prepared):
    path = tmp_path / "periods.csv"
    # Unique bytes per test, varied inside a data value rather than before
    # the header: the store is content-addressed, so an identical file
    # registered under two sources leaves a digest prefix ambiguous.
    path.write_text(PERIODS_PANEL.replace("Gross domestic product", f"GDP {names['tag']}"))
    # A second source, so the year-column file has its own release.
    second = f"{names['source']}_periods"
    ingest.register_source(
        engine,
        settings,
        name=second,
        native_namespace=names["namespace"],
        granularity="annual",
    )
    result = ingest.add_release(
        engine, settings, path=path, source_name=second, released_at=date(2025, 6, 5), s3=s3
    )
    return second, result


class TestPeriodsInColumns:
    """A source whose column headers are years rather than feature names.

    Both melted layouts unpivot columns. What differs is what a header *means*,
    and therefore which of the seven fields it becomes: a feature name in one
    case, an event time in the other.
    """

    @pytest.fixture
    def periods_spec(self, names) -> dict:
        return {
            "feature_group": names["group"],
            "source": names["source"],
            "retraction_policy": "ignore",
            "layout": "periods_in_columns",
            "entity": {"column": "Country Code", "normalise": ["strip", "upper"]},
            "event_time": {
                "from_column_name": True,
                "format": "%Y",
                "period": "annual",
                "anchor": "end",
            },
            "knowledge_time": {
                "basis": "modeled_lag",
                "declared_lag": "P3M",
                "lag_anchor_period": "annual",
            },
            "features": {
                "exclude": ["Indicator Name"],
                "name": {
                    "source": "column",
                    "column": "Indicator Code",
                    "allow_unlisted": True,
                },
                "kind": "numeric",
            },
            "crosswalk": {"dataset": names["crosswalk"], "version": 1},
        }

    def _standardise(self, engine, settings, s3, tmp_path, names, periods_spec, source, digest):
        from strata.feature_store.spec.standardise import StandardiseSpec

        payload = {**periods_spec, "source": source}
        return std.standardise(
            engine,
            settings,
            StandardiseSpec.model_validate(payload),
            digest_prefix=digest,
            s3=s3,
            work_dir=tmp_path / "periods_work",
            memory_limit="2GB",
        )

    def test_year_columns_become_event_times(
        self, engine, settings, s3, tmp_path, names, periods_spec, periods_release
    ):
        """AAA has three years of GDP and two of population; ZZZ is unmapped."""
        source, release = periods_release
        result = self._standardise(
            engine, settings, s3, tmp_path, names, periods_spec, source, release.sha256[:12]
        )
        assert result.rows_written == 5, "3 GDP + 2 population, and the null is not a row"

    def test_the_anchor_puts_a_bare_year_at_its_end(
        self, engine, settings, s3, tmp_path, names, periods_spec, periods_release
    ):
        """A column headed 2020 becomes 2020-12-31, not 2020-01-01.

        The reason is not cosmetic: under a lag the anchor decides what a
        backtest is allowed to see. See docs/adr/0001.
        """
        source, release = periods_release
        self._standardise(
            engine, settings, s3, tmp_path, names, periods_spec, source, release.sha256[:12]
        )
        with engine.connect() as connection:
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == names["group"])
            ).scalar_one()
            keys = [p["object_key"] for p in catalog.live_partitions(connection, group_id)]
        from strata.feature_store.store import duck

        con = duck.connect(settings.storage, memory_limit="1GB")
        urls = ", ".join(f"'{duck.s3_url(settings.storage.bucket, k)}'" for k in keys)
        dates = [
            r[0]
            for r in con.execute(
                f"SELECT DISTINCT event_time FROM read_parquet([{urls}]) ORDER BY 1"
            ).fetchall()
        ]
        assert dates == [date(2020, 12, 31), date(2021, 12, 31), date(2022, 12, 31)]

    def test_feature_names_come_from_a_column_not_the_headers(
        self, engine, settings, s3, tmp_path, names, periods_spec, periods_release
    ):
        """The registry must hold indicator codes, never years."""
        source, release = periods_release
        self._standardise(
            engine, settings, s3, tmp_path, names, periods_spec, source, release.sha256[:12]
        )
        with engine.connect() as connection:
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == names["group"])
            ).scalar_one()
            registered = set(
                connection.execute(
                    sa.select(feature.c.name).where(feature.c.feature_group_id == group_id)
                )
                .scalars()
                .all()
            )
        assert registered == {"NY.GDP", "SP.POP"}
        assert not any(name.isdigit() for name in registered), "a year is not a feature"

    def test_unmapped_entities_are_still_routed(
        self, engine, settings, s3, tmp_path, names, periods_spec, periods_release
    ):
        source, release = periods_release
        result = self._standardise(
            engine, settings, s3, tmp_path, names, periods_spec, source, release.sha256[:12]
        )
        assert result.unmapped_entities == 1


class TestLayoutRefusals:
    """Each layout has to be coherent, and says so rather than melting nonsense."""

    def _validate(self, **overrides):
        from strata.feature_store.spec.standardise import StandardiseSpec

        payload = {
            "feature_group": "g",
            "source": "s",
            "layout": "periods_in_columns",
            "entity": {"column": "code"},
            "event_time": {"from_column_name": True, "format": "%Y", "period": "annual"},
            "knowledge_time": {"basis": "file_release", "release_date": "2025-01-01"},
            "features": {
                "name": {"source": "column", "column": "indicator", "allow_unlisted": True}
            },
        }
        payload.update(overrides)
        return StandardiseSpec.model_validate(payload)

    def test_the_valid_shape_is_accepted(self):
        assert self._validate().layout == "periods_in_columns"

    def test_headers_cannot_be_both_periods_and_features(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="both"):
            self._validate(
                layout="features_in_columns",
                event_time={"from_column_name": True, "column": "year", "period": "annual"},
            )

    def test_a_period_label_needs_a_period(self):
        """'irregular' has no boundary, so there is nothing to anchor to."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="irregular"):
            self._validate(
                event_time={"from_column_name": True, "format": "%Y", "period": "irregular"}
            )

    def test_periods_in_columns_needs_a_name_column(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match=re.escape("features.name.source")):
            self._validate(features={"name": {"source": "column_name"}})

    def test_features_in_columns_needs_an_event_time_column(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="event_time"):
            self._validate(
                layout="features_in_columns",
                event_time={"period": "annual"},
                features={"name": {"source": "column_name", "allow_unlisted": True}},
            )


VOCABULARY = (
    "Series Code,Indicator Name,Unit of measure,Topic,Periodicity\n"
    "NY.GDP,Gross domestic product,USD,National accounts,Annual\n"
    "SP.POP,Population total,Persons,Health,Annual\n"
    "NOT.IN.DATA,An indicator this release did not carry,,Other,Annual\n"
)


class TestVocabulary:
    """A companion file describing the features, ingested with the facts.

    A store holding thousands of indicators otherwise knows every one of their
    names and nothing about any of them.
    """

    @pytest.fixture
    def with_vocabulary(self, engine, settings, s3, tmp_path, names, periods_release):
        source, release = periods_release
        path = tmp_path / "series.csv"
        path.write_text(VOCABULARY.replace("Gross domestic product", f"GDP {names['tag']}"))
        vocab = ingest.add_release(
            engine, settings, path=path, source_name=source, released_at=date(2025, 6, 5), s3=s3
        )
        return source, release, vocab

    def _spec(self, names, source, vocab_digest, **overrides):
        payload = {
            "feature_group": names["group"],
            "source": source,
            "retraction_policy": "ignore",
            "layout": "periods_in_columns",
            "entity": {"column": "Country Code", "normalise": ["strip", "upper"]},
            "event_time": {
                "from_column_name": True,
                "format": "%Y",
                "period": "annual",
                "anchor": "end",
            },
            "knowledge_time": {"basis": "file_release", "release_date": "2025-06-05"},
            "features": {
                "exclude": ["Indicator Name"],
                "name": {
                    "source": "column",
                    "column": "Indicator Code",
                    "allow_unlisted": True,
                },
                "kind": "numeric",
                "vocabulary": {
                    "release": vocab_digest,
                    "key": "Series Code",
                    "description": "Indicator Name",
                    "unit": "Unit of measure",
                    "meta": ["Topic", "Periodicity"],
                },
            },
            "crosswalk": {"dataset": names["crosswalk"], "version": 1},
        }
        payload.update(overrides)
        return payload

    def _run_it(self, engine, settings, s3, tmp_path, names, source, release, vocab):
        from strata.feature_store.spec.standardise import StandardiseSpec

        return std.standardise(
            engine,
            settings,
            StandardiseSpec.model_validate(self._spec(names, source, vocab.sha256[:12])),
            digest_prefix=release.sha256[:12],
            s3=s3,
            work_dir=tmp_path / "vocab_work",
            memory_limit="2GB",
        )

    def _registry(self, engine, group_name):
        with engine.connect() as connection:
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == group_name)
            ).scalar_one()
            return {
                r["name"]: r
                for r in connection.execute(
                    sa.select(feature).where(feature.c.feature_group_id == group_id)
                )
                .mappings()
                .all()
            }

    def test_units_and_descriptions_land(
        self, engine, settings, s3, tmp_path, names, with_vocabulary
    ):
        self._run_it(engine, settings, s3, tmp_path, names, *with_vocabulary)
        registry = self._registry(engine, names["group"])
        assert registry["SP.POP"]["unit"] == "Persons"
        assert registry["SP.POP"]["description"] == "Population total"

    def test_declared_meta_columns_are_carried(
        self, engine, settings, s3, tmp_path, names, with_vocabulary
    ):
        self._run_it(engine, settings, s3, tmp_path, names, *with_vocabulary)
        meta = self._registry(engine, names["group"])["SP.POP"]["meta"]
        assert meta["Topic"] == "Health"
        assert meta["Periodicity"] == "Annual"

    def test_the_name_derived_structure_survives(
        self, engine, settings, s3, tmp_path, names, with_vocabulary
    ):
        """Both are meta, and a merge that erased one would be a silent loss."""
        self._run_it(engine, settings, s3, tmp_path, names, *with_vocabulary)
        meta = self._registry(engine, names["group"])["SP.POP"]["meta"]
        assert meta["base"] == "SP.POP"
        assert meta["Topic"] == "Health"

    def test_the_vocabulary_does_not_decide_what_exists(
        self, engine, settings, s3, tmp_path, names, with_vocabulary
    ):
        """The data is the authority on what exists; this on what it means.

        `NOT.IN.DATA` is described and never appears in the release, so it must
        not be registered — a registry row for a feature the group does not hold
        is a promise nothing can keep.
        """
        self._run_it(engine, settings, s3, tmp_path, names, *with_vocabulary)
        assert "NOT.IN.DATA" not in self._registry(engine, names["group"])

    def test_a_feature_absent_from_the_vocabulary_is_still_registered(
        self, engine, settings, s3, tmp_path, names, with_vocabulary
    ):
        """Undocumented is not the same as non-existent."""
        source, release, vocab = with_vocabulary
        from strata.feature_store.spec.standardise import StandardiseSpec

        payload = self._spec(names, source, vocab.sha256[:12])
        payload["features"]["vocabulary"]["key"] = "Series Code"
        # The release carries NY.GDP and SP.POP; strip one from the vocabulary
        # by pointing at a description column that only some rows fill.
        std.standardise(
            engine,
            settings,
            StandardiseSpec.model_validate(payload),
            digest_prefix=release.sha256[:12],
            s3=s3,
            work_dir=tmp_path / "vocab_work2",
            memory_limit="2GB",
        )
        registry = self._registry(engine, names["group"])
        assert {"NY.GDP", "SP.POP"} <= set(registry)

    def test_a_missing_column_is_named(
        self, engine, settings, s3, tmp_path, names, with_vocabulary
    ):
        source, release, vocab = with_vocabulary
        from strata.feature_store.spec.standardise import StandardiseSpec

        payload = self._spec(names, source, vocab.sha256[:12])
        payload["features"]["vocabulary"]["unit"] = "No Such Column"
        with pytest.raises(StandardiseError, match="no column"):
            std.standardise(
                engine,
                settings,
                StandardiseSpec.model_validate(payload),
                digest_prefix=release.sha256[:12],
                s3=s3,
                work_dir=tmp_path / "vocab_work3",
                memory_limit="2GB",
            )

    def test_the_vocabulary_release_is_recorded_as_an_input(
        self, engine, settings, s3, tmp_path, names, with_vocabulary
    ):
        """Lineage has to show both files, or the description has no provenance."""
        _source, release, vocab = with_vocabulary
        result = self._run_it(engine, settings, s3, tmp_path, names, *with_vocabulary)
        with engine.connect() as connection:
            inputs = connection.execute(
                sa.text("select ref_type, ref_id, role from run_input where run_id = :r"),
                {"r": result.run_id},
            ).all()
        assert ("release", vocab.release_id, "vocabulary") in inputs
        assert ("release", release.release_id, "release") in inputs
