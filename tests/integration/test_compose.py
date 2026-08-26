"""Workflow step 4, over a panel whose every answer can be worked out by hand.

The point-in-time property is the reason this store exists, so the tests that
matter here are the ones that would catch a leak: a value visible before it was
knowable, a revision visible before it was published, a feature matched against
another feature's timeline.

The fixture is deliberately awkward — one entity, one feature, three vintages of
the same year — because that is the shape that breaks a naive join.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

import pytest
import sqlalchemy as sa

from feature_store.spec.compose import ComposeSpec
from feature_store.store import catalog, compose, duck
from feature_store.store.compose import ComposeError
from feature_store.store.runs import run as open_run
from feature_store.store.tables import feature_group, feature_partition

pytestmark = pytest.mark.integration

NAMESPACE = "iso3166-1-alpha3"


def _write_facts(engine, settings, s3, rows, *, group_name, namespace=NAMESPACE):
    """Put facts straight into a group, bypassing standardise.

    Composition is what is under test, and going through the melt would make
    every one of these tests depend on a wide CSV being reshaped correctly. The
    facts are written in the same schema standardise produces.
    """
    import tempfile
    from pathlib import Path

    from feature_store.spec.canonical import hash_file

    work = Path(tempfile.mkdtemp())
    parquet = work / "part.parquet"
    con = duck.connect()
    con.execute(
        "CREATE TABLE f (entity_id VARCHAR, event_time DATE, knowledge_time TIMESTAMP, "
        "ingest_time TIMESTAMP, feature_name VARCHAR, value_num DOUBLE, value_str VARCHAR)"
    )
    for row in rows:
        con.execute("INSERT INTO f VALUES (?, ?, ?, ?, ?, ?, ?)", row)
    con.execute(
        f"COPY (SELECT * FROM f ORDER BY feature_name, entity_id, event_time) "
        f"TO '{parquet}' (FORMAT PARQUET)"
    )
    digest, size = hash_file(str(parquet))
    key = f"features/{group_name}/entity_namespace={namespace}/part-{digest[:16]}.parquet"
    s3.upload_file(Filename=str(parquet), Bucket=settings.storage.bucket, Key=key)

    with open_run(engine, "standardise", f"seed {group_name}", {}) as current:
        with current.transaction() as connection:
            source_id = catalog.get_source(connection, f"src_{group_name}")
            if source_id is None:
                catalog.ensure_namespace(current, namespace)
                catalog.add_source(
                    current,
                    name=f"src_{group_name}",
                    native_namespace=namespace,
                    granularity="annual",
                )
                source_id = catalog.get_source(connection, f"src_{group_name}")
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == group_name)
            ).scalar_one_or_none()
            if group_id is None:
                group_id = connection.execute(
                    sa.insert(feature_group)
                    .values(
                        name=group_name,
                        source_id=source_id["id"],
                        entity_namespace=namespace,
                        time_basis="publisher_timestamp",
                        storage_prefix=f"features/{group_name}",
                        created_by_run_id=current.id,
                    )
                    .returning(feature_group.c.id)
                ).scalar_one()
            existing_features = set(
                connection.execute(
                    sa.select(sa.text("name")).select_from(sa.text("feature")).where(
                        sa.text("feature_group_id = :g")
                    ),
                    {"g": group_id},
                ).scalars().all()
            )
            partition_id = connection.execute(
                sa.insert(feature_partition)
                .values(
                    feature_group_id=group_id,
                    object_key=key,
                    content_sha256=digest,
                    size_bytes=size,
                    row_count=len(rows),
                    ingest_time=max(r[3] for r in rows),
                    max_knowledge_time=max(r[2] for r in rows),
                    created_by_run_id=current.id,
                )
                .returning(feature_partition.c.id)
            ).scalar_one()
            # The vocabulary registry, which standardise writes and which
            # composition resolves dtypes from. Registered from the feature
            # names rather than skipped, because a group without it is not a
            # faithful stand-in for a real one.
            from feature_store.store.tables import feature as feature_table

            fresh = sorted({r[4] for r in rows} - existing_features)
            if fresh:
                connection.execute(
                    sa.insert(feature_table),
                    [
                        {
                            "feature_group_id": group_id,
                            "name": feature_name,
                            "dtype": "num",
                            "created_by_run_id": current.id,
                        }
                        for feature_name in fresh
                    ],
                )
        current.wrote("feature_partition", partition_id)
        current.observed_knowledge_time(max(r[2] for r in rows))
    return group_id


@pytest.fixture
def revised(engine, settings, s3):
    """One entity, one feature, one event year, revised twice.

    gdp for 2020, as published three times:
        1.0 knowable 2021-03-01   (first estimate)
        2.0 knowable 2021-09-01   (first revision)
        3.0 knowable 2022-03-01   (second revision)

    A join on event date alone returns 3.0 for every question ever asked.
    """
    name = f"rev_{uuid.uuid4().hex[:8]}"
    ingest = datetime(2026, 1, 1)
    _write_facts(
        engine,
        settings,
        s3,
        [
            ("AAA", date(2020, 12, 31), datetime(2021, 3, 1), ingest, "gdp", 1.0, None),
            ("AAA", date(2020, 12, 31), datetime(2021, 9, 1), ingest, "gdp", 2.0, None),
            ("AAA", date(2020, 12, 31), datetime(2022, 3, 1), ingest, "gdp", 3.0, None),
        ],
        group_name=name,
    )
    return name


def _spec(group: str, **overrides) -> ComposeSpec:
    payload = {
        "name": f"panel_{uuid.uuid4().hex[:8]}",
        "spine": {
            "entities": {"ids": ["AAA"]},
            "times": {"dates": ["2021-06-01", "2021-12-01", "2022-06-01"]},
        },
        "features": [{"group": group, "name": "gdp"}],
        "as_of": {"policy": "spine_time"},
    }
    payload.update(overrides)
    return ComposeSpec.model_validate(payload)


def _resolve(engine, settings, spec):
    with engine.connect() as connection:
        plan = compose.plan(connection, spec, settings)
    return compose.resolve(plan).fetchall()


class TestPointInTime:
    def test_each_spine_row_sees_only_what_was_published_by_then(
        self, engine, settings, s3, revised
    ):
        """The whole store, in one assertion.

        June 2021 knew the first estimate. December 2021 knew the first
        revision. June 2022 knew the second. A naive join returns 3.0 for all
        three and inflates every backtest built on it.
        """
        rows = _resolve(engine, settings, _spec(revised))
        assert [(str(r[1]), r[4]) for r in rows] == [
            ("2021-06-01", 1.0),
            ("2021-12-01", 2.0),
            ("2022-06-01", 3.0),
        ]

    def test_a_cutoff_before_anything_was_published_resolves_to_nothing(
        self, engine, settings, s3, revised
    ):
        """Absence of knowledge is not zero, and it is not the first estimate."""
        spec = _spec(revised, spine={
            "entities": {"ids": ["AAA"]},
            "times": {"dates": ["2020-12-31"]},
        })
        rows = _resolve(engine, settings, spec)
        assert len(rows) == 1
        assert rows[0][4] is None

    def test_the_boundary_is_inclusive_on_the_publication_date(
        self, engine, settings, s3, revised
    ):
        """Knowable *at* the cutoff counts as knowable."""
        spec = _spec(revised, spine={
            "entities": {"ids": ["AAA"]},
            "times": {"dates": ["2021-03-01"]},
        })
        assert _resolve(engine, settings, spec)[0][4] == 1.0

    def test_a_fixed_cutoff_reproduces_one_moment(self, engine, settings, s3, revised):
        """Every spine row sees the same thing, whatever its own timestamp."""
        spec = _spec(revised, as_of={"policy": "fixed", "at": "2021-06-01"})
        rows = _resolve(engine, settings, spec)
        assert {r[4] for r in rows} == {1.0}

    def test_a_lag_pushes_the_cutoff_back(self, engine, settings, s3, revised):
        """Month T using data through T-1, which is the ordinary alignment."""
        spec = _spec(
            revised,
            as_of={"policy": "lag", "offset": "P6M"},
            spine={"entities": {"ids": ["AAA"]}, "times": {"dates": ["2021-06-01"]}},
        )
        # 2021-06-01 minus six months is 2020-12-01, before anything was published.
        assert _resolve(engine, settings, spec)[0][4] is None


class TestFeatureIsolation:
    def test_a_feature_resolves_against_its_own_timeline(self, engine, settings, s3):
        """The bug the architecture sketch invites.

        Its ASOF example keys on entity alone. With two features whose
        publication schedules differ, that matches one feature's value against
        the other's timeline: the join succeeds, the row count is right, and
        every number is attached to the wrong history.
        """
        name = f"two_{uuid.uuid4().hex[:8]}"
        ingest = datetime(2026, 1, 1)
        _write_facts(
            engine,
            settings,
            s3,
            [
                ("AAA", date(2020, 12, 31), datetime(2021, 3, 1), ingest, "early", 10.0, None),
                ("AAA", date(2020, 12, 31), datetime(2021, 12, 1), ingest, "late", 20.0, None),
            ],
            group_name=name,
        )
        spec = _spec(
            name,
            features=[
                {"group": name, "name": "early"},
                {"group": name, "name": "late"},
            ],
            spine={"entities": {"ids": ["AAA"]}, "times": {"dates": ["2021-06-01"]}},
        )
        rows = {r[3]: r[4] for r in _resolve(engine, settings, spec)}
        assert rows["early"] == 10.0, "published in March, knowable in June"
        assert rows["late"] is None, "not published until December; June cannot see it"


class TestStaleness:
    def test_max_age_nulls_a_value_that_carried_too_far(self, engine, settings, s3, revised):
        """A 2019 figure and last week's look identical without this."""
        spec = _spec(
            revised,
            features=[{"group": revised, "name": "gdp", "max_age": "P1M"}],
            spine={"entities": {"ids": ["AAA"]}, "times": {"dates": ["2022-12-01"]}},
        )
        row = _resolve(engine, settings, spec)[0]
        assert row[4] is None, "the newest fact is nine months old, beyond P1M"

    def test_the_age_goes_with_the_value(self, engine, settings, s3, revised):
        """Nulling a value must null its metadata too.

        An age beside an empty cell says the store knows how old nothing is.
        """
        spec = _spec(
            revised,
            features=[{"group": revised, "name": "gdp", "max_age": "P1M"}],
            spine={"entities": {"ids": ["AAA"]}, "times": {"dates": ["2022-12-01"]}},
        )
        row = _resolve(engine, settings, spec)[0]
        assert row[6] is None, "knowledge_time"
        assert row[7] is None, "age_days"

    def test_a_fresh_value_survives_the_same_bound(self, engine, settings, s3, revised):
        spec = _spec(
            revised,
            features=[{"group": revised, "name": "gdp", "max_age": "P2M"}],
            spine={"entities": {"ids": ["AAA"]}, "times": {"dates": ["2021-03-15"]}},
        )
        assert _resolve(engine, settings, spec)[0][4] == 1.0


class TestFreezing:
    def test_materialized_equals_query(self, engine, settings, s3, revised):
        """The materialised path is a cache, not a second implementation.

        Nothing else keeps that true: the two produce the same answer only
        because they run the same function, and this is what notices if they
        ever stop.
        """
        base = _spec(revised).canonical()
        base["name"] = f"eq_{uuid.uuid4().hex[:8]}"
        as_query = ComposeSpec.model_validate({**base, "freeze": {"mode": "query"}})
        as_materialized = ComposeSpec.model_validate(
            {**base, "freeze": {"mode": "materialized"}}
        )
        compose.freeze(engine, settings, as_query, s3=s3)
        compose.freeze(engine, settings, as_materialized, s3=s3)

        left, _ = compose.read(engine, settings, base["name"], 1)
        right, _ = compose.read(engine, settings, base["name"], 2)
        def key(rows):
            return sorted((r[0], str(r[1]), r[3], r[4]) for r in rows)

        assert key(left.fetchall()) == key(right.fetchall())

    def test_a_frozen_version_pins_both_time_axes(self, engine, settings, s3, revised):
        """A cutoff alone is not enough to make a version reproducible."""
        spec = _spec(revised)
        frozen = compose.freeze(engine, settings, spec, s3=s3)
        assert frozen.watermark is not None
        with engine.connect() as connection:
            row = compose.load_version(connection, spec.name, frozen.version_num)
        assert row["ingest_watermark"] == frozen.watermark

    def test_a_later_arrival_does_not_change_a_pinned_answer(
        self, engine, settings, s3, revised
    ):
        """The reason the watermark exists.

        A fact ingested after the freeze, but knowable before the cutoff, would
        otherwise change what a pinned version resolves to — silently, and
        without its hash changing.
        """
        spec = _spec(revised, spine={
            "entities": {"ids": ["AAA"]}, "times": {"dates": ["2022-06-01"]},
        })
        frozen = compose.freeze(engine, settings, spec, s3=s3)
        before, _ = compose.read(engine, settings, spec.name, frozen.version_num)
        original = before.fetchall()[0][4]

        # A correction, knowable long ago, arriving now.
        _write_facts(
            engine,
            settings,
            s3,
            [("AAA", date(2020, 12, 31), datetime(2022, 4, 1), datetime(2026, 6, 1),
              "gdp", 99.0, None)],
            group_name=revised,
        )

        after, warning = compose.read(engine, settings, spec.name, frozen.version_num)
        assert after.fetchall()[0][4] == original, "a pinned version must not move"
        assert warning["facts"] == 1, "and must say what it is withholding"

        latest, _ = compose.read(
            engine, settings, spec.name, frozen.version_num, latest=True
        )
        assert latest.fetchall()[0][4] == 99.0, "--latest sees what we know now"


class TestRefusals:
    def test_an_unknown_group_lists_what_exists(self, engine, settings, s3, revised):
        with pytest.raises(ComposeError, match="Available"):
            _resolve(engine, settings, _spec("no_such_group"))

    def test_a_non_canonical_group_is_refused(self, engine, settings, s3):
        """The unmapped sibling holds ids that only look like canonical ones."""
        name = f"unmapped_{uuid.uuid4().hex[:8]}"
        _write_facts(
            engine,
            settings,
            s3,
            [("ZZZ", date(2020, 12, 31), datetime(2021, 3, 1), datetime(2026, 1, 1),
              "gdp", 1.0, None)],
            group_name=name,
            namespace="some-native-vocab",
        )
        with pytest.raises(ComposeError, match="different vocabularies"):
            _resolve(engine, settings, _spec(name))

    def test_a_group_with_no_finished_run_is_refused(self, engine, settings, s3, revised):
        """Reading a half-ingested group would answer from part of the data."""
        with engine.begin() as connection:
            group_id = connection.execute(
                sa.select(feature_group.c.id).where(feature_group.c.name == revised)
            ).scalar_one()
            connection.execute(
                sa.text(
                    "update run set status='running' where id in "
                    "(select created_by_run_id from feature_partition where "
                    "feature_group_id = :g)"
                ),
                {"g": group_id},
            )
        try:
            with pytest.raises(ComposeError, match="never finished"):
                _resolve(engine, settings, _spec(revised))
        finally:
            # Put the run back. A run left `running` blocks prune_orphans for
            # the rest of the session, correctly: bytes are written before their
            # catalog row, so an unfinished run means an object that has landed
            # and not yet been recorded is indistinguishable from an orphan.
            with engine.begin() as connection:
                connection.execute(
                    sa.text(
                        "update run set status='ok' where id in "
                        "(select created_by_run_id from feature_partition where "
                        "feature_group_id = :g)"
                    ),
                    {"g": group_id},
                )


def test_read_writes_parquet_to_a_path(engine, settings, s3, revised, tmp_path):
    """`read --out` had never been executed; it is one call and no test."""
    spec = _spec(revised)
    frozen = compose.freeze(engine, settings, spec, s3=s3)
    relation, _ = compose.read(engine, settings, spec.name, frozen.version_num)

    out = tmp_path / "panel.parquet"
    relation.to_parquet(str(out))
    assert out.exists()

    from feature_store.store import duck

    rows = duck.connect().execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    assert rows == 3, "one row per spine timestamp"
