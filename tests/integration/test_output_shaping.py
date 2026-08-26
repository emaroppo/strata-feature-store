"""Output orientation, the include flags, retraction, and stored entity lists.

Every one of these was a field the spec accepted and the store ignored. That is
the worst shape a gap can take: the run succeeds, nothing errors, and the answer
is quietly not the one that was asked for. These tests exist so that stays
fixed.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

import pytest
import sqlalchemy as sa

from feature_store.spec.compose import ComposeSpec
from feature_store.store import catalog, compose
from feature_store.store.compose import ComposeError
from feature_store.store.runs import run as open_run

from ..pit import panel

pytestmark = pytest.mark.integration


def _facts(feature: str = "gdp", *, entity: str = "AAA") -> list[panel.Fact]:
    return [
        panel.Fact(entity, date(2020, 12, 31), datetime(2021, 3, 1), feature, 1.0),
        panel.Fact(entity, date(2021, 12, 31), datetime(2022, 3, 1), feature, 2.0),
    ]


@pytest.fixture
def group(engine, settings, s3) -> str:
    return panel.build_store(engine, settings, s3, _facts() + _facts("pop"))


def _register_features(engine, group_name: str, features: dict[str, str]) -> None:
    """Override the dtype build_store recorded, for the string cases."""
    from feature_store.store.tables import feature as feature_table
    from feature_store.store.tables import feature_group

    with engine.begin() as connection:
        group_id = connection.execute(
            sa.select(feature_group.c.id).where(feature_group.c.name == group_name)
        ).scalar_one()
        for name, dtype in features.items():
            connection.execute(
                sa.update(feature_table)
                .where(feature_table.c.feature_group_id == group_id, feature_table.c.name == name)
                .values(dtype=dtype)
            )


def _spec(group: str, **overrides) -> ComposeSpec:
    payload = {
        "name": f"shape_{uuid.uuid4().hex[:8]}",
        "spine": {
            "entities": {"ids": ["AAA"]},
            "times": {"dates": ["2021-06-01", "2022-06-01"]},
        },
        "features": [{"group": group, "name": "gdp"}, {"group": group, "name": "pop"}],
        "as_of": {"policy": "spine_time"},
    }
    payload.update(overrides)
    return ComposeSpec.model_validate(payload)


def _relation(engine, settings, spec):
    with engine.connect() as connection:
        plan = compose.plan(connection, spec, settings)
    return compose.resolve(plan)


class TestOrientation:
    def test_long_is_one_row_per_sample(self, engine, settings, group):
        rows = _relation(engine, settings, _spec(group)).fetchall()
        assert len(rows) == 4, "two spine rows times two features"

    def test_wide_is_one_row_per_observation(self, engine, settings, group):
        rows = _relation(
            engine, settings, _spec(group, output={"orientation": "wide"})
        ).fetchall()
        assert len(rows) == 2, "the same four samples, presented as two rows"

    def test_wide_names_a_column_per_feature(self, engine, settings, group):
        relation = _relation(engine, settings, _spec(group, output={"orientation": "wide"}))
        assert "gdp" in relation.columns
        assert "pop" in relation.columns

    def test_wide_uses_the_alias(self, engine, settings, group):
        spec = _spec(
            group,
            features=[{"group": group, "name": "gdp", "alias": "output_gap"}],
            output={"orientation": "wide"},
        )
        assert "output_gap" in _relation(engine, settings, spec).columns

    def test_wide_holds_the_same_samples(self, engine, settings, group):
        """A pivot is a presentation, not a different set of facts."""
        long_rows = _relation(engine, settings, _spec(group)).fetchall()
        wide = _relation(engine, settings, _spec(group, output={"orientation": "wide"}))
        wide_rows = wide.fetchall()

        # Look the value columns up by name. Hardcoding positions makes the
        # test agree with itself rather than with the output.
        value_at = [wide.columns.index(name) for name in ("gdp", "pop")]
        long_values = sorted(r[4] for r in long_rows if r[4] is not None)
        wide_values = sorted(
            row[i] for row in wide_rows for i in value_at if row[i] is not None
        )
        assert long_values == wide_values

    def test_a_feature_that_resolved_to_nothing_still_gets_a_column(
        self, engine, settings, group
    ):
        """Written out explicitly rather than with a dynamic PIVOT.

        DuckDB's PIVOT infers its columns from the data, so a feature that
        resolved to nothing for every row would silently vanish and the output
        would have a different shape than the spec asked for.
        """
        spec = _spec(
            group,
            spine={"entities": {"ids": ["AAA"]}, "times": {"dates": ["2019-01-01"]}},
            output={"orientation": "wide"},
        )
        relation = _relation(engine, settings, spec)
        assert "gdp" in relation.columns and "pop" in relation.columns
        assert relation.fetchall()[0][3] is None


class TestIncludeFlags:
    def test_all_on_by_default(self, engine, settings, group):
        columns = _relation(engine, settings, _spec(group)).columns
        assert {"data_through", "knowledge_time", "age_days"} <= set(columns)

    def test_each_can_be_turned_off(self, engine, settings, group):
        spec = _spec(
            group,
            output={
                "include_data_through": False,
                "include_knowledge_time": False,
                "include_age_days": False,
            },
        )
        columns = set(_relation(engine, settings, spec).columns)
        assert not ({"data_through", "knowledge_time", "age_days"} & columns)

    def test_wide_honours_them_per_feature(self, engine, settings, group):
        spec = _spec(
            group,
            output={"orientation": "wide", "include_knowledge_time": False},
        )
        columns = _relation(engine, settings, spec).columns
        assert "gdp__age_days" in columns
        assert "gdp__knowledge_time" not in columns


class TestDtypes:
    def test_a_string_feature_gets_a_string_column(self, engine, settings, s3):
        """Not both columns with one of them null in every row."""
        name = panel.build_store(engine, settings, s3, _facts("label"))
        _register_features(engine, name, {"label": "str"})
        spec = _spec(
            name,
            features=[{"group": name, "name": "label"}],
            output={"orientation": "wide"},
        )
        columns = _relation(engine, settings, spec).columns
        assert "label" in columns
        assert "label__str" not in columns

    def test_an_unknown_feature_is_refused_with_suggestions(self, engine, settings, group):
        """A typo must fail, not resolve to a column of nulls that looks like gaps."""
        with pytest.raises(ComposeError, match="no feature named"):
            _relation(engine, settings, _spec(group, features=[{"group": group, "name": "gdpp"}]))


class TestRetraction:
    """Whether a publisher's null withdraws a value or means 'not reported'."""

    def _with_null(self, engine, settings, s3, policy: str) -> str:
        facts = [
            panel.Fact("AAA", date(2020, 12, 31), datetime(2021, 3, 1), "gdp", 1.0),
            # A later null: a withdrawal under `honour`, filler under `ignore`.
            panel.Fact("AAA", date(2020, 12, 31), datetime(2021, 9, 1), "gdp", None),
        ]
        name = panel.build_store(engine, settings, s3, facts)
        with engine.begin() as connection:
            from feature_store.store.tables import feature_group

            connection.execute(
                sa.update(feature_group)
                .where(feature_group.c.name == name)
                .values(retraction_policy=policy)
            )
        return name

    def test_honour_lets_a_null_withdraw_the_earlier_value(self, engine, settings, s3):
        name = self._with_null(engine, settings, s3, "honour")
        spec = _spec(
            name,
            features=[{"group": name, "name": "gdp"}],
            spine={"entities": {"ids": ["AAA"]}, "times": {"dates": ["2022-01-01"]}},
        )
        assert _relation(engine, settings, spec).fetchall()[0][4] is None

    def test_ignore_lets_the_earlier_value_stand(self, engine, settings, s3):
        """The publisher reports nothing this cycle; the last figure is still the figure."""
        name = self._with_null(engine, settings, s3, "ignore")
        spec = _spec(
            name,
            features=[{"group": name, "name": "gdp"}],
            spine={"entities": {"ids": ["AAA"]}, "times": {"dates": ["2022-01-01"]}},
        )
        assert _relation(engine, settings, spec).fetchall()[0][4] == 1.0


class TestStoredEntityLists:
    def test_a_spine_can_draw_from_a_pinned_list(self, engine, settings, s3, group):
        name = f"region_{uuid.uuid4().hex[:8]}"
        with open_run(engine, "ingest_release", "list", {}) as run:
            catalog.add_entity_list(
                run,
                name=name,
                entity_namespace=panel.NAMESPACE,
                members=["AAA", "BBB"],
            )
        spec = _spec(
            group,
            spine={
                "entities": {"name": name, "version": 1},
                "times": {"dates": ["2022-06-01"]},
            },
        )
        rows = _relation(engine, settings, spec).fetchall()
        assert sorted({r[0] for r in rows}) == ["AAA", "BBB"]

    def test_a_new_version_does_not_change_an_older_pin(self, engine, settings, s3, group):
        """The reason a list is versioned rather than edited in place."""
        name = f"region_{uuid.uuid4().hex[:8]}"
        with open_run(engine, "ingest_release", "list", {}) as run:
            catalog.add_entity_list(
                run, name=name, entity_namespace=panel.NAMESPACE, members=["AAA"]
            )
        with open_run(engine, "ingest_release", "list again", {}) as run:
            _, version_num = catalog.add_entity_list(
                run, name=name, entity_namespace=panel.NAMESPACE, members=["AAA", "BBB", "CCC"]
            )
        assert version_num == 2

        pinned = _spec(
            group,
            spine={
                "entities": {"name": name, "version": 1},
                "times": {"dates": ["2022-06-01"]},
            },
        )
        assert {r[0] for r in _relation(engine, settings, pinned).fetchall()} == {"AAA"}

    def test_an_unknown_version_is_refused(self, engine, settings, group):
        spec = _spec(
            group,
            spine={
                "entities": {"name": "never_defined", "version": 1},
                "times": {"dates": ["2022-06-01"]},
            },
        )
        with pytest.raises(ComposeError, match="no entity list"):
            _relation(engine, settings, spec)

    def test_an_empty_list_is_refused_at_creation(self, engine):
        with pytest.raises(catalog.CatalogError, match="selects nothing"):
            with open_run(engine, "ingest_release", "empty", {}) as run:
                catalog.add_entity_list(
                    run, name="empty", entity_namespace=panel.NAMESPACE, members=[]
                )


class TestStalenessArithmetic:
    def test_a_bound_in_months_uses_the_calendar(self, engine, settings, s3):
        """Not thirty-day months.

        A fact published on 31 January is exactly one calendar month old on
        28 February, and 28 days old by the clock. Under a P1M bound the first
        answer keeps it and the second discards it, and only one of them is what
        the bound says.
        """
        facts = [panel.Fact("AAA", date(2020, 12, 31), datetime(2021, 1, 31), "gdp", 1.0)]
        name = panel.build_store(engine, settings, s3, facts)
        spec = _spec(
            name,
            features=[{"group": name, "name": "gdp", "max_age": "P1M"}],
            spine={"entities": {"ids": ["AAA"]}, "times": {"dates": ["2021-02-28"]}},
        )
        assert _relation(engine, settings, spec).fetchall()[0][4] == 1.0, (
            "one calendar month after 31 January is 28 February, so it is not yet stale"
        )
