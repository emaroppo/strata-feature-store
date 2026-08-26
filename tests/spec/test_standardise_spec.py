"""The standardisation config: what it accepts, and what it refuses.

Most of these are refusals. A config decides what a number means, so the
failure mode is not a crash — it is a store full of plausible values that are
quietly wrong. Every rejection here stands in for one of those.
"""

from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from feature_store.spec.standardise import (
    FeatureMetaSpec,
    KnowledgeTimeSpec,
    StandardiseSpec,
    SuffixRule,
    decompose,
)

MINIMAL = {
    "feature_group": "g",
    "source": "s",
    "entity": {"column": "code"},
    "event_time": {"column": "d", "period": "annual"},
    "knowledge_time": {"basis": "file_release", "release_date": "2024-04-15"},
    "features": {"name": {"allow_unlisted": True}},
}


def spec(**overrides):
    payload = {**MINIMAL, **overrides}
    return StandardiseSpec.model_validate(payload)


class TestKnowledgeTime:
    """Each basis needs exactly what it needs, and nothing it would ignore."""

    def test_file_release_requires_a_vintage(self):
        with pytest.raises(ValidationError, match="release_date"):
            KnowledgeTimeSpec(basis="file_release")

    def test_publisher_timestamp_requires_its_column(self):
        with pytest.raises(ValidationError, match="column"):
            KnowledgeTimeSpec(basis="publisher_timestamp")

    def test_modeled_lag_requires_a_lag(self):
        with pytest.raises(ValidationError, match="declared_lag"):
            KnowledgeTimeSpec(basis="modeled_lag")

    def test_a_lag_on_a_stored_basis_is_refused(self):
        """It would be silently ignored, which is worse than being rejected."""
        with pytest.raises(ValidationError, match="silently ignored"):
            KnowledgeTimeSpec(
                basis="file_release", release_date=date(2024, 4, 15), declared_lag="P18M"
            )

    def test_the_lag_must_parse(self):
        with pytest.raises(ValidationError):
            KnowledgeTimeSpec(basis="modeled_lag", declared_lag="eighteen months")

    def test_fallback_needs_something_to_fall_back_to(self):
        with pytest.raises(ValidationError, match="fall back"):
            KnowledgeTimeSpec(
                basis="publisher_timestamp", column="t", on_null="fallback_to_file_release"
            )

    def test_on_null_defaults_to_error(self):
        """A null knowledge_time has no safe default, so it has none."""
        assert KnowledgeTimeSpec(basis="publisher_timestamp", column="t").on_null == "error"

    def test_the_seed_panels_rule_validates(self):
        parsed = KnowledgeTimeSpec(
            basis="modeled_lag",
            declared_lag="P2M12D",
            lag_anchor_period="annual",
            rationale="annual publication in March",
        )
        assert parsed.lag.months == 2
        assert parsed.lag.days == 12

    def test_an_unknown_anchor_period_is_refused(self):
        with pytest.raises(ValidationError, match="lag_anchor_period"):
            KnowledgeTimeSpec(
                basis="modeled_lag", declared_lag="P1M", lag_anchor_period="fortnight"
            )


class TestStrictness:
    def test_an_unknown_key_is_an_error(self):
        """A misspelled key that is ignored means the config lies about itself."""
        with pytest.raises(ValidationError):
            spec(retracton_policy="ignore")

    def test_an_unknown_period_is_refused(self):
        with pytest.raises(ValidationError, match="period"):
            spec(event_time={"column": "d", "period": "fortnightly"})


class TestLayout:
    # The `long` layout's coherence checks are gone with it: the layout is
    # refused outright until the melt can read one, so there is nothing to
    # validate. See TestUnimplementedIsRefused.

    def test_features_in_columns_takes_names_from_the_headers(self):
        with pytest.raises(ValidationError, match="column_name"):
            spec(features={"name": {"source": "column", "column": "c"}})


class TestDefaults:
    def test_allow_unlisted_is_false_by_default(self):
        """A new upstream column is a feature nobody named."""
        parsed = StandardiseSpec.model_validate({**MINIMAL, "features": {}})
        assert parsed.features.name.allow_unlisted is False

    def test_retraction_defaults_to_honouring(self):
        assert spec().retraction_policy == "honour"

    def test_the_anchor_defaults_to_the_period_end(self):
        assert spec().event_time.anchor == "end"


class TestSuffixDecomposition:
    """The grammar that makes four thousand feature names selectable."""

    meta = FeatureMetaSpec(
        suffixes=[
            SuffixRule(suffix="_codelow", statistic="ci_low"),
            SuffixRule(suffix="_codehigh", statistic="ci_high"),
            SuffixRule(suffix="_sd", statistic="sd"),
            SuffixRule(suffix="_nr", statistic="coder_count", scale="raw"),
            SuffixRule(suffix="_osp", scale="original"),
            SuffixRule(suffix="_ord", scale="ordinal"),
        ],
        default_scale="latent",
        default_statistic="point",
    )

    def test_a_bare_name_takes_the_defaults(self):
        assert decompose("v2clacjstm", self.meta) == {
            "base": "v2clacjstm",
            "scale": "latent",
            "statistic": "point",
        }

    @pytest.mark.parametrize(
        ("name", "scale", "statistic"),
        [
            ("v2clacjstm_codelow", "latent", "ci_low"),
            ("v2clacjstm_sd", "latent", "sd"),
            ("v2clacjstm_osp", "original", "point"),
            ("v2clacjstm_ord", "ordinal", "point"),
            ("v2clacjstm_nr", "raw", "coder_count"),
        ],
    )
    def test_single_suffixes(self, name, scale, statistic):
        got = decompose(name, self.meta)
        assert (got["base"], got["scale"], got["statistic"]) == ("v2clacjstm", scale, statistic)

    @pytest.mark.parametrize(
        ("name", "scale", "statistic"),
        [
            ("v2clacjstm_osp_codelow", "original", "ci_low"),
            ("v2clacjstm_osp_sd", "original", "sd"),
            ("v2clacjstm_ord_codehigh", "ordinal", "ci_high"),
        ],
    )
    def test_two_part_suffixes_decompose_fully(self, name, scale, statistic):
        """The case that makes hand-written suffix matching unreliable.

        A single pass reads `_osp_codelow` as either an `_osp` column, losing
        the interval, or a `_codelow` column, losing the scale. Both answers are
        wrong and neither looks wrong.
        """
        got = decompose(name, self.meta)
        assert (got["base"], got["scale"], got["statistic"]) == ("v2clacjstm", scale, statistic)

    def test_the_outermost_suffix_wins_its_axis(self):
        """`_osp_codelow` is a low bound on the original scale, not two scales."""
        got = decompose("v2clacjstm_osp_codelow", self.meta)
        assert got["statistic"] == "ci_low"
        assert got["scale"] == "original"

    def test_a_name_that_is_only_a_suffix_is_left_alone(self):
        """Stripping to nothing would invent an empty base."""
        assert decompose("_sd", self.meta)["base"] == "_sd"

    def test_no_grammar_means_no_structure_claimed(self):
        assert decompose("v2clacjstm_osp_codelow", None) == {"base": "v2clacjstm_osp_codelow"}


class TestUnimplementedIsRefused:
    """The store refuses what it cannot do, rather than doing something else.

    Every one of these validated happily until it was closed, and each failed in
    a way that looked like data rather than like a missing feature. Refusing an
    option costs an afternoon; ignoring one costs a result nobody traces back.
    """

    def test_the_long_layout_is_refused(self):
        """The melt always unpivots, so a long file would be reshaped to nonsense."""
        with pytest.raises(ValidationError, match="not implemented"):
            spec(layout="long")

    @pytest.mark.parametrize("fmt", ["parquet", "json", "xlsx"])
    def test_unimplemented_readers_are_refused(self, fmt):
        """Every source is read with read_csv, whatever the config says.

        A parquet file read as CSV does not fail cleanly; it produces garbage
        that reads like a badly formatted source.
        """
        with pytest.raises(ValidationError, match="not implemented"):
            spec(reader={"format": fmt})

    def test_csv_is_accepted(self):
        assert spec(reader={"format": "csv"}).reader.format == "csv"

    def test_the_refusal_says_what_to_use_instead(self):
        """An error that only says no costs a search through the source."""
        try:
            spec(layout="long")
        except ValidationError as exc:
            assert "features_in_columns" in str(exc)
