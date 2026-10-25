"""The standardisation config: how a source file's columns become long-format facts.

It declares what a column means and never computes a value; derivation belongs
to post-processing. The run hash is taken over the effective config after
command-line overrides, so two runs under different release dates differ.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .duration import Duration, parse_duration
from .periods import ANCHORS, PERIODS

SCHEMA_VERSION = 1

Normalisation = Literal["strip", "upper", "lower"]


class Strict(BaseModel):
    """Unknown keys are an error; a misspelled one would silently do nothing."""

    model_config = ConfigDict(extra="forbid", frozen=True)


#: Formats the reader can open; the type below is wider so an unimplemented one is named as such.
IMPLEMENTED_FORMATS = frozenset({"csv"})


class ReaderSpec(Strict):
    format: Literal["csv", "parquet", "json", "xlsx"] = "csv"
    #: Passed to the reader verbatim.
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _format_is_implemented(self):
        if self.format not in IMPLEMENTED_FORMATS:
            raise ValueError(
                f"reader format {self.format!r} is declared but not implemented: every "
                f"source is read with read_csv. Only {sorted(IMPLEMENTED_FORMATS)} works "
                "today. Convert the file, or implement the reader — but do not let this "
                "through, because a parquet file read as CSV fails in ways that look like "
                "bad data rather than a missing feature."
            )
        return self


class EntitySpec(Strict):
    column: str
    normalise: list[Normalisation] = Field(default_factory=list)
    #: Rows with no entity id cannot be attached to anything.
    drop_if_null: bool = True


class EventTimeSpec(Strict):
    #: Which column carries the time; omitted under the `periods_in_columns` layout.
    column: str | None = None
    #: Set when each melted column name is a period label rather than a feature.
    from_column_name: bool = False
    #: strptime format. Omitted means the reader already produced a date.
    format: str | None = None
    period: str = "irregular"
    #: Which end of the period `event_time` names; see ``docs/adr/0001``.
    anchor: str = "end"

    @model_validator(mode="after")
    def _known_period_and_anchor(self):
        if self.from_column_name and self.column:
            raise ValueError("event_time comes from the column names or from a column, not both")
        if not self.from_column_name and not self.column:
            raise ValueError("event_time needs a column, or from_column_name")
        if self.from_column_name and self.period == "irregular":
            raise ValueError(
                "a period label needs a period to anchor within; 'irregular' has no "
                "boundary to anchor to"
            )
        if self.period not in PERIODS:
            raise ValueError(f"period must be one of {PERIODS}, not {self.period!r}")
        if self.anchor not in ANCHORS:
            raise ValueError(f"anchor must be one of {ANCHORS}, not {self.anchor!r}")
        return self


class KnowledgeTimeSpec(Strict):
    """When a value became available, and how that was determined. See ``docs/adr/0003``."""

    basis: Literal["publisher_timestamp", "file_release", "modeled_lag", "ingest_fallback"]

    #: file_release: the publisher's vintage date for this release.
    release_date: date | None = None
    #: publisher_timestamp: the column carrying a per-row availability time.
    column: str | None = None
    format: str | None = None
    #: modeled_lag: ISO-8601, e.g. 'P18M'.
    declared_lag: str | None = None
    #: modeled_lag: measure the lag from the end of this period rather than the raw event date.
    lag_anchor_period: str | None = None
    #: Why this rule, in the author's words; carried into the catalog.
    rationale: str | None = None

    #: What a null knowledge_time means here; it has no safe default, see ``docs/adr/0003``.
    on_null: Literal["error", "drop", "fallback_to_file_release"] = "error"

    @model_validator(mode="after")
    def _basis_has_what_it_needs(self):
        if self.basis == "file_release" and self.release_date is None:
            raise ValueError(
                "a file_release basis needs release_date: the publisher's vintage, "
                "which is not the download date"
            )
        if self.basis == "publisher_timestamp" and not self.column:
            raise ValueError("a publisher_timestamp basis needs the column carrying it")
        if self.basis == "modeled_lag":
            if not self.declared_lag:
                raise ValueError(
                    "a modeled_lag basis needs declared_lag; there is nothing to derive"
                )
            parse_duration(self.declared_lag)
            if self.lag_anchor_period and self.lag_anchor_period not in PERIODS:
                raise ValueError(f"lag_anchor_period must be one of {PERIODS}")
        if self.basis != "modeled_lag" and self.declared_lag:
            raise ValueError(
                f"a {self.basis} basis stores knowledge_time, so declared_lag would be "
                "silently ignored"
            )
        if self.on_null == "fallback_to_file_release" and self.release_date is None:
            raise ValueError(
                "on_null='fallback_to_file_release' needs a release_date to fall back to"
            )
        return self

    @property
    def lag(self) -> Duration | None:
        return parse_duration(self.declared_lag) if self.declared_lag else None


class SuffixRule(Strict):
    """One suffix, and what it says about the feature carrying it."""

    suffix: str
    scale: str | None = None
    statistic: str | None = None


class FeatureMetaSpec(Strict):
    """How to read a base quantity, scale and statistic out of a feature name.

    Annotation only; it never touches a value.
    """

    #: Applied longest-first and repeatedly, so a two-part suffix decomposes fully.
    suffixes: list[SuffixRule] = Field(default_factory=list)
    default_scale: str | None = None
    default_statistic: str | None = None


class FeatureNameSpec(Strict):
    #: 'column_name' for a wide layout, where each column is a feature.
    #: 'column' for a long one, where a column holds the feature's name.
    source: Literal["column_name", "column"] = "column_name"
    #: Long layout only: which column carries the name.
    column: str | None = None
    rename: dict[str, str] = Field(default_factory=dict)
    #: Only these, if given.
    allow: list[str] | None = None
    #: Whether a column absent from `rename` may be ingested under its raw name.
    allow_unlisted: bool = False


class VocabularySpec(Strict):
    """A companion release describing the features; it never decides what is ingested.

    Pinned to a release digest so a reissued file cannot change what an old
    ingest claimed a number meant.
    """

    #: Which release describes the features. A digest, or an unambiguous prefix.
    release: str
    #: The column holding the feature name, matched against the vocabulary.
    key: str
    description: str | None = None
    unit: str | None = None
    #: Columns carried into `feature.meta` verbatim, under their own names.
    meta: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _describes_something(self):
        if not (self.description or self.unit or self.meta):
            raise ValueError(
                "a vocabulary that names no description, unit or meta columns describes "
                "nothing; omit it rather than reading a file to no effect"
            )
        return self


class FeatureSelection(Strict):
    #: Wide layout: columns that are identifiers rather than features.
    exclude: list[str] = Field(default_factory=list)
    #: Wide layout: if given, only these columns are features.
    include: list[str] | None = None
    name: FeatureNameSpec = Field(default_factory=FeatureNameSpec)
    kind: Literal["numeric", "string"] = "numeric"
    #: Long layout: which column holds the value.
    value_column: str | None = None
    units: dict[str, str] = Field(default_factory=dict)
    meta: FeatureMetaSpec | None = None
    #: A companion release describing what the features mean.
    vocabulary: VocabularySpec | None = None


class StandardiseSpec(Strict):
    """One source file, mapped onto the long schema."""

    schema_version: int = SCHEMA_VERSION
    feature_group: str
    source: str
    reader: ReaderSpec = Field(default_factory=ReaderSpec)
    #: What the melted column headers are: features, periods, or nothing to melt.
    layout: Literal["features_in_columns", "periods_in_columns", "long"] = "features_in_columns"
    entity: EntitySpec
    event_time: EventTimeSpec
    knowledge_time: KnowledgeTimeSpec
    features: FeatureSelection
    #: Whether this publisher's nulls withdraw a value; see ``docs/adr/0009``.
    retraction_policy: Literal["honour", "ignore"] = "honour"
    #: Crosswalk pinned for canonicalising entity ids; required for a non-canonical source.
    crosswalk: CrosswalkPin | None = None
    description: str | None = None

    @model_validator(mode="after")
    def _layout_is_coherent(self):
        if self.layout == "long":
            # Kept in the type because the shape is real; refused until the melt can read it.
            raise ValueError(
                "layout 'long' is declared but not implemented: the melt always "
                "unpivots, so a file already in long form would be reshaped into "
                "nonsense. Use features_in_columns or periods_in_columns."
            )
        if self.layout == "features_in_columns":
            if self.features.name.source != "column_name":
                raise ValueError(
                    "features_in_columns takes feature names from the column headers, so "
                    "features.name.source must be 'column_name'"
                )
            if self.event_time.from_column_name:
                raise ValueError("the column headers cannot be both feature names and periods")
            if not self.event_time.column:
                raise ValueError("features_in_columns needs event_time.column")
        else:  # periods_in_columns
            if not self.event_time.from_column_name:
                raise ValueError(
                    "periods_in_columns takes event times from the column headers, so "
                    "event_time.from_column_name must be true"
                )
            if self.features.name.source != "column":
                raise ValueError(
                    "periods_in_columns takes feature names from a column, so "
                    "features.name.source must be 'column'"
                )
            if not self.features.name.column:
                raise ValueError("periods_in_columns needs features.name.column")
        return self


class CrosswalkPin(Strict):
    dataset: str
    version: int


StandardiseSpec.model_rebuild()


def decompose(name: str, meta: FeatureMetaSpec | None) -> dict[str, Any]:
    """Split a feature name into its base and what its suffixes declare.

    Suffixes are stripped longest-first, repeatedly, until none match.
    """
    if meta is None:
        return {"base": name}

    attributes: dict[str, Any] = {}
    stem = name
    rules = sorted(meta.suffixes, key=lambda r: len(r.suffix), reverse=True)
    changed = True
    while changed:
        changed = False
        for rule in rules:
            if not stem.endswith(rule.suffix) or len(stem) <= len(rule.suffix):
                continue
            # First match wins per axis: the outermost suffix is the more specific statement.
            if rule.scale is not None:
                attributes.setdefault("scale", rule.scale)
            if rule.statistic is not None:
                attributes.setdefault("statistic", rule.statistic)
            stem = stem[: -len(rule.suffix)]
            changed = True
            break

    if meta.default_scale is not None:
        attributes.setdefault("scale", meta.default_scale)
    if meta.default_statistic is not None:
        attributes.setdefault("statistic", meta.default_statistic)
    return {"base": stem, **attributes}
