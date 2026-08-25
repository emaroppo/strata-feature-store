"""The standardisation config: how a source file's columns become long-format facts.

This maps columns onto the seven-column schema and **does nothing else**. It
cannot compute, rescale, impute, aggregate or derive a value. If a config here
starts wanting an expression language, that is the separate post-processing
repository asking to exist, and letting it in here would make this a
transformation engine that happens to store things.

What it *may* do is decide what a number means — which column is the entity,
which is the time, what a feature is called, when the value became knowable.
Those are declarations, not computations, and they are exactly what has to be
written down rather than inferred.

Everything is overridable from the command line, and the hash that identifies a
run is taken over the effective config *after* overrides. Hashing the file on
disk would make two runs of one release under different release dates look
identical.
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
    """Unknown keys are an error.

    A misspelled key that is silently ignored means the config does not say what
    its author thinks it says, and the resulting data is wrong in a way nothing
    reports.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)


class ReaderSpec(Strict):
    format: Literal["csv", "parquet", "json", "xlsx"] = "csv"
    #: Passed to the reader. Kept opaque so a format's own options do not have to
    #: be re-declared here every time one is added.
    options: dict[str, Any] = Field(default_factory=dict)


class EntitySpec(Strict):
    column: str
    normalise: list[Normalisation] = Field(default_factory=list)
    #: Rows with no entity id cannot be attached to anything.
    drop_if_null: bool = True


class EventTimeSpec(Strict):
    column: str
    #: strptime format. Omitted means the reader already produced a date.
    format: str | None = None
    period: str = "irregular"
    anchor: str = "end"

    @model_validator(mode="after")
    def _known_period_and_anchor(self):
        if self.period not in PERIODS:
            raise ValueError(f"period must be one of {PERIODS}, not {self.period!r}")
        if self.anchor not in ANCHORS:
            raise ValueError(f"anchor must be one of {ANCHORS}, not {self.anchor!r}")
        return self


class KnowledgeTimeSpec(Strict):
    """When a value became available — never when the file was downloaded."""

    basis: Literal["publisher_timestamp", "file_release", "modeled_lag", "ingest_fallback"]

    #: file_release: the publisher's vintage date for this release.
    release_date: date | None = None
    #: publisher_timestamp: the column carrying a per-row availability time.
    column: str | None = None
    format: str | None = None
    #: modeled_lag: ISO-8601, e.g. 'P18M'.
    declared_lag: str | None = None
    #: modeled_lag: measure the lag from the end of this period rather than from
    #: the raw event date. A publisher that releases annually publishes January
    #: and December of one year together; lagging each event date separately
    #: would claim they became knowable a year apart.
    lag_anchor_period: str | None = None
    #: Why this rule, in the author's words. Carried into the catalog because a
    #: lag with no stated reason cannot be reviewed, only inherited.
    rationale: str | None = None

    #: A null knowledge_time has no safe default. In a publisher_timestamp group
    #: it is indistinguishable at read from a modeled_lag group's deliberate
    #: null, so the COALESCE would silently apply a lag that group never
    #: declared. Say what to do, or it is an error.
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
    """How to read structure out of feature names.

    Publishers with thousands of columns encode structure in the names: a base
    quantity, the scale it is expressed on, and which summary statistic this
    particular column is. Recording that structure makes "the point estimates"
    selectable without listing four thousand names or matching suffixes by hand
    at query time.

    This is annotation, not derivation. It records what a feature *is*; it never
    touches a value.
    """

    #: Applied longest-first and repeatedly, so a two-part suffix decomposes
    #: fully. Matching once would read `_osp_codelow` as an `_osp` column and
    #: lose the interval, or as a `_codelow` column and lose the scale.
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
    #: False by default: a new upstream column is a feature nobody named, and
    #: taking it under its raw code is how a vocabulary registry rots. Sources
    #: whose published names *are* the vocabulary set this true, deliberately.
    allow_unlisted: bool = False


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


class StandardiseSpec(Strict):
    """One source file, mapped onto the long schema."""

    schema_version: int = SCHEMA_VERSION
    feature_group: str
    source: str
    reader: ReaderSpec = Field(default_factory=ReaderSpec)
    layout: Literal["wide", "long"] = "wide"
    entity: EntitySpec
    event_time: EventTimeSpec
    knowledge_time: KnowledgeTimeSpec
    features: FeatureSelection
    #: Whether this publisher's nulls withdraw a value or mean "not reported".
    #: Opposite meanings the format cannot distinguish, so it is declared.
    retraction_policy: Literal["honour", "ignore"] = "honour"
    #: Crosswalk pinned for converting native entity ids to canonical ones.
    #: Required when the source's vocabulary is not the canonical one.
    crosswalk: CrosswalkPin | None = None
    description: str | None = None

    @model_validator(mode="after")
    def _layout_is_coherent(self):
        if self.layout == "long":
            if self.features.name.source != "column":
                raise ValueError("a long layout needs features.name.source = 'column'")
            if not self.features.name.column:
                raise ValueError("a long layout needs features.name.column")
            if not self.features.value_column:
                raise ValueError("a long layout needs features.value_column")
        else:
            if self.features.name.source != "column_name":
                raise ValueError("a wide layout needs features.name.source = 'column_name'")
        return self


class CrosswalkPin(Strict):
    dataset: str
    version: int


StandardiseSpec.model_rebuild()


def decompose(name: str, meta: FeatureMetaSpec | None) -> dict[str, Any]:
    """Split a feature name into its base and what its suffixes declare.

    Suffixes are stripped repeatedly, longest first, so a name carrying two of
    them decomposes fully:

        v2clacjstm_osp_codelow -> base v2clacjstm, scale original, statistic ci_low

    A single pass would report the base as ``v2clacjstm_osp`` and invent a
    feature family that does not exist — which is exactly the mistake that makes
    hand-written suffix matching at query time unreliable.
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
            # First match wins per axis: the outermost suffix is the more
            # specific statement, and an inner one must not overwrite it.
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
