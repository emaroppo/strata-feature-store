"""The composition spec: a spine, a set of features, and the cutoff each is read at.

A composition retrieves and never computes; anything with a coefficient in it
belongs to post-processing. Its hash is the identity of a frozen version.
See ``docs/adr/0007``.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .duration import parse_duration
from .periods import ANCHORS, PERIODS

SCHEMA_VERSION = 1


class Strict(BaseModel):
    """Unknown keys are an error; a misspelled one would silently do nothing."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class EntityList(Strict):
    """Where the spine's entities come from; exactly one source, each pinned by hash or watermark.

    See ``docs/adr/0007``.
    """

    #: Named outright. Not called `list`: that would shadow the builtin in later annotations.
    ids: list[str] | None = None
    #: Every entity present in a feature group.
    from_group: str | None = None
    #: A stored, versioned named list, pinned by `version`.
    name: str | None = None
    version: int | None = None

    @model_validator(mode="after")
    def _exactly_one_source(self):
        chosen = [
            self.ids is not None,
            self.from_group is not None,
            self.name is not None,
        ]
        if sum(chosen) != 1:
            raise ValueError(
                "give the spine's entities exactly one way: `ids`, `from_group`, or "
                "`name` with `version`"
            )
        if self.ids is not None and not self.ids:
            raise ValueError("an empty entity list produces an empty panel; say what you mean")
        if self.name is not None and self.version is None:
            raise ValueError(
                "a stored entity list must be pinned to a version. Redefining a region "
                "would otherwise rewrite what an earlier composition meant."
            )
        return self


class TimeGrid(Strict):
    """The spine's timestamps."""

    #: Named outright, for a handful of dates.
    dates: list[date] | None = None
    #: Or generated: every `step` from `start` to `end` inclusive.
    start: date | None = None
    end: date | None = None
    step: str | None = None
    anchor: str = "end"
    period: str = "monthly"

    @model_validator(mode="after")
    def _coherent(self):
        generated = self.start is not None or self.end is not None or self.step is not None
        if (self.dates is not None) == generated:
            raise ValueError("give the spine's times either as `dates` or as start/end/step")
        if generated:
            if not (self.start and self.end and self.step):
                raise ValueError("a generated grid needs start, end and step")
            if self.end < self.start:
                raise ValueError(f"end {self.end} precedes start {self.start}")
            parse_duration(self.step)
            if self.anchor not in ANCHORS:
                raise ValueError(f"anchor must be one of {ANCHORS}")
            if self.period not in PERIODS:
                raise ValueError(f"period must be one of {PERIODS}")
        elif not self.dates:
            raise ValueError("an empty time list produces an empty panel")
        return self


class Spine(Strict):
    entities: EntityList
    times: TimeGrid


class AsOfException(Strict):
    """One feature read at a different cutoff.

    The reason is required and is part of the hashed spec.
    """

    feature: str
    policy: Literal["spine_time", "lag", "fixed"]
    offset: str | None = None
    at: date | None = None
    reason: str

    @model_validator(mode="after")
    def _policy_has_what_it_needs(self):
        _check_policy(self.policy, self.offset, self.at)
        if not self.reason.strip():
            raise ValueError("an as-of exception needs a stated reason")
        return self


def _check_policy(policy: str, offset: str | None, at: date | None) -> None:
    if policy == "lag":
        if not offset:
            raise ValueError("a 'lag' as-of policy needs an offset")
        parse_duration(offset)
    elif offset:
        raise ValueError(f"a {policy!r} as-of policy has no use for an offset")
    if policy == "fixed" and at is None:
        raise ValueError("a 'fixed' as-of policy needs the date to fix it at")
    if policy != "fixed" and at is not None:
        raise ValueError(f"a {policy!r} as-of policy has no use for a fixed date")


class AsOf(Strict):
    """When each spine row is allowed to know things.

    ``spine_time``  the cutoff is the spine timestamp itself
    ``lag``         the spine timestamp minus an offset, which is the ordinary
                    "month T uses data through T-1" alignment
    ``fixed``       one constant cutoff, for reproducing what was visible then
    """

    policy: Literal["spine_time", "lag", "fixed"] = "spine_time"
    offset: str | None = None
    at: date | None = None
    exceptions: list[AsOfException] = Field(default_factory=list)

    @model_validator(mode="after")
    def _policy_has_what_it_needs(self):
        _check_policy(self.policy, self.offset, self.at)
        seen = [e.feature for e in self.exceptions]
        if len(seen) != len(set(seen)):
            raise ValueError("a feature has more than one as-of exception")
        return self


class FeatureRef(Strict):
    group: str
    name: str
    #: What to call it in the output. Defaults to the feature's own name.
    alias: str | None = None
    #: Beyond this age a value resolves to null rather than carrying forward.
    max_age: str | None = None

    @model_validator(mode="after")
    def _valid_age(self):
        if self.max_age:
            parse_duration(self.max_age)
        return self

    @property
    def output_name(self) -> str:
        return self.alias or self.name


class Output(Strict):
    #: Wide is a presentation, never a storage layout; see ``docs/adr/0002``.
    orientation: Literal["long", "wide"] = "long"
    #: Carry the resolved knowledge_time.
    include_knowledge_time: bool = True
    #: Carry its distance from the cutoff.
    include_age_days: bool = True
    #: Carry the cutoff itself as `data_through`, derived from the as-of policy.
    include_data_through: bool = True


class Freeze(Strict):
    mode: Literal["query", "materialized"] = "query"


class ComposeSpec(Strict):
    """One named, reproducible retrieval."""

    schema_version: int = SCHEMA_VERSION
    name: str
    spine: Spine
    features: list[FeatureRef]
    as_of: AsOf = Field(default_factory=AsOf)
    output: Output = Field(default_factory=Output)
    freeze: Freeze = Field(default_factory=Freeze)
    description: str | None = None

    @model_validator(mode="after")
    def _features_are_usable(self):
        if not self.features:
            raise ValueError("a composition with no features produces nothing")
        names = [f.output_name for f in self.features]
        duplicates = {n for n in names if names.count(n) > 1}
        if duplicates:
            raise ValueError(
                f"two features would land in the same output column: {sorted(duplicates)}. "
                "Give one of them an alias."
            )
        known = {f.name for f in self.features} | {f.output_name for f in self.features}
        unknown = [e.feature for e in self.as_of.exceptions if e.feature not in known]
        if unknown:
            raise ValueError(f"as-of exceptions name features the spec does not select: {unknown}")
        return self

    @property
    def groups(self) -> list[str]:
        """Every feature group this spec reads, in first-seen order."""
        return list(dict.fromkeys(f.group for f in self.features))

    def canonical(self) -> dict[str, Any]:
        """The form that gets hashed: every default materialised, nothing implied."""
        return self.model_dump(mode="python")
