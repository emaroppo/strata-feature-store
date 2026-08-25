"""The catalog: what exists, and where it came from.

**No feature values live here.** Postgres holds descriptions, pointers and
lineage; the values are in parquet in the bucket. That division is what keeps
this database in the low megabytes after bulk-ingesting every indicator
collection on offer, and it is why there is no table here with a value column.

Core rather than the ORM. These are tables, and what a query returns is a row
of data — wrapping them in instances buys nothing when nothing has behaviour.

Two conventions worth knowing before reading:

**Additive only.** Nothing is deleted and nothing is renamed. A feature that
stops being published gets a ``deprecated_at``; a partition that is corrected
gets a ``superseded_by_run_id``; a feature whose type changes is a new feature
with a new name. Rewriting the past is how a catalog becomes fiction.

**Enum-like columns are TEXT with a CHECK**, not native Postgres enums.
Widening a CHECK is a cheap migration; widening a native enum is a transactional
one, and these vocabularies are expected to widen.
"""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB

# Set from the very first migration. Retrofitting a naming convention means
# renaming every constraint Postgres already invented a name for, so it is
# cheaper to be right once than to be sorry across a schema.
metadata = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)

SOURCE_KINDS = ("panel", "crosswalk")
DATASET_KINDS = ("composed",)
RESOLUTION_MODES = ("query", "materialized")
TIME_BASES = ("ingest_fallback", "modeled_lag", "file_release", "publisher_timestamp")
GRANULARITIES = ("annual", "quarterly", "monthly", "weekly", "daily", "irregular")
RUN_KINDS = ("ingest_release", "standardise", "register_crosswalk", "compose", "materialize")
RUN_STATUSES = ("running", "ok", "failed", "rolled_back")
REF_TYPES = (
    "release",
    "feature_partition",
    "feature_group",
    "dataset_version",
    "entity_list_version",
)


def _tuple_check(column: str, allowed: tuple[str, ...], name: str) -> CheckConstraint:
    values = ", ".join(f"'{value}'" for value in allowed)
    return CheckConstraint(f"{column} IN ({values})", name=name)


entity_namespace = Table(
    "entity_namespace",
    metadata,
    Column("name", String(64), primary_key=True),
    Column("description", Text),
    # Where the vocabulary is defined, when it is public. Preferring published
    # vocabularies over minting ids that then need maintaining is a rule; this
    # column is what makes following it checkable.
    Column("external_ref", Text),
    Column("created_at", DateTime, nullable=False, server_default=func.now()),
    comment="Registered entity vocabularies. The join key only - no entity knowledge.",
)


source = Table(
    "source",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(128), nullable=False, unique=True),
    Column("kind", String(16), nullable=False, server_default="panel"),
    # What the publisher publishes in, which is not necessarily what we store
    # in. Standardisation converts to the canonical namespace and the stored
    # parquet is always canonical; this records where it started so the
    # crosswalk requirement can be enforced at ingest.
    Column("native_namespace", ForeignKey("entity_namespace.name"), nullable=False),
    # How the publisher emits observations. Read at standardise to check that
    # event times actually land on the declared boundary: an annual source whose
    # dates are not year ends means the anchor is wrong, and a wrong anchor
    # shifts every derived knowledge_time by up to a year.
    Column("granularity", String(16), nullable=False),
    Column("licence", Text),
    Column("source_url", Text),
    Column("description", Text),
    Column("created_at", DateTime, nullable=False, server_default=func.now()),
    _tuple_check("kind", SOURCE_KINDS, "kind"),
    _tuple_check("granularity", GRANULARITIES, "granularity"),
    comment="A publisher's product. Registered once; releases arrive against it.",
)


release = Table(
    "release",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("source_id", ForeignKey("source.id"), nullable=False),
    # Content-addressed. Publishers reuse filenames across vintages constantly;
    # keying on the name would let September's download silently overwrite
    # April's, after which no earlier standardisation is reproducible and
    # nothing records that it happened.
    #
    # Unique per source rather than globally: the bytes dedupe to one object in
    # the store either way, but a release is a fact about a *product*, and two
    # publishers distributing an identical file is unusual rather than illegal.
    # A global constraint would refuse the second one with an error about
    # ownership, which is a strange thing for a store to have an opinion about.
    Column("sha256", String(64), nullable=False),
    Column("object_key", Text, nullable=False),
    Column("size_bytes", Integer, nullable=False),
    Column("original_filename", Text, nullable=False),
    Column("media_type", String(128)),
    Column("source_url", Text),
    # The publisher's vintage, and the usual origin of a file_release
    # knowledge_time. Deliberately separate from retrieved_at: stamping a 2024
    # archive with a 2026 download date makes every as-of query before 2026
    # return nothing, which reads as missing data rather than as a mistake.
    Column("released_at", Date),
    Column("retrieved_at", DateTime),
    Column("ingested_at", DateTime, nullable=False, server_default=func.now()),
    Column("created_by_run_id", ForeignKey("run.id"), nullable=False),
    # Set when the bytes are pruned. The row and the hash stay as a tombstone,
    # so lineage still resolves and the file is still identifiable.
    Column("pruned_at", DateTime),
    Column("meta", JSONB),
    UniqueConstraint("source_id", "sha256"),
    Index("ix_release_source", "source_id"),
    Index("ix_release_sha256", "sha256"),
    comment="One file exactly as downloaded, before anything decided what it means.",
)


feature_group = Table(
    "feature_group",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(128), nullable=False, unique=True),
    Column("source_id", ForeignKey("source.id"), nullable=False),
    # Always the canonical namespace for a group holding mapped entities, and
    # the source's native one for the sibling group holding the entities that
    # had no canonical id. The second is not joinable to the first without a
    # crosswalk, which is the namespace guard doing its job.
    Column("entity_namespace", ForeignKey("entity_namespace.name"), nullable=False),
    # The unit of uniform knowledge-time semantics: every sample in a group
    # shares one basis and one lag. A revised indicator and an unrevised one
    # belong to different groups even when they arrive in the same file.
    Column("time_basis", String(32), nullable=False),
    # ISO-8601, e.g. 'P18M'. Promoted out of a JSON blob because it is read at
    # query time by the COALESCE that derives knowledge_time, so it has to be
    # typed and constrained rather than fished out.
    Column("declared_lag", String(16)),
    Column("period_anchor", String(8), nullable=False, server_default="end"),
    # Whether this publisher's nulls withdraw a value or merely mean "not
    # reported this cycle". Opposite meanings that the format cannot tell
    # apart, so the group declares which convention applies. Resolved at read,
    # so changing the ruling is a metadata update rather than a rebuild.
    Column("retraction_policy", String(8), nullable=False, server_default="honour"),
    Column("storage_prefix", Text, nullable=False),
    Column("partition_spec", JSONB, nullable=False, server_default='{"by": []}'),
    Column("meta", JSONB),
    Column("created_by_run_id", ForeignKey("run.id"), nullable=False),
    Column("created_at", DateTime, nullable=False, server_default=func.now()),
    _tuple_check("time_basis", TIME_BASES, "time_basis"),
    CheckConstraint("period_anchor IN ('start', 'end')", name="period_anchor"),
    CheckConstraint("retraction_policy IN ('honour', 'ignore')", name="retraction_policy"),
    # A lag is meaningful for exactly one basis. Declared anywhere else it would
    # be silently ignored; missing here there would be nothing to derive.
    CheckConstraint(
        "(time_basis = 'modeled_lag') = (declared_lag IS NOT NULL)",
        name="lag_iff_modeled",
    ),
    comment="Samples sharing uniform knowledge-time semantics.",
)


feature = Table(
    "feature",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("feature_group_id", ForeignKey("feature_group.id"), nullable=False),
    Column("name", String(255), nullable=False),
    # 'num' or 'str', and one name is one dtype forever. A name that arrives
    # numeric in one release and as a code in the next is rejected at
    # standardise, because the alternative is every consumer branching on which
    # value column happens to be populated.
    Column("dtype", String(8), nullable=False),
    Column("unit", Text),
    Column("description", Text),
    Column("created_by_run_id", ForeignKey("run.id"), nullable=False),
    Column("deprecated_at", DateTime),
    UniqueConstraint("feature_group_id", "name"),
    CheckConstraint("dtype IN ('num', 'str')", name="dtype"),
    comment="Vocabulary registry: one row per feature name. A cache of what the parquet holds.",
)


feature_partition = Table(
    "feature_partition",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("feature_group_id", ForeignKey("feature_group.id"), nullable=False),
    Column("object_key", Text, nullable=False, unique=True),
    Column("content_sha256", String(64), nullable=False),
    Column("size_bytes", Integer, nullable=False),
    Column("row_count", Integer, nullable=False),
    Column("partition_values", JSONB, nullable=False, server_default="{}"),
    Column("min_event_time", Date),
    Column("max_event_time", Date),
    # Null for a modeled_lag group, where knowledge_time is not materialised.
    Column("max_knowledge_time", DateTime),
    Column("ingest_time", DateTime, nullable=False),
    Column("created_by_run_id", ForeignKey("run.id"), nullable=False),
    # A correction supersedes rather than deletes, so old pins keep resolving.
    Column("superseded_by_run_id", ForeignKey("run.id")),
    Index("ix_feature_partition_group", "feature_group_id", "ingest_time"),
    comment=(
        "One row per physical parquet object. Not an index for convenience: a reader that "
        "globs the prefix also reads the orphans a failed run leaves behind, so the file "
        "list comes from here instead."
    ),
)


entity_list = Table(
    "entity_list",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(128), nullable=False, unique=True),
    Column("entity_namespace", ForeignKey("entity_namespace.name"), nullable=False),
    Column("description", Text),
    Column("created_at", DateTime, nullable=False, server_default=func.now()),
    comment=(
        "A named set of entity ids, for reuse across composition specs. A bounded exception "
        "to holding no entity knowledge: a list of ids is a selection, not knowledge. No "
        "aliases, no hierarchy, no succession, no attributes."
    ),
)


entity_list_version = Table(
    "entity_list_version",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("entity_list_id", ForeignKey("entity_list.id"), nullable=False),
    Column("version_num", Integer, nullable=False),
    Column("frozen_at", DateTime, nullable=False, server_default=func.now()),
    Column("created_by_run_id", ForeignKey("run.id"), nullable=False),
    UniqueConstraint("entity_list_id", "version_num"),
    comment="Pinned by specs, so redefining a list never rewrites what an old composition meant.",
)


entity_list_member = Table(
    "entity_list_member",
    metadata,
    Column("entity_list_version_id", ForeignKey("entity_list_version.id"), primary_key=True),
    Column("entity_id", String(64), primary_key=True),
)


dataset = Table(
    "dataset",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(128), nullable=False, unique=True),
    Column("kind", String(16), nullable=False, server_default="composed"),
    Column("description", Text),
    Column("created_at", DateTime, nullable=False, server_default=func.now()),
    _tuple_check("kind", DATASET_KINDS, "kind"),
    comment="A named collection of samples, produced by composition.",
)


dataset_version = Table(
    "dataset_version",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("dataset_id", ForeignKey("dataset.id"), nullable=False),
    Column("version_num", Integer, nullable=False),
    Column("resolution_mode", String(16), nullable=False),
    Column("spec", JSONB, nullable=False),
    Column("content_hash", String(64), nullable=False),
    # A pinned as-of cutoff alone does NOT make a version reproducible: a fact
    # ingested later, whose knowledge_time is before the cutoff, changes the
    # answer - which is exactly what a bitemporal store exists to represent. So
    # the other time axis is pinned too, and reads warn when it withholds.
    Column("ingest_watermark", DateTime, nullable=False),
    Column("object_prefix", Text),
    Column("frozen_at", DateTime, nullable=False, server_default=func.now()),
    Column("created_by_run_id", ForeignKey("run.id"), nullable=False),
    UniqueConstraint("dataset_id", "version_num"),
    _tuple_check("resolution_mode", RESOLUTION_MODES, "resolution_mode"),
    CheckConstraint(
        "(resolution_mode = 'materialized') <= (object_prefix IS NOT NULL)",
        name="materialized_has_bytes",
    ),
    comment="A frozen collection of samples. Immutable; corrections make new versions.",
)


run = Table(
    "run",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("kind", String(32), nullable=False),
    Column("name", Text, nullable=False),
    Column("config_hash", String(64), nullable=False),
    Column("code_git_sha", String(40)),
    Column("code_dirty", Integer),
    # The *effective* config, after overrides and with defaults materialised -
    # which is also what config_hash is over. Hashing the file on disk would
    # make two runs with different overrides look identical.
    Column("params", JSONB, nullable=False),
    Column("env", JSONB, nullable=False),
    Column("status", String(16), nullable=False, server_default="running"),
    Column("manifest_key", Text),
    # The high-water mark this run's outputs reached. Invariant 1 compares it
    # against the same column on the runs that produced its inputs.
    Column("max_knowledge_time", DateTime),
    Column("started_at", DateTime, nullable=False, server_default=func.now()),
    Column("finished_at", DateTime),
    _tuple_check("kind", RUN_KINDS, "kind"),
    _tuple_check("status", RUN_STATUSES, "status"),
    Index("ix_run_kind_started", "kind", "started_at"),
    comment="Every write happens inside one. Lineage is not optional.",
)


def _lineage_table(name: str, comment: str) -> Table:
    """``run_input`` and ``run_output`` are the same shape and stay that way.

    ``ref_type`` is a checked string rather than a polymorphic foreign key.
    Four target tables through one column is where the ORM stops helping and a
    ``verify`` pass starts doing more good than a constraint would.
    """
    return Table(
        name,
        metadata,
        Column("run_id", ForeignKey("run.id"), primary_key=True),
        Column("ref_type", String(32), primary_key=True),
        Column("ref_id", Integer, primary_key=True),
        Column("role", String(64), primary_key=True),
        _tuple_check("ref_type", REF_TYPES, "ref_type"),
        comment=comment,
    )


run_input = _lineage_table("run_input", "What a run read.")
run_output = _lineage_table("run_output", "What a run produced.")
