"""The long-format schema, defined once.

Every feature parquet in the store has these seven columns and no others.
Sameness is the point: combining two sources is a ``UNION ALL`` rather than a
join, adding a feature is an append rather than a migration across every
registered file, and a reader never has to ask what shape a file is in.

**Why long rather than wide.** ``knowledge_time`` attaches to a fact, not to a
row. A wide layout forces every column in a row to share one revision cadence,
and that is simply false — a GDP figure is revised for years, a population
count is not. Wide exists here only as an output orientation, produced by
pivoting at freeze time, and is never a registered storage layout.

The dtypes are named rather than constructed, so this module stays free of
``pyarrow``. ``strata.feature_store.store.parquet`` turns them into a real schema.
"""

from typing import Final

#: Column order is fixed. Files are written in this order so that a bare
#: parquet read without the catalog still lines up.
ENTITY_ID: Final = "entity_id"
EVENT_TIME: Final = "event_time"
KNOWLEDGE_TIME: Final = "knowledge_time"
INGEST_TIME: Final = "ingest_time"
FEATURE_NAME: Final = "feature_name"
VALUE_NUM: Final = "value_num"
VALUE_STR: Final = "value_str"

COLUMNS: Final[tuple[str, ...]] = (
    ENTITY_ID,
    EVENT_TIME,
    KNOWLEDGE_TIME,
    INGEST_TIME,
    FEATURE_NAME,
    VALUE_NUM,
    VALUE_STR,
)

#: Logical types, resolved to physical ones by the store. ``knowledge_time`` is
#: nullable because a ``modeled_lag`` group leaves it null on first load and
#: derives it at read; see :mod:`strata.feature_store.spec.timebasis`.
DTYPES: Final[dict[str, str]] = {
    ENTITY_ID: "string",
    EVENT_TIME: "date32",
    KNOWLEDGE_TIME: "timestamp[us]",
    INGEST_TIME: "timestamp[us]",
    FEATURE_NAME: "string",
    VALUE_NUM: "double",
    VALUE_STR: "string",
}

NULLABLE: Final[frozenset[str]] = frozenset({KNOWLEDGE_TIME, VALUE_NUM, VALUE_STR})

#: Dictionary-encoded on write. Both repeat heavily within a file — a group
#: holds a few hundred entity ids and up to a few thousand feature names across
#: millions of rows.
DICTIONARY_ENCODED: Final[frozenset[str]] = frozenset({ENTITY_ID, FEATURE_NAME})

#: Sort order applied before writing. Puts ``feature_name`` first so row-group
#: min/max statistics on it are tight: a query for three indicators then skips
#: the row groups holding the other nine hundred. Sorting by entity first would
#: spread every feature across every row group and make the statistics useless.
SORT_ORDER: Final[tuple[str, ...]] = (FEATURE_NAME, ENTITY_ID, EVENT_TIME)

#: A fact's identity, for deciding whether a release changed a value. Note it
#: excludes ``knowledge_time``: the whole point of a revision is that the value
#: for one identity now differs and gains a new knowledge_time.
FACT_KEY: Final[tuple[str, ...]] = (ENTITY_ID, EVENT_TIME, FEATURE_NAME)

#: Bumped when the meaning of a column changes. Written into every parquet's
#: key-value metadata, so a file found without a catalog says how to read it.
SCHEMA_VERSION: Final = 1

#: Parquet key-value metadata keys. Prefixed so they cannot collide with a
#: writer's own.
META_PREFIX: Final = "strata.feature_store."


def value_column(dtype: str) -> str:
    """Which value column a feature of *dtype* is stored in.

    One feature name is one dtype forever. A name that arrives numeric in one
    release and as a code in the next is rejected at standardise rather than
    discovered at read, because the alternative is every consumer branching on
    which column happens to be populated.
    """
    if dtype == "num":
        return VALUE_NUM
    if dtype == "str":
        return VALUE_STR
    raise ValueError(f"dtype must be 'num' or 'str', not {dtype!r}")
