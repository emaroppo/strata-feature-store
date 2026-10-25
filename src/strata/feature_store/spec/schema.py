"""The long-format schema: every feature parquet has these seven columns and no others.

Dtypes are named, not constructed, so this module stays free of ``pyarrow``.
See ``docs/adr/0002``.
"""

from typing import Final

#: Column order is fixed, so a bare parquet read without the catalog still lines up.
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

#: Logical types, resolved to physical ones by the store; ``knowledge_time`` is
#: nullable for ``modeled_lag`` groups, see ``docs/adr/0003``.
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

#: Dictionary-encoded on write: both repeat heavily within a file.
DICTIONARY_ENCODED: Final[frozenset[str]] = frozenset({ENTITY_ID, FEATURE_NAME})

#: Sort order before writing; ``feature_name`` first keeps row-group statistics tight.
SORT_ORDER: Final[tuple[str, ...]] = (FEATURE_NAME, ENTITY_ID, EVENT_TIME)

#: A fact's identity for diffing releases; excludes ``knowledge_time``, see ``docs/adr/0004``.
FACT_KEY: Final[tuple[str, ...]] = (ENTITY_ID, EVENT_TIME, FEATURE_NAME)

#: Bumped when the meaning of a column changes.
SCHEMA_VERSION: Final = 1

#: Prefix for parquet key-value metadata keys, so they cannot collide with a writer's own.
META_PREFIX: Final = "strata.feature_store."


def value_column(dtype: str) -> str:
    """Which value column a feature of *dtype* is stored in; refuses all but ``num`` and ``str``."""
    if dtype == "num":
        return VALUE_NUM
    if dtype == "str":
        return VALUE_STR
    raise ValueError(f"dtype must be 'num' or 'str', not {dtype!r}")
