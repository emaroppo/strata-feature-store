"""Alembic environment.

The database URL comes from ``config.toml``, with ``$FEATURE_STORE_DB_URL``
as the override the test suite uses. See ``strata/docs/adr/0019``.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from strata.feature_store.store import config as store_config
from strata.feature_store.store.tables import metadata

alembic_config = context.config

if alembic_config.config_file_name is not None:
    fileConfig(alembic_config.config_file_name)

target_metadata = metadata


def database_url() -> str:
    override = os.environ.get("FEATURE_STORE_DB_URL")
    if override:
        return override
    return store_config.load().catalog.with_password()


def run_migrations_offline() -> None:
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # Without this a CHECK constraint's name is Postgres's own and a downgrade cannot drop it.
        render_as_batch=False,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = alembic_config.get_section(alembic_config.config_ini_section, {})
    section["sqlalchemy.url"] = database_url()
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
