"""A real Postgres, on a scratch database, migrated the way production is.

No SQLite fallback and no mocks. The catalog leans on things SQLite does not
have — ``JSONB``, real CHECK enforcement, transactional DDL — so a suite that
passed against SQLite would be testing a different schema than the one that
ships.

The database is created and dropped per session, and migrated with alembic
rather than ``metadata.create_all``. That distinction matters: ``create_all``
would test the table definitions while leaving the migration untested, and the
migration is the artifact that actually runs against a database with data in it.
"""

from __future__ import annotations

import os
import uuid

import pytest
import sqlalchemy as sa

#: Points at the dev stack in docker-compose.yml by default. Override to run
#: against something else -- but never against an instance holding real data:
#: this creates and drops databases.
DEFAULT_ADMIN_URL = "postgresql+psycopg://feature_store:feature_store@localhost:5434/postgres"

ADMIN_URL = os.environ.get("FEATURE_STORE_TEST_ADMIN_URL", DEFAULT_ADMIN_URL)


def _admin_engine() -> sa.Engine:
    # AUTOCOMMIT because CREATE DATABASE cannot run inside a transaction.
    return sa.create_engine(ADMIN_URL, isolation_level="AUTOCOMMIT", poolclass=sa.pool.NullPool)


@pytest.fixture(scope="session")
def scratch_database_url() -> str:
    """A freshly created, empty database, dropped when the session ends."""
    try:
        engine = _admin_engine()
        with engine.connect() as connection:
            connection.execute(sa.text("select 1"))
    except Exception as exc:  # pragma: no cover - depends on the host
        pytest.skip(f"no Postgres at {ADMIN_URL}: {exc}")

    name = f"feature_store_test_{uuid.uuid4().hex[:12]}"
    with engine.connect() as connection:
        connection.execute(sa.text(f'create database "{name}"'))

    url = ADMIN_URL.rsplit("/", 1)[0] + f"/{name}"
    try:
        yield url
    finally:
        with engine.connect() as connection:
            connection.execute(
                sa.text(
                    "select pg_terminate_backend(pid) from pg_stat_activity "
                    "where datname = :name and pid <> pg_backend_pid()"
                ),
                {"name": name},
            )
            connection.execute(sa.text(f'drop database if exists "{name}"'))
        engine.dispose()


@pytest.fixture(scope="session")
def migrated_database_url(scratch_database_url: str) -> str:
    """The scratch database with ``alembic upgrade head`` applied."""
    from alembic import command
    from alembic.config import Config

    config = Config("alembic.ini")
    previous = os.environ.get("FEATURE_STORE_DB_URL")
    os.environ["FEATURE_STORE_DB_URL"] = scratch_database_url
    try:
        command.upgrade(config, "head")
    finally:
        if previous is None:
            del os.environ["FEATURE_STORE_DB_URL"]
        else:
            os.environ["FEATURE_STORE_DB_URL"] = previous
    return scratch_database_url


@pytest.fixture(scope="session")
def engine(migrated_database_url: str) -> sa.Engine:
    made = sa.create_engine(migrated_database_url, poolclass=sa.pool.NullPool)
    yield made
    made.dispose()


@pytest.fixture
def connection(engine: sa.Engine):
    """A connection whose work is rolled back when the test ends.

    Every test therefore starts from the same empty catalog without paying for a
    migration, and a test that leaves a mess cannot affect the next one.
    """
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            yield conn
        finally:
            transaction.rollback()
