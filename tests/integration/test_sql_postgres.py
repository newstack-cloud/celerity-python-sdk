"""Integration tests for SQL database resource against PostgreSQL.

Requires Docker PostgreSQL on port 5499 (see docker-compose.yml).
Run via: ./scripts/run-tests.sh integration
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from celerity.resources.sql_database.config import (
    PoolConfig,
    build_connection_url,
)
from celerity.resources.sql_database.credentials import resolve_database_credentials
from celerity.resources.sql_database.factory import create_sql_database
from celerity.resources.sql_database.types import (
    SqlConnectionInfo,
    SqlPasswordAuth,
)
from tests.integration.conftest import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

pytestmark = pytest.mark.integration


def _connection_url() -> str:
    return build_connection_url(
        "postgres",
        POSTGRES_USER,
        POSTGRES_PASSWORD,
        POSTGRES_HOST,
        POSTGRES_PORT,
        POSTGRES_DB,
        ssl=False,
    )


@pytest.fixture(scope="module")
async def setup_database() -> AsyncGenerator[None]:
    """Create test_items table and seed data."""
    engine = create_async_engine(_connection_url())
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS test_items"))
        await conn.execute(
            text(
                """
                CREATE TABLE test_items (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    value NUMERIC NOT NULL,
                    category VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """
            )
        )
        await conn.execute(
            text(
                """
                INSERT INTO test_items (name, value, category) VALUES
                ('item-a', 10.0, 'electronics'),
                ('item-b', 20.5, 'electronics'),
                ('item-c', 5.0, 'books'),
                ('item-d', 15.0, 'books'),
                ('item-e', 30.0, 'clothing')
            """
            )
        )
    await engine.dispose()
    yield
    engine = create_async_engine(_connection_url())
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS test_items"))
    await engine.dispose()


class FakeConfigNamespace:
    def __init__(self, data: dict[str, str]) -> None:
        self._data = data

    async def get(self, key: str) -> str | None:
        return self._data.get(key)

    async def get_or_throw(self, key: str) -> str:
        val = self._data.get(key)
        if val is None:
            raise KeyError(key)
        return val

    async def get_all(self) -> dict[str, str]:
        return dict(self._data)


class TestCredentials:
    @pytest.mark.asyncio
    async def test_resolve_password_credentials(self, setup_database: None) -> None:
        ns = FakeConfigNamespace(
            {
                "pg_host": POSTGRES_HOST,
                "pg_port": str(POSTGRES_PORT),
                "pg_database": POSTGRES_DB,
                "pg_user": POSTGRES_USER,
                "pg_password": POSTGRES_PASSWORD,
                "pg_ssl": "false",
            }
        )

        info, auth = await resolve_database_credentials(ns, "pg")  # type: ignore[arg-type]

        assert info.host == POSTGRES_HOST
        assert info.port == POSTGRES_PORT
        assert info.database == POSTGRES_DB
        assert isinstance(auth, SqlPasswordAuth)
        assert "postgresql+asyncpg://" in auth.url


class TestEngineCreation:
    @pytest.mark.asyncio
    async def test_select_one(self, setup_database: None) -> None:
        engine = create_async_engine(_connection_url())
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT 1"))
                assert result.scalar() == 1
        finally:
            await engine.dispose()


class TestSelect:
    @pytest.mark.asyncio
    async def test_query_all_rows(self, setup_database: None) -> None:
        engine = create_async_engine(_connection_url())
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT * FROM test_items"))
                rows = result.fetchall()
                assert len(rows) == 5
        finally:
            await engine.dispose()


class TestFilter:
    @pytest.mark.asyncio
    async def test_where_clause(self, setup_database: None) -> None:
        engine = create_async_engine(_connection_url())
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT * FROM test_items WHERE category = :cat"),
                    {"cat": "electronics"},
                )
                rows = result.fetchall()
                assert len(rows) == 2
        finally:
            await engine.dispose()


class TestInsertRetrieveDelete:
    @pytest.mark.asyncio
    async def test_crud(self, setup_database: None) -> None:
        engine = create_async_engine(_connection_url())
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("INSERT INTO test_items (name, value, category) VALUES (:n, :v, :c)"),
                    {"n": "temp-item", "v": 99.0, "c": "temp"},
                )

            async with engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT * FROM test_items WHERE name = :n"),
                    {"n": "temp-item"},
                )
                row = result.fetchone()
                assert row is not None
                assert row.name == "temp-item"

            async with engine.begin() as conn:
                await conn.execute(
                    text("DELETE FROM test_items WHERE name = :n"),
                    {"n": "temp-item"},
                )

            async with engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT * FROM test_items WHERE name = :n"),
                    {"n": "temp-item"},
                )
                assert result.fetchone() is None
        finally:
            await engine.dispose()


class TestUpdate:
    @pytest.mark.asyncio
    async def test_update_value(self, setup_database: None) -> None:
        engine = create_async_engine(_connection_url())
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("UPDATE test_items SET value = :v WHERE name = :n"),
                    {"v": 999.0, "n": "item-a"},
                )

            async with engine.connect() as conn:
                result = await conn.execute(
                    text("SELECT value FROM test_items WHERE name = :n"),
                    {"n": "item-a"},
                )
                row = result.fetchone()
                assert row is not None
                assert float(row.value) == 999.0

            # Restore original value
            async with engine.begin() as conn:
                await conn.execute(
                    text("UPDATE test_items SET value = :v WHERE name = :n"),
                    {"v": 10.0, "n": "item-a"},
                )
        finally:
            await engine.dispose()


class TestAggregate:
    @pytest.mark.asyncio
    async def test_sum_by_category(self, setup_database: None) -> None:
        engine = create_async_engine(_connection_url())
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        """
                        SELECT category, SUM(value) as total
                        FROM test_items
                        GROUP BY category
                        ORDER BY category
                    """
                    )
                )
                rows = result.fetchall()
                totals = {row.category: float(row.total) for row in rows}
                assert totals["books"] == 20.0
                assert totals["electronics"] == 30.5
                assert totals["clothing"] == 30.0
        finally:
            await engine.dispose()


class TestSqlDatabaseInstance:
    @pytest.mark.asyncio
    async def test_writer_and_reader(self, setup_database: None) -> None:
        info = SqlConnectionInfo(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            engine="postgres",
            ssl=False,
            auth_mode="password",
        )
        auth = SqlPasswordAuth(
            password=POSTGRES_PASSWORD,
            url=_connection_url(),
        )
        pool = PoolConfig(
            pool_min_size=0,
            pool_max_size=2,
            idle_timeout_seconds=1,
            acquire_timeout_seconds=5,
        )

        instance = create_sql_database(info, auth, pool)
        try:
            # Writer and reader should be the same engine (no read replica)
            assert instance.writer() is instance.reader()

            async with instance.writer().connect() as conn:
                result = await conn.execute(text("SELECT COUNT(*) FROM test_items"))
                assert result.scalar() == 5

            async with instance.reader().connect() as conn:
                result = await conn.execute(text("SELECT 1"))
                assert result.scalar() == 1
        finally:
            await instance.close()

    @pytest.mark.asyncio
    async def test_close_disposes_pools(self, setup_database: None) -> None:
        info = SqlConnectionInfo(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            engine="postgres",
            ssl=False,
            auth_mode="password",
        )
        auth = SqlPasswordAuth(
            password=POSTGRES_PASSWORD,
            url=_connection_url(),
        )
        pool = PoolConfig(
            pool_min_size=0,
            pool_max_size=2,
            idle_timeout_seconds=1,
            acquire_timeout_seconds=5,
        )

        instance = create_sql_database(info, auth, pool)

        # Verify engine works
        async with instance.writer().connect() as conn:
            await conn.execute(text("SELECT 1"))

        await instance.close()

        # After dispose, checked-out connections should be 0
        pool_status = instance.writer().pool.status()
        assert "Current Checked out connections: 0" in pool_status
