"""Tests for SQL database types."""

from __future__ import annotations

from unittest.mock import AsyncMock

from celerity.resources.sql_database.config import PoolConfig
from celerity.resources.sql_database.errors import SqlDatabaseError
from celerity.resources.sql_database.types import (
    SqlConnectionInfo,
    SqlIamAuth,
    SqlPasswordAuth,
)


class TestSqlDatabaseError:
    def test_message(self) -> None:
        err = SqlDatabaseError("connection failed")
        assert str(err) == "connection failed"

    def test_resource(self) -> None:
        err = SqlDatabaseError("fail", resource="orders-db")
        assert err.resource == "orders-db"

    def test_resource_default_none(self) -> None:
        err = SqlDatabaseError("fail")
        assert err.resource is None

    def test_cause_chaining(self) -> None:
        cause = RuntimeError("underlying")
        err = SqlDatabaseError("wrapped", cause=cause)
        assert err.__cause__ is cause

    def test_cause_default_none(self) -> None:
        err = SqlDatabaseError("fail")
        assert err.__cause__ is None


class TestSqlConnectionInfo:
    def test_all_fields(self) -> None:
        info = SqlConnectionInfo(
            host="db.example.com",
            port=5432,
            database="mydb",
            user="admin",
            engine="postgres",
            ssl=True,
            auth_mode="password",
            read_host="replica.example.com",
        )
        assert info.host == "db.example.com"
        assert info.port == 5432
        assert info.database == "mydb"
        assert info.user == "admin"
        assert info.engine == "postgres"
        assert info.ssl is True
        assert info.auth_mode == "password"
        assert info.read_host == "replica.example.com"

    def test_read_host_default_none(self) -> None:
        info = SqlConnectionInfo(
            host="h",
            port=5432,
            database="d",
            user="u",
            engine="postgres",
            ssl=True,
            auth_mode="password",
        )
        assert info.read_host is None


class TestSqlPasswordAuth:
    def test_fields(self) -> None:
        auth = SqlPasswordAuth(
            password="secret",
            url="postgresql+asyncpg://u:secret@h:5432/d",
            read_url="postgresql+asyncpg://u:secret@r:5432/d",
        )
        assert auth.password == "secret"
        assert auth.url == "postgresql+asyncpg://u:secret@h:5432/d"
        assert auth.read_url == "postgresql+asyncpg://u:secret@r:5432/d"

    def test_read_url_default_none(self) -> None:
        auth = SqlPasswordAuth(password="s", url="u")
        assert auth.read_url is None


class TestSqlIamAuth:
    def test_fields(self) -> None:
        provider = AsyncMock()
        auth = SqlIamAuth(
            token_provider=provider,
            url="postgresql+asyncpg://u:tok@h:5432/d",
            read_url="postgresql+asyncpg://u:tok@r:5432/d",
        )
        assert auth.token_provider is provider
        assert auth.url == "postgresql+asyncpg://u:tok@h:5432/d"
        assert auth.read_url == "postgresql+asyncpg://u:tok@r:5432/d"

    def test_read_url_default_none(self) -> None:
        provider = AsyncMock()
        auth = SqlIamAuth(token_provider=provider, url="u")
        assert auth.read_url is None


class TestPoolConfig:
    def test_all_fields(self) -> None:
        pc = PoolConfig(
            pool_min_size=2,
            pool_max_size=10,
            idle_timeout_seconds=30,
            acquire_timeout_seconds=10,
        )
        assert pc.pool_min_size == 2
        assert pc.pool_max_size == 10
        assert pc.idle_timeout_seconds == 30
        assert pc.acquire_timeout_seconds == 10
