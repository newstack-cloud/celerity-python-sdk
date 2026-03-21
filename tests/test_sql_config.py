"""Tests for SQL database pool configuration."""

from __future__ import annotations

from celerity.resources.sql_database.config import (
    FUNCTIONS_POOL,
    RUNTIME_POOL,
    PoolConfig,
    build_connection_url,
    resolve_pool_config,
    resolve_pool_overrides,
)


class TestPoolPresets:
    def test_functions_pool(self) -> None:
        assert FUNCTIONS_POOL.pool_min_size == 0
        assert FUNCTIONS_POOL.pool_max_size == 2
        assert FUNCTIONS_POOL.idle_timeout_seconds == 1
        assert FUNCTIONS_POOL.acquire_timeout_seconds == 10

    def test_runtime_pool(self) -> None:
        assert RUNTIME_POOL.pool_min_size == 2
        assert RUNTIME_POOL.pool_max_size == 10
        assert RUNTIME_POOL.idle_timeout_seconds == 30
        assert RUNTIME_POOL.acquire_timeout_seconds == 10


class TestResolvePoolOverrides:
    def test_extracts_min_size(self) -> None:
        result = resolve_pool_overrides("db", {"db_poolMinSize": "5"})
        assert result["pool_min_size"] == 5

    def test_extracts_max_size(self) -> None:
        result = resolve_pool_overrides("db", {"db_poolMaxSize": "20"})
        assert result["pool_max_size"] == 20

    def test_extracts_idle_timeout(self) -> None:
        result = resolve_pool_overrides("db", {"db_poolIdleTimeout": "60"})
        assert result["idle_timeout_seconds"] == 60.0

    def test_extracts_acquire_timeout(self) -> None:
        result = resolve_pool_overrides("db", {"db_poolAcquireTimeout": "15"})
        assert result["acquire_timeout_seconds"] == 15.0

    def test_empty_when_no_matching_keys(self) -> None:
        result = resolve_pool_overrides("db", {"other_key": "value"})
        assert result == {}


class TestResolvePoolConfig:
    def test_functions_preset(self) -> None:
        config = resolve_pool_config("functions")
        assert config is FUNCTIONS_POOL

    def test_runtime_preset(self) -> None:
        config = resolve_pool_config("runtime")
        assert config is RUNTIME_POOL

    def test_explicit_takes_precedence(self) -> None:
        explicit = PoolConfig(
            pool_min_size=1,
            pool_max_size=5,
            idle_timeout_seconds=10,
            acquire_timeout_seconds=5,
        )
        config = resolve_pool_config("functions", explicit=explicit)
        assert config is explicit

    def test_overrides_applied(self) -> None:
        config = resolve_pool_config(
            "functions",
            config_key="db",
            overrides={"db_poolMaxSize": "20"},
        )
        assert config.pool_max_size == 20
        assert config.pool_min_size == FUNCTIONS_POOL.pool_min_size

    def test_no_config_key_returns_preset(self) -> None:
        config = resolve_pool_config(
            "functions",
            overrides={"db_poolMaxSize": "20"},
        )
        assert config is FUNCTIONS_POOL

    def test_no_overrides_returns_preset(self) -> None:
        config = resolve_pool_config("functions", config_key="db")
        assert config is FUNCTIONS_POOL


class TestBuildConnectionUrl:
    def test_postgres_with_ssl(self) -> None:
        url = build_connection_url(
            "postgres",
            "user",
            "pass",
            "host",
            5432,
            "mydb",
            ssl=True,
        )
        assert url == "postgresql+asyncpg://user:pass@host:5432/mydb?ssl=require"

    def test_postgres_without_ssl(self) -> None:
        url = build_connection_url(
            "postgres",
            "user",
            "pass",
            "host",
            5432,
            "mydb",
            ssl=False,
        )
        assert url == "postgresql+asyncpg://user:pass@host:5432/mydb"

    def test_mysql_with_ssl(self) -> None:
        url = build_connection_url(
            "mysql",
            "user",
            "pass",
            "host",
            3306,
            "mydb",
            ssl=True,
        )
        assert url == "mysql+aiomysql://user:pass@host:3306/mydb?ssl=true"

    def test_mysql_without_ssl(self) -> None:
        url = build_connection_url(
            "mysql",
            "user",
            "pass",
            "host",
            3306,
            "mydb",
            ssl=False,
        )
        assert url == "mysql+aiomysql://user:pass@host:3306/mydb"

    def test_url_encodes_special_password(self) -> None:
        url = build_connection_url(
            "postgres",
            "user",
            "p@ss:w/rd%",
            "host",
            5432,
            "mydb",
            ssl=False,
        )
        assert "p%40ss%3Aw%2Frd%25" in url

    def test_invalid_engine_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Unsupported engine"):
            build_connection_url(
                "oracle",
                "user",
                "pass",
                "host",
                1521,
                "mydb",
                ssl=False,
            )
