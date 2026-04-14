"""Tests for SqlDatabaseLayer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

import pytest

from celerity.resources.sql_database.layer import SqlDatabaseLayer


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


class FakeConfigService:
    def __init__(self, namespaces: dict[str, FakeConfigNamespace]) -> None:
        self._namespaces = namespaces

    def namespace(self, name: str) -> FakeConfigNamespace:
        return self._namespaces[name]


class FakeContainer:
    def __init__(self) -> None:
        self._registry: dict[str, object] = {}

    def register_value(self, token: str, value: object) -> None:
        self._registry[token] = value

    async def resolve(self, token: str) -> object:
        return self._registry[token]


class FakeContext:
    def __init__(self, container: FakeContainer) -> None:
        self.container = container


@pytest.fixture
def container() -> FakeContainer:
    return FakeContainer()


@pytest.fixture
def single_sql_env(
    resource_links_file: Callable[[dict[str, Any]], Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resource_links_file({"orders-db": {"type": "sqlDatabase", "configKey": "ordersDb"}})
    monkeypatch.delenv("CELERITY_RUNTIME", raising=False)


@pytest.fixture
def multi_sql_env(
    resource_links_file: Callable[[dict[str, Any]], Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resource_links_file(
        {
            "orders-db": {"type": "sqlDatabase", "configKey": "ordersDb"},
            "users-db": {"type": "sqlDatabase", "configKey": "usersDb"},
        }
    )
    monkeypatch.delenv("CELERITY_RUNTIME", raising=False)


@pytest.fixture
def no_sql_env(resource_links_file: Callable[[dict[str, Any]], Path]) -> None:
    resource_links_file({"app-cache": {"type": "cache", "configKey": "appCache"}})


class TestSqlDatabaseLayerNoLinks:
    @pytest.mark.asyncio
    async def test_no_op(self, container: FakeContainer, no_sql_env: None) -> None:
        layer = SqlDatabaseLayer()
        next_handler = AsyncMock(return_value="result")
        ctx = FakeContext(container)

        result = await layer.handle(ctx, next_handler)

        assert result == "result"
        next_handler.assert_awaited_once()
        assert layer._initialized is True


class TestSqlDatabaseLayerSingleResource:
    @pytest.mark.asyncio
    @patch("celerity.resources.sql_database.layer.create_sql_database")
    @patch("celerity.resources.sql_database.layer.resolve_database_credentials")
    async def test_registers_tokens(
        self,
        mock_resolve_creds: AsyncMock,
        mock_create: MagicMock,
        container: FakeContainer,
        single_sql_env: None,
    ) -> None:
        config_ns = FakeConfigNamespace(
            {
                "ordersDb_host": "localhost",
                "ordersDb_database": "orders",
                "ordersDb_user": "admin",
                "ordersDb_password": "secret",
            }
        )
        config_service = FakeConfigService({"resources": config_ns})
        container.register_value("ConfigService", config_service)

        mock_info = MagicMock()
        mock_auth = MagicMock()
        mock_resolve_creds.return_value = (mock_info, mock_auth)

        mock_writer = MagicMock()
        mock_reader = MagicMock()
        mock_instance = MagicMock()
        mock_instance.writer.return_value = mock_writer
        mock_instance.reader.return_value = mock_reader
        mock_create.return_value = mock_instance

        layer = SqlDatabaseLayer()
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(FakeContext(container), next_handler)

        # Per-resource tokens
        assert await container.resolve("celerity:sqlDatabase:instance:orders-db") is mock_instance
        assert await container.resolve("celerity:sqlDatabase:writer:orders-db") is mock_writer
        assert await container.resolve("celerity:sqlDatabase:reader:orders-db") is mock_reader
        assert "celerity:sqlDatabase:credentials:orders-db" in container._registry

        # Default tokens (single resource)
        assert await container.resolve("celerity:sqlDatabase:writer:default") is mock_writer
        assert await container.resolve("celerity:sqlDatabase:reader:default") is mock_reader
        assert "celerity:sqlDatabase:credentials:default" in container._registry

    @pytest.mark.asyncio
    @patch("celerity.resources.sql_database.layer.create_sql_database")
    @patch("celerity.resources.sql_database.layer.resolve_database_credentials")
    async def test_idempotent(
        self,
        mock_resolve_creds: AsyncMock,
        mock_create: MagicMock,
        container: FakeContainer,
        single_sql_env: None,
    ) -> None:
        config_ns = FakeConfigNamespace(
            {
                "ordersDb_host": "localhost",
                "ordersDb_database": "orders",
                "ordersDb_user": "admin",
                "ordersDb_password": "secret",
            }
        )
        config_service = FakeConfigService({"resources": config_ns})
        container.register_value("ConfigService", config_service)

        mock_resolve_creds.return_value = (MagicMock(), MagicMock())
        mock_instance = MagicMock()
        mock_instance.writer.return_value = MagicMock()
        mock_instance.reader.return_value = MagicMock()
        mock_create.return_value = mock_instance

        layer = SqlDatabaseLayer()
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(FakeContext(container), next_handler)
        await layer.handle(FakeContext(container), next_handler)

        mock_create.assert_called_once()


class TestSqlDatabaseLayerMultipleResources:
    @pytest.mark.asyncio
    @patch("celerity.resources.sql_database.layer.create_sql_database")
    @patch("celerity.resources.sql_database.layer.resolve_database_credentials")
    async def test_no_default_token(
        self,
        mock_resolve_creds: AsyncMock,
        mock_create: MagicMock,
        container: FakeContainer,
        multi_sql_env: None,
    ) -> None:
        config_ns = FakeConfigNamespace(
            {
                "ordersDb_host": "localhost",
                "ordersDb_database": "orders",
                "ordersDb_user": "admin",
                "ordersDb_password": "secret",
                "usersDb_host": "localhost",
                "usersDb_database": "users",
                "usersDb_user": "admin",
                "usersDb_password": "secret",
            }
        )
        config_service = FakeConfigService({"resources": config_ns})
        container.register_value("ConfigService", config_service)

        mock_resolve_creds.return_value = (MagicMock(), MagicMock())
        mock_instance = MagicMock()
        mock_instance.writer.return_value = MagicMock()
        mock_instance.reader.return_value = MagicMock()
        mock_create.return_value = mock_instance

        layer = SqlDatabaseLayer()
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(FakeContext(container), next_handler)

        # Named tokens registered
        assert "celerity:sqlDatabase:writer:orders-db" in container._registry
        assert "celerity:sqlDatabase:writer:users-db" in container._registry
        # No default token
        assert "celerity:sqlDatabase:writer:default" not in container._registry
        assert "celerity:sqlDatabase:reader:default" not in container._registry
        assert "celerity:sqlDatabase:credentials:default" not in container._registry


class TestSqlDatabaseLayerDispose:
    @pytest.mark.asyncio
    async def test_dispose_closes_instances(self) -> None:
        layer = SqlDatabaseLayer()
        mock_instance1 = AsyncMock()
        mock_instance2 = AsyncMock()
        layer._instances = [mock_instance1, mock_instance2]

        await layer.dispose()

        mock_instance1.close.assert_awaited_once()
        mock_instance2.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dispose_no_instances(self) -> None:
        layer = SqlDatabaseLayer()
        await layer.dispose()  # Should not raise
