"""Tests for CacheLayer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

import pytest

from celerity.resources.cache.layer import CacheLayer


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
def single_cache_env(
    resource_links_file: Callable[[dict[str, Any]], Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resource_links_file({"app-cache": {"type": "cache", "configKey": "appCache"}})
    monkeypatch.delenv("CELERITY_RUNTIME", raising=False)


@pytest.fixture
def multi_cache_env(
    resource_links_file: Callable[[dict[str, Any]], Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resource_links_file(
        {
            "session": {"type": "cache", "configKey": "sessionCache"},
            "data": {"type": "cache", "configKey": "dataCache"},
        }
    )
    monkeypatch.delenv("CELERITY_RUNTIME", raising=False)


@pytest.fixture
def no_cache_env(resource_links_file: Callable[[dict[str, Any]], Path]) -> None:
    resource_links_file({"orders-db": {"type": "datastore", "configKey": "ordersDb"}})


class TestCacheLayerNoCacheLinks:
    @pytest.mark.asyncio
    async def test_no_op(self, container: FakeContainer, no_cache_env: None) -> None:
        layer = CacheLayer()
        next_handler = AsyncMock(return_value="result")
        ctx = FakeContext(container)

        result = await layer.handle(ctx, next_handler)

        assert result == "result"
        next_handler.assert_awaited_once()
        assert layer._initialized is True


class TestCacheLayerSingleResource:
    @pytest.mark.asyncio
    @patch("celerity.resources.cache.layer.create_redis_cache_client")
    async def test_registers_tokens(
        self,
        mock_create: AsyncMock,
        container: FakeContainer,
        single_cache_env: None,
    ) -> None:
        # Set up config
        config_ns = FakeConfigNamespace(
            {
                "appCache_host": "localhost",
                "appCache_port": "6379",
                "appCache": "my-cache",
            }
        )
        config_service = FakeConfigService({"resources": config_ns})
        container.register_value("ConfigService", config_service)

        # Mock Redis client
        mock_cache_handle = MagicMock()
        mock_client = MagicMock()
        mock_client.cache.return_value = mock_cache_handle
        mock_create.return_value = mock_client

        layer = CacheLayer()
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(FakeContext(container), next_handler)

        # Per-resource token registered
        assert await container.resolve("celerity:cache:app-cache") is mock_cache_handle
        # Default token registered (single resource)
        assert await container.resolve("celerity:cache:default") is mock_cache_handle

    @pytest.mark.asyncio
    @patch("celerity.resources.cache.layer.create_redis_cache_client")
    async def test_idempotent(
        self,
        mock_create: AsyncMock,
        container: FakeContainer,
        single_cache_env: None,
    ) -> None:
        config_ns = FakeConfigNamespace({"appCache_host": "localhost", "appCache": "c"})
        config_service = FakeConfigService({"resources": config_ns})
        container.register_value("ConfigService", config_service)

        mock_client = MagicMock()
        mock_client.cache.return_value = MagicMock()
        mock_create.return_value = mock_client

        layer = CacheLayer()
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(FakeContext(container), next_handler)
        await layer.handle(FakeContext(container), next_handler)

        # Client only created once
        mock_create.assert_awaited_once()


class TestCacheLayerMultipleResources:
    @pytest.mark.asyncio
    @patch("celerity.resources.cache.layer.create_redis_cache_client")
    async def test_no_default_token(
        self,
        mock_create: AsyncMock,
        container: FakeContainer,
        multi_cache_env: None,
    ) -> None:
        config_ns = FakeConfigNamespace(
            {
                "sessionCache_host": "localhost",
                "sessionCache": "session-c",
                "dataCache_host": "localhost",
                "dataCache": "data-c",
            }
        )
        config_service = FakeConfigService({"resources": config_ns})
        container.register_value("ConfigService", config_service)

        mock_client = MagicMock()
        mock_client.cache.return_value = MagicMock()
        mock_create.return_value = mock_client

        layer = CacheLayer()
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(FakeContext(container), next_handler)

        # Both named tokens registered
        assert "celerity:cache:session" in container._registry
        assert "celerity:cache:data" in container._registry
        # No default token
        assert "celerity:cache:default" not in container._registry


class TestCacheLayerDispose:
    @pytest.mark.asyncio
    async def test_dispose_closes_client(self) -> None:
        layer = CacheLayer()
        mock_client = AsyncMock()
        layer._client = mock_client  # type: ignore[assignment]

        await layer.dispose()
        mock_client.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dispose_no_client(self) -> None:
        layer = CacheLayer()
        await layer.dispose()  # Should not raise
