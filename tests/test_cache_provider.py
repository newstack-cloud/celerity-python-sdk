"""Unit tests for RedisCache — covers behaviour not reachable via integration tests.

Happy-path CRUD is exercised by integration tests against real Valkey.
This file focuses on: key-prefix wiring, cluster-mode slot validation,
cluster fan-out, edge cases, option variants, and error wrapping.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from celerity.resources.cache.errors import CacheError
from celerity.resources.cache.providers.redis.cache import RedisCache
from celerity.resources.cache.types import SetOptions


@pytest.fixture
def mock_client() -> AsyncMock:
    """Create a mock Redis client with common methods."""
    client = AsyncMock()
    client.pipeline = MagicMock()
    return client


@pytest.fixture
def cache(mock_client: AsyncMock) -> RedisCache:
    """Create a standalone RedisCache with no prefix."""
    return RedisCache(client=mock_client, cluster_mode=False, key_prefix="")


@pytest.fixture
def prefixed_cache(mock_client: AsyncMock) -> RedisCache:
    """Create a standalone RedisCache with a key prefix."""
    return RedisCache(client=mock_client, cluster_mode=False, key_prefix="app:")


@pytest.fixture
def cluster_cache(mock_client: AsyncMock) -> RedisCache:
    """Create a cluster-mode RedisCache."""
    return RedisCache(client=mock_client, cluster_mode=True, key_prefix="")


# ---------------------------------------------------------------------------
# Key prefix validation
# ---------------------------------------------------------------------------


class TestKeyPrefixValidation:
    def test_rejects_open_brace(self) -> None:
        with pytest.raises(CacheError, match="must not contain"):
            RedisCache(client=AsyncMock(), cluster_mode=False, key_prefix="bad{prefix")

    def test_rejects_close_brace(self) -> None:
        with pytest.raises(CacheError, match="must not contain"):
            RedisCache(client=AsyncMock(), cluster_mode=False, key_prefix="bad}prefix")

    def test_accepts_clean_prefix(self) -> None:
        cache = RedisCache(client=AsyncMock(), cluster_mode=False, key_prefix="good:")
        assert cache._key_prefix == "good:"


# ---------------------------------------------------------------------------
# Key prefix wiring
# ---------------------------------------------------------------------------


class TestKeyPrefixWiring:
    @pytest.mark.asyncio
    async def test_get_with_prefix(
        self, prefixed_cache: RedisCache, mock_client: AsyncMock
    ) -> None:
        mock_client.get.return_value = "val"
        await prefixed_cache.get("key")
        mock_client.get.assert_awaited_once_with("app:key")


# ---------------------------------------------------------------------------
# SetOptions variants not covered by integration tests
# ---------------------------------------------------------------------------


class TestSetOptionsVariants:
    @pytest.mark.asyncio
    async def test_set_xx(self, cache: RedisCache, mock_client: AsyncMock) -> None:
        mock_client.set.return_value = True
        await cache.set("key", "val", SetOptions(if_exists=True))
        mock_client.set.assert_awaited_once_with("key", "val", xx=True)


# ---------------------------------------------------------------------------
# Cluster-mode specific behaviour
# ---------------------------------------------------------------------------


class TestClusterMode:
    @pytest.mark.asyncio
    async def test_mget_cluster_fans_out(
        self, cluster_cache: RedisCache, mock_client: AsyncMock
    ) -> None:
        keys = ["{x}:1", "{x}:2"]
        mock_client.mget.return_value = ["v1", "v2"]
        result = await cluster_cache.mget(keys)
        assert result == ["v1", "v2"]

    @pytest.mark.asyncio
    async def test_rename_cluster_validates_slot(
        self, cluster_cache: RedisCache, mock_client: AsyncMock
    ) -> None:
        with pytest.raises(CacheError, match="same slot"):
            await cluster_cache.rename("alpha", "beta")

    @pytest.mark.asyncio
    async def test_set_union_cluster_validates_slot(self, cluster_cache: RedisCache) -> None:
        with pytest.raises(CacheError, match="same slot"):
            await cluster_cache.set_union(["alpha", "beta"])


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_list_pop_empty(self, cache: RedisCache, mock_client: AsyncMock) -> None:
        mock_client.lpop.return_value = None
        result = await cache.list_pop("empty")
        assert result == []

    @pytest.mark.asyncio
    async def test_sorted_set_range_reverse(
        self, cache: RedisCache, mock_client: AsyncMock
    ) -> None:
        mock_client.zrevrange.return_value = ["b", "a"]
        result = await cache.sorted_set_range("z", 0, -1, reverse=True)
        assert result == ["b", "a"]

    @pytest.mark.asyncio
    async def test_sorted_set_range_by_score_with_limit(
        self, cache: RedisCache, mock_client: AsyncMock
    ) -> None:
        mock_client.zrangebyscore.return_value = ["a"]
        result = await cache.sorted_set_range_by_score("z", 0, 10, offset=0, count=1)
        assert result == ["a"]
        mock_client.zrangebyscore.assert_awaited_once_with(
            "z", 0, 10, withscores=False, start=0, num=1
        )


# ---------------------------------------------------------------------------
# Error wrapping
# ---------------------------------------------------------------------------


class TestErrorWrapping:
    @pytest.mark.asyncio
    async def test_redis_error_wrapped(self, cache: RedisCache, mock_client: AsyncMock) -> None:
        from redis.exceptions import RedisError

        mock_client.get.side_effect = RedisError("conn lost")
        with pytest.raises(CacheError, match="conn lost") as exc_info:
            await cache.get("key")
        assert isinstance(exc_info.value.__cause__, RedisError)
