"""Integration tests for RedisCache against Docker Valkey.

Requires: docker compose up -d (Valkey on port 6399)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline

from celerity.resources.cache.providers.redis.cache import RedisCache
from celerity.resources.cache.types import SetOptions, SortedSetMember

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


@pytest.fixture
async def redis_client() -> AsyncGenerator[Redis[str]]:
    """Standalone async Redis client for Valkey on port 6399."""
    client: Redis[str] = Redis(host="localhost", port=6399, decode_responses=True)
    yield client
    await client.close()


@pytest.fixture
async def cache(redis_client: Redis[str]) -> RedisCache:
    """A RedisCache instance with test prefix."""
    return RedisCache(client=redis_client, cluster_mode=False, key_prefix="test:")


@pytest.fixture(autouse=True)
async def cleanup(redis_client: Redis[str]) -> AsyncGenerator[None]:
    """Clean test keys before and after each test."""
    await _flush_test_keys(redis_client)
    yield
    await _flush_test_keys(redis_client)


async def _flush_test_keys(client: Redis[str]) -> None:
    """Delete all keys with test: prefix."""
    keys = []
    async for key in client.scan_iter(match="test:*"):
        keys.append(key)
    if keys:
        await client.delete(*keys)


class TestKeyValueIntegration:
    @pytest.mark.asyncio
    async def test_set_get_delete(self, cache: RedisCache) -> None:
        assert await cache.set("k1", "hello") is True
        assert await cache.get("k1") == "hello"
        assert await cache.delete("k1") is True
        assert await cache.get("k1") is None

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, cache: RedisCache) -> None:
        await cache.set("k2", "val", SetOptions(ttl_seconds=10))
        ttl = await cache.ttl("k2")
        assert 0 < ttl <= 10

    @pytest.mark.asyncio
    async def test_set_nx(self, cache: RedisCache) -> None:
        await cache.set("k3", "first")
        result = await cache.set("k3", "second", SetOptions(if_not_exists=True))
        assert result is False
        assert await cache.get("k3") == "first"

    @pytest.mark.asyncio
    async def test_get_set(self, cache: RedisCache) -> None:
        await cache.set("k4", "old")
        old = await cache.get_set("k4", "new")
        assert old == "old"
        assert await cache.get("k4") == "new"

    @pytest.mark.asyncio
    async def test_append(self, cache: RedisCache) -> None:
        await cache.set("k5", "hello")
        length = await cache.append("k5", " world")
        assert length == 11
        assert await cache.get("k5") == "hello world"


class TestBatchIntegration:
    @pytest.mark.asyncio
    async def test_mset_mget_mdelete(self, cache: RedisCache) -> None:
        await cache.mset([("b1", "v1"), ("b2", "v2"), ("b3", "v3")])
        result = await cache.mget(["b1", "b2", "b3", "missing"])
        assert result == ["v1", "v2", "v3", None]

        deleted = await cache.mdelete(["b1", "b2"])
        assert deleted == 2
        assert await cache.get("b1") is None


class TestKeyManagementIntegration:
    @pytest.mark.asyncio
    async def test_exists(self, cache: RedisCache) -> None:
        assert await cache.exists("nope") is False
        await cache.set("exists-key", "yes")
        assert await cache.exists("exists-key") is True

    @pytest.mark.asyncio
    async def test_expire_persist(self, cache: RedisCache) -> None:
        await cache.set("ep", "val")
        await cache.expire("ep", 30)
        ttl = await cache.ttl("ep")
        assert ttl > 0
        await cache.persist("ep")
        ttl = await cache.ttl("ep")
        assert ttl == -1

    @pytest.mark.asyncio
    async def test_key_type(self, cache: RedisCache) -> None:
        await cache.set("typed", "val")
        assert await cache.key_type("typed") == "string"
        assert await cache.key_type("nonexistent") is None

    @pytest.mark.asyncio
    async def test_rename(self, cache: RedisCache) -> None:
        await cache.set("old-name", "val")
        await cache.rename("old-name", "new-name")
        assert await cache.get("old-name") is None
        assert await cache.get("new-name") == "val"

    @pytest.mark.asyncio
    async def test_scan_keys(self, cache: RedisCache) -> None:
        await cache.mset([("scan1", "a"), ("scan2", "b")])
        found: list[str] = []
        async for key in cache.scan_keys(match="scan*"):
            found.append(key)
        assert sorted(found) == ["scan1", "scan2"]


class TestCounterIntegration:
    @pytest.mark.asyncio
    async def test_incr_decr(self, cache: RedisCache) -> None:
        assert await cache.incr("ctr") == 1
        assert await cache.incr("ctr", 5) == 6
        assert await cache.decr("ctr") == 5
        assert await cache.decr("ctr", 3) == 2

    @pytest.mark.asyncio
    async def test_incr_float(self, cache: RedisCache) -> None:
        result = await cache.incr_float("flt", 1.5)
        assert result == pytest.approx(1.5)
        result = await cache.incr_float("flt", 0.3)
        assert result == pytest.approx(1.8)


class TestHashIntegration:
    @pytest.mark.asyncio
    async def test_hash_ops(self, cache: RedisCache) -> None:
        await cache.hash_set("h", {"a": "1", "b": "2", "c": "3"})
        assert await cache.hash_get("h", "a") == "1"
        assert await cache.hash_exists("h", "b") is True
        assert await cache.hash_len("h") == 3
        assert sorted(await cache.hash_keys("h")) == ["a", "b", "c"]

        all_fields = await cache.hash_get_all("h")
        assert all_fields == {"a": "1", "b": "2", "c": "3"}

        deleted = await cache.hash_delete("h", ["a", "b"])
        assert deleted == 2

        result = await cache.hash_incr("h", "c", 5)
        assert result == 8  # "3" + 5


class TestListIntegration:
    @pytest.mark.asyncio
    async def test_list_ops(self, cache: RedisCache) -> None:
        await cache.list_push("lst", ["a", "b", "c"])
        assert await cache.list_len("lst") == 3
        assert await cache.list_range("lst", 0, -1) == ["a", "b", "c"]

        await cache.list_push("lst", ["z"], end="left")
        assert await cache.list_index("lst", 0) == "z"

        popped = await cache.list_pop("lst", end="left")
        assert popped == ["z"]

        popped = await cache.list_pop("lst", end="right")
        assert popped == ["c"]

        await cache.list_trim("lst", 0, 0)
        assert await cache.list_range("lst", 0, -1) == ["a"]


class TestSetIntegration:
    @pytest.mark.asyncio
    async def test_set_ops(self, cache: RedisCache) -> None:
        await cache.set_add("s1", ["a", "b", "c"])
        assert await cache.set_len("s1") == 3
        assert await cache.set_is_member("s1", "a") is True
        assert await cache.set_is_member("s1", "z") is False

        members = await cache.set_members("s1")
        assert sorted(members) == ["a", "b", "c"]

        await cache.set_remove("s1", ["b"])
        assert await cache.set_len("s1") == 2

    @pytest.mark.asyncio
    async def test_set_operations(self, cache: RedisCache) -> None:
        await cache.set_add("sa", ["a", "b", "c"])
        await cache.set_add("sb", ["b", "c", "d"])

        union = await cache.set_union(["sa", "sb"])
        assert sorted(union) == ["a", "b", "c", "d"]

        inter = await cache.set_intersect(["sa", "sb"])
        assert sorted(inter) == ["b", "c"]

        diff = await cache.set_diff(["sa", "sb"])
        assert sorted(diff) == ["a"]


class TestSortedSetIntegration:
    @pytest.mark.asyncio
    async def test_sorted_set_ops(self, cache: RedisCache) -> None:
        members = [
            SortedSetMember("alice", 10.0),
            SortedSetMember("bob", 20.0),
            SortedSetMember("charlie", 15.0),
        ]
        added = await cache.sorted_set_add("zs", members)
        assert added == 3
        assert await cache.sorted_set_len("zs") == 3

        score = await cache.sorted_set_score("zs", "alice")
        assert score == 10.0

        rank = await cache.sorted_set_rank("zs", "bob")
        assert rank == 2  # highest score, last in ascending order

        rank_rev = await cache.sorted_set_rank("zs", "bob", reverse=True)
        assert rank_rev == 0

        range_result = await cache.sorted_set_range("zs", 0, -1)
        assert range_result == ["alice", "charlie", "bob"]

        range_with_scores = await cache.sorted_set_range("zs", 0, -1, with_scores=True)
        assert range_with_scores == [
            SortedSetMember("alice", 10.0),
            SortedSetMember("charlie", 15.0),
            SortedSetMember("bob", 20.0),
        ]

        by_score = await cache.sorted_set_range_by_score("zs", 10, 16)
        assert by_score == ["alice", "charlie"]

        new_score = await cache.sorted_set_incr("zs", "alice", 25.0)
        assert new_score == 35.0

        removed = await cache.sorted_set_remove("zs", ["alice"])
        assert removed == 1


class TestTransactionIntegration:
    @pytest.mark.asyncio
    async def test_pipeline_transaction(self, cache: RedisCache) -> None:
        await cache.set("tx-key", "0")

        def build_pipeline(pipe: Pipeline[str]) -> list[str]:
            pipe.set("test:tx-key", "1")
            pipe.get("test:tx-key")
            return ["tx-key"]

        result = await cache.transaction(build_pipeline)
        assert len(result.results) == 2
