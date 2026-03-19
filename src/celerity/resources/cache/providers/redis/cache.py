"""RedisCache -- full Cache implementation backed by redis-py."""

# mypy: disable-error-code="no-any-return"
# All Redis commands are called via _call() which returns Any because
# self._client is typed as Any (types-redis stubs are incomplete for
# RedisCluster).  The return types on each method are still correct.

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from redis.asyncio import Redis, RedisCluster
from redis.exceptions import RedisError

from celerity.resources.cache.errors import CacheError
from celerity.resources.cache.providers.redis.cluster import (
    assert_same_slot,
    group_by_slot,
    hash_slot,
)
from celerity.resources.cache.types import (
    Cache,
    SetOptions,
    SortedSetMember,
    TransactionResult,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

# The types-redis stubs are incomplete for RedisCluster — method
# definitions live on command mixin classes that mypy cannot resolve
# through a ``Redis | RedisCluster`` union.  All Redis commands are
# called via ``_call`` which accepts ``Any`` at the boundary and wraps
# errors.  The constructor still accepts the typed union so call-sites
# are checked.
type _RedisClient = Redis[str] | RedisCluster[str]


def _validate_key_prefix(prefix: str) -> None:
    """Reject prefixes containing hash tag characters."""
    if "{" in prefix or "}" in prefix:
        raise CacheError(f"Key prefix must not contain '{{' or '}}': {prefix!r}")


class RedisCache(Cache):
    """Per-resource cache handle backed by redis-py."""

    def __init__(
        self,
        client: _RedisClient,
        cluster_mode: bool,
        key_prefix: str,
    ) -> None:
        _validate_key_prefix(key_prefix)
        # Stored as Any to avoid union-attr errors from incomplete
        # types-redis stubs — the constructor param is still typed.
        self._client: Any = client
        self._cluster_mode = cluster_mode
        self._key_prefix = key_prefix

    # -- Key helpers -------------------------------------------------------

    def _prefix_key(self, key: str) -> str:
        return f"{self._key_prefix}{key}" if self._key_prefix else key

    def _prefix_key_list(self, keys: list[str]) -> list[str]:
        if not self._key_prefix:
            return keys
        return [f"{self._key_prefix}{k}" for k in keys]

    def _strip(self, key: str | bytes) -> str:
        s = key.decode("utf-8") if isinstance(key, bytes) else key
        if self._key_prefix and s.startswith(self._key_prefix):
            return s[len(self._key_prefix) :]
        return s

    # -- Wrapped call ------------------------------------------------------

    async def _call(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return await fn(*args, **kwargs)
        except RedisError as exc:
            raise CacheError(str(exc), cause=exc) from exc

    # -- Key-Value ---------------------------------------------------------

    async def get(self, key: str) -> str | None:
        return await self._call(self._client.get, self._prefix_key(key))

    async def set(self, key: str, value: str, options: SetOptions | None = None) -> bool:
        kwargs: dict[str, Any] = {}
        if options:
            if options.ttl_seconds is not None:
                kwargs["ex"] = options.ttl_seconds
            if options.if_not_exists:
                kwargs["nx"] = True
            if options.if_exists:
                kwargs["xx"] = True
        result = await self._call(self._client.set, self._prefix_key(key), value, **kwargs)
        return result is not None and result is not False

    async def delete(self, key: str) -> bool:
        result = await self._call(self._client.delete, self._prefix_key(key))
        return result > 0

    async def ttl(self, key: str) -> int:
        return await self._call(self._client.ttl, self._prefix_key(key))

    async def get_set(self, key: str, value: str) -> str | None:
        return await self._call(self._client.getset, self._prefix_key(key), value)

    async def append(self, key: str, value: str) -> int:
        return await self._call(self._client.append, self._prefix_key(key), value)

    # -- Batch (cluster-aware) ---------------------------------------------

    async def mget(self, keys: list[str]) -> list[str | None]:
        prefixed = self._prefix_key_list(keys)
        if not self._cluster_mode:
            return await self._call(self._client.mget, prefixed)

        slots = group_by_slot(prefixed)
        results: list[str | None] = [None] * len(keys)
        for slot_keys in slots.values():
            slot_key_list = [k for _, k in slot_keys]
            slot_values = await self._call(self._client.mget, slot_key_list)
            for (orig_idx, _), val in zip(slot_keys, slot_values, strict=False):
                results[orig_idx] = val
        return results

    async def mset(self, entries: list[tuple[str, str]]) -> None:
        prefixed = [(self._prefix_key(k), v) for k, v in entries]
        if not self._cluster_mode:
            await self._call(self._client.mset, dict(prefixed))
            return

        slots: dict[int, dict[str, str]] = {}
        for key, val in prefixed:
            slot = hash_slot(key)
            slots.setdefault(slot, {})[key] = val
        for slot_entries in slots.values():
            await self._call(self._client.mset, slot_entries)

    async def mdelete(self, keys: list[str]) -> int:
        prefixed = self._prefix_key_list(keys)
        if not self._cluster_mode:
            return await self._call(self._client.delete, *prefixed)

        total = 0
        slots = group_by_slot(prefixed)
        for slot_keys in slots.values():
            slot_key_list = [k for _, k in slot_keys]
            total += await self._call(self._client.delete, *slot_key_list)
        return total

    # -- Key management ----------------------------------------------------

    async def exists(self, key: str) -> bool:
        result = await self._call(self._client.exists, self._prefix_key(key))
        return result > 0

    async def expire(self, key: str, seconds: int) -> bool:
        result = await self._call(self._client.expire, self._prefix_key(key), seconds)
        return bool(result)

    async def persist(self, key: str) -> bool:
        result = await self._call(self._client.persist, self._prefix_key(key))
        return bool(result)

    async def key_type(self, key: str) -> str | None:
        result = await self._call(self._client.type, self._prefix_key(key))
        return None if result == "none" else result

    async def rename(self, key: str, new_key: str) -> None:
        pk, pnk = self._prefix_key(key), self._prefix_key(new_key)
        if self._cluster_mode:
            assert_same_slot([pk, pnk])
        await self._call(self._client.rename, pk, pnk)

    async def scan_keys(
        self, match: str | None = None, count: int | None = None
    ) -> AsyncIterator[str]:
        pattern = f"{self._key_prefix}{match}" if match else f"{self._key_prefix}*"
        kwargs: dict[str, Any] = {"match": pattern}
        if count is not None:
            kwargs["count"] = count

        if self._cluster_mode:
            async for key in self._cluster_scan(**kwargs):
                yield key
        else:
            async for key in self._client.scan_iter(**kwargs):
                yield self._strip(key)

    async def _cluster_scan(self, **kwargs: Any) -> AsyncIterator[str]:
        """Iterate all master nodes individually."""
        for node in self._client.get_primaries():
            async for key in node.scan_iter(**kwargs):
                yield self._strip(key)

    # -- Counters ----------------------------------------------------------

    async def incr(self, key: str, amount: int = 1) -> int:
        return await self._call(self._client.incrby, self._prefix_key(key), amount)

    async def decr(self, key: str, amount: int = 1) -> int:
        return await self._call(self._client.decrby, self._prefix_key(key), amount)

    async def incr_float(self, key: str, amount: float) -> float:
        return await self._call(self._client.incrbyfloat, self._prefix_key(key), amount)

    # -- Hashes ------------------------------------------------------------

    async def hash_get(self, key: str, field: str) -> str | None:
        return await self._call(self._client.hget, self._prefix_key(key), field)

    async def hash_set(self, key: str, fields: dict[str, str]) -> None:
        await self._call(self._client.hset, self._prefix_key(key), mapping=fields)

    async def hash_delete(self, key: str, fields: list[str]) -> int:
        return await self._call(self._client.hdel, self._prefix_key(key), *fields)

    async def hash_get_all(self, key: str) -> dict[str, str]:
        return await self._call(self._client.hgetall, self._prefix_key(key))

    async def hash_exists(self, key: str, field: str) -> bool:
        result = await self._call(self._client.hexists, self._prefix_key(key), field)
        return bool(result)

    async def hash_incr(self, key: str, field: str, amount: int = 1) -> int:
        return await self._call(self._client.hincrby, self._prefix_key(key), field, amount)

    async def hash_keys(self, key: str) -> list[str]:
        return await self._call(self._client.hkeys, self._prefix_key(key))

    async def hash_len(self, key: str) -> int:
        return await self._call(self._client.hlen, self._prefix_key(key))

    # -- Lists -------------------------------------------------------------

    async def list_push(self, key: str, values: list[str], end: str = "right") -> int:
        pk = self._prefix_key(key)
        if end == "left":
            return await self._call(self._client.lpush, pk, *values)
        return await self._call(self._client.rpush, pk, *values)

    async def list_pop(self, key: str, end: str = "left", count: int = 1) -> list[str]:
        pk = self._prefix_key(key)
        if end == "left":
            result = await self._call(self._client.lpop, pk, count)
        else:
            result = await self._call(self._client.rpop, pk, count)
        if result is None:
            return []
        if isinstance(result, str):
            return [result]
        return list(result)

    async def list_range(self, key: str, start: int, stop: int) -> list[str]:
        return await self._call(self._client.lrange, self._prefix_key(key), start, stop)

    async def list_len(self, key: str) -> int:
        return await self._call(self._client.llen, self._prefix_key(key))

    async def list_trim(self, key: str, start: int, stop: int) -> None:
        await self._call(self._client.ltrim, self._prefix_key(key), start, stop)

    async def list_index(self, key: str, index: int) -> str | None:
        return await self._call(self._client.lindex, self._prefix_key(key), index)

    # -- Sets --------------------------------------------------------------

    async def set_add(self, key: str, members: list[str]) -> int:
        return await self._call(self._client.sadd, self._prefix_key(key), *members)

    async def set_remove(self, key: str, members: list[str]) -> int:
        return await self._call(self._client.srem, self._prefix_key(key), *members)

    async def set_members(self, key: str) -> list[str]:
        result = await self._call(self._client.smembers, self._prefix_key(key))
        return list(result)

    async def set_is_member(self, key: str, member: str) -> bool:
        result = await self._call(self._client.sismember, self._prefix_key(key), member)
        return bool(result)

    async def set_len(self, key: str) -> int:
        return await self._call(self._client.scard, self._prefix_key(key))

    async def set_union(self, keys: list[str]) -> list[str]:
        prefixed = self._prefix_key_list(keys)
        if self._cluster_mode:
            assert_same_slot(prefixed)
        result = await self._call(self._client.sunion, *prefixed)
        return list(result)

    async def set_intersect(self, keys: list[str]) -> list[str]:
        prefixed = self._prefix_key_list(keys)
        if self._cluster_mode:
            assert_same_slot(prefixed)
        result = await self._call(self._client.sinter, *prefixed)
        return list(result)

    async def set_diff(self, keys: list[str]) -> list[str]:
        prefixed = self._prefix_key_list(keys)
        if self._cluster_mode:
            assert_same_slot(prefixed)
        result = await self._call(self._client.sdiff, *prefixed)
        return list(result)

    # -- Sorted sets -------------------------------------------------------

    async def sorted_set_add(self, key: str, members: list[SortedSetMember]) -> int:
        mapping = {m.member: m.score for m in members}
        return await self._call(self._client.zadd, self._prefix_key(key), mapping)

    async def sorted_set_remove(self, key: str, members: list[str]) -> int:
        return await self._call(self._client.zrem, self._prefix_key(key), *members)

    async def sorted_set_score(self, key: str, member: str) -> float | None:
        return await self._call(self._client.zscore, self._prefix_key(key), member)

    async def sorted_set_rank(self, key: str, member: str, *, reverse: bool = False) -> int | None:
        if reverse:
            return await self._call(self._client.zrevrank, self._prefix_key(key), member)
        return await self._call(self._client.zrank, self._prefix_key(key), member)

    async def sorted_set_range(
        self,
        key: str,
        start: int,
        stop: int,
        *,
        reverse: bool = False,
        with_scores: bool = False,
    ) -> list[str] | list[SortedSetMember]:
        pk = self._prefix_key(key)
        if reverse:
            result = await self._call(
                self._client.zrevrange, pk, start, stop, withscores=with_scores
            )
        else:
            result = await self._call(self._client.zrange, pk, start, stop, withscores=with_scores)
        if with_scores:
            return [SortedSetMember(member=m, score=s) for m, s in result]
        return list(result)

    async def sorted_set_range_by_score(
        self,
        key: str,
        min_score: float | str,
        max_score: float | str,
        *,
        reverse: bool = False,
        with_scores: bool = False,
        offset: int | None = None,
        count: int | None = None,
    ) -> list[str] | list[SortedSetMember]:
        pk = self._prefix_key(key)
        kwargs: dict[str, Any] = {"withscores": with_scores}
        if offset is not None and count is not None:
            kwargs["start"] = offset
            kwargs["num"] = count

        if reverse:
            result = await self._call(
                self._client.zrevrangebyscore, pk, max_score, min_score, **kwargs
            )
        else:
            result = await self._call(
                self._client.zrangebyscore, pk, min_score, max_score, **kwargs
            )
        if with_scores:
            return [SortedSetMember(member=m, score=s) for m, s in result]
        return list(result)

    async def sorted_set_incr(self, key: str, member: str, amount: float) -> float:
        return await self._call(self._client.zincrby, self._prefix_key(key), amount, member)

    async def sorted_set_len(self, key: str) -> int:
        return await self._call(self._client.zcard, self._prefix_key(key))

    # -- Transactions ------------------------------------------------------

    async def transaction(self, fn: Any) -> TransactionResult:
        try:
            pipe = self._client.pipeline(transaction=True)
            watched_keys = fn(pipe)
            if watched_keys and self._cluster_mode:
                prefixed = self._prefix_key_list(watched_keys)
                assert_same_slot(prefixed)
            results = await pipe.execute()
            return TransactionResult(results=list(results))
        except RedisError as exc:
            raise CacheError(str(exc), cause=exc) from exc
