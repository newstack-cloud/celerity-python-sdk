"""Cache ABCs and supporting types."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@dataclass(frozen=True)
class SetOptions:
    """Options for the ``set`` operation."""

    ttl_seconds: int | None = None
    if_not_exists: bool = False
    if_exists: bool = False


@dataclass(frozen=True)
class SortedSetMember:
    """A member-score pair for sorted set operations."""

    member: str
    score: float


@dataclass(frozen=True)
class TransactionResult:
    """Result of a cache transaction (MULTI/EXEC)."""

    results: list[Any]


class Cache(ABC):
    """Per-resource cache handle with a full Redis-like API."""

    # -- Key-Value ---------------------------------------------------------

    @abstractmethod
    async def get(self, key: str) -> str | None: ...

    @abstractmethod
    async def set(self, key: str, value: str, options: SetOptions | None = None) -> bool: ...

    @abstractmethod
    async def delete(self, key: str) -> bool: ...

    @abstractmethod
    async def ttl(self, key: str) -> int: ...

    @abstractmethod
    async def get_set(self, key: str, value: str) -> str | None: ...

    @abstractmethod
    async def append(self, key: str, value: str) -> int: ...

    # -- Batch (cluster-aware) ---------------------------------------------

    @abstractmethod
    async def mget(self, keys: list[str]) -> list[str | None]: ...

    @abstractmethod
    async def mset(self, entries: list[tuple[str, str]]) -> None: ...

    @abstractmethod
    async def mdelete(self, keys: list[str]) -> int: ...

    # -- Key management ----------------------------------------------------

    @abstractmethod
    async def exists(self, key: str) -> bool: ...

    @abstractmethod
    async def expire(self, key: str, seconds: int) -> bool: ...

    @abstractmethod
    async def persist(self, key: str) -> bool: ...

    @abstractmethod
    async def key_type(self, key: str) -> str | None: ...

    @abstractmethod
    async def rename(self, key: str, new_key: str) -> None: ...

    @abstractmethod
    def scan_keys(
        self, match: str | None = None, count: int | None = None
    ) -> AsyncIterator[str]: ...

    # -- Counters ----------------------------------------------------------

    @abstractmethod
    async def incr(self, key: str, amount: int = 1) -> int: ...

    @abstractmethod
    async def decr(self, key: str, amount: int = 1) -> int: ...

    @abstractmethod
    async def incr_float(self, key: str, amount: float) -> float: ...

    # -- Hashes ------------------------------------------------------------

    @abstractmethod
    async def hash_get(self, key: str, field: str) -> str | None: ...

    @abstractmethod
    async def hash_set(self, key: str, fields: dict[str, str]) -> None: ...

    @abstractmethod
    async def hash_delete(self, key: str, fields: list[str]) -> int: ...

    @abstractmethod
    async def hash_get_all(self, key: str) -> dict[str, str]: ...

    @abstractmethod
    async def hash_exists(self, key: str, field: str) -> bool: ...

    @abstractmethod
    async def hash_incr(self, key: str, field: str, amount: int = 1) -> int: ...

    @abstractmethod
    async def hash_keys(self, key: str) -> list[str]: ...

    @abstractmethod
    async def hash_len(self, key: str) -> int: ...

    # -- Lists -------------------------------------------------------------

    @abstractmethod
    async def list_push(self, key: str, values: list[str], end: str = "right") -> int: ...

    @abstractmethod
    async def list_pop(self, key: str, end: str = "left", count: int = 1) -> list[str]: ...

    @abstractmethod
    async def list_range(self, key: str, start: int, stop: int) -> list[str]: ...

    @abstractmethod
    async def list_len(self, key: str) -> int: ...

    @abstractmethod
    async def list_trim(self, key: str, start: int, stop: int) -> None: ...

    @abstractmethod
    async def list_index(self, key: str, index: int) -> str | None: ...

    # -- Sets --------------------------------------------------------------

    @abstractmethod
    async def set_add(self, key: str, members: list[str]) -> int: ...

    @abstractmethod
    async def set_remove(self, key: str, members: list[str]) -> int: ...

    @abstractmethod
    async def set_members(self, key: str) -> list[str]: ...

    @abstractmethod
    async def set_is_member(self, key: str, member: str) -> bool: ...

    @abstractmethod
    async def set_len(self, key: str) -> int: ...

    @abstractmethod
    async def set_union(self, keys: list[str]) -> list[str]: ...

    @abstractmethod
    async def set_intersect(self, keys: list[str]) -> list[str]: ...

    @abstractmethod
    async def set_diff(self, keys: list[str]) -> list[str]: ...

    # -- Sorted sets -------------------------------------------------------

    @abstractmethod
    async def sorted_set_add(self, key: str, members: list[SortedSetMember]) -> int: ...

    @abstractmethod
    async def sorted_set_remove(self, key: str, members: list[str]) -> int: ...

    @abstractmethod
    async def sorted_set_score(self, key: str, member: str) -> float | None: ...

    @abstractmethod
    async def sorted_set_rank(
        self, key: str, member: str, *, reverse: bool = False
    ) -> int | None: ...

    @abstractmethod
    async def sorted_set_range(
        self,
        key: str,
        start: int,
        stop: int,
        *,
        reverse: bool = False,
        with_scores: bool = False,
    ) -> list[str] | list[SortedSetMember]: ...

    @abstractmethod
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
    ) -> list[str] | list[SortedSetMember]: ...

    @abstractmethod
    async def sorted_set_incr(self, key: str, member: str, amount: float) -> float: ...

    @abstractmethod
    async def sorted_set_len(self, key: str) -> int: ...

    # -- Transactions ------------------------------------------------------

    @abstractmethod
    async def transaction(self, fn: Any) -> TransactionResult: ...


class CacheClient(ABC):
    """Top-level cache client managing the Redis connection.

    Resource metadata (key prefix) is resolved at construction time
    so ``cache()`` takes only the logical resource name.
    """

    @abstractmethod
    def cache(self, name: str) -> Cache:
        """Get a cache handle for a named resource.

        Args:
            name: The logical resource name. The provider resolves
                key prefix and other metadata internally.
        """

    @abstractmethod
    async def close(self) -> None:
        """Close the underlying connection."""
