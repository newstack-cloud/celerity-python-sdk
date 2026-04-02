"""Auto-generated mock factories for Celerity resource types."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from celerity.testing.discovery import ResourceTokenInfo


class MockAsyncIter:
    """Async iterator over a list of items.

    Used to mock sync methods that return async iterables
    (``scan``, ``query``, ``list``, ``scan_keys``).

    Example::

        ds = app.get_datastore_mock("usersDatastore")
        ds.scan.return_value = MockAsyncIter([user1, user2])
    """

    def __init__(self, items: list[Any] | None = None) -> None:
        self._items = iter(items or [])

    def __aiter__(self) -> AsyncIterator[Any]:
        return self

    async def __anext__(self) -> Any:
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration from None


# Async methods (awaited by the caller).
_DATASTORE_ASYNC_METHODS = [
    "get_item",
    "put_item",
    "delete_item",
    "batch_get_items",
    "batch_write_items",
]
# Sync methods that return an async iterable.
_DATASTORE_ITER_METHODS = ["query", "scan"]

_TOPIC_METHODS = ["publish", "publish_batch"]
_QUEUE_METHODS = ["send_message", "send_message_batch"]

_CACHE_ASYNC_METHODS = [
    "get",
    "set",
    "delete",
    "incr",
    "decr",
    "incr_float",
    "mget",
    "mset",
    "mdelete",
    "exists",
    "expire",
    "persist",
    "ttl",
    "rename",
    "get_set",
    "append",
    "key_type",
    "hash_get",
    "hash_set",
    "hash_delete",
    "hash_get_all",
    "hash_exists",
    "hash_incr",
    "hash_keys",
    "hash_len",
    "list_push",
    "list_pop",
    "list_range",
    "list_len",
    "list_trim",
    "list_index",
    "set_add",
    "set_remove",
    "set_members",
    "set_is_member",
    "set_len",
    "set_union",
    "set_intersect",
    "set_diff",
    "sorted_set_add",
    "sorted_set_remove",
    "sorted_set_score",
    "sorted_set_rank",
    "sorted_set_range",
    "sorted_set_range_by_score",
    "sorted_set_incr",
    "sorted_set_len",
    "transaction",
]
_CACHE_ITER_METHODS = ["scan_keys"]

_BUCKET_ASYNC_METHODS = [
    "get",
    "put",
    "delete",
    "info",
    "exists",
    "copy",
    "sign_url",
]
_BUCKET_ITER_METHODS = ["list"]

_CONFIG_METHODS = ["get", "get_or_throw", "get_all", "parse"]

# (async_methods, iter_methods) per resource type.
_RESOURCE_SPEC: dict[str, tuple[list[str], list[str]]] = {
    "datastore": (_DATASTORE_ASYNC_METHODS, _DATASTORE_ITER_METHODS),
    "topic": (_TOPIC_METHODS, []),
    "queue": (_QUEUE_METHODS, []),
    "cache": (_CACHE_ASYNC_METHODS, _CACHE_ITER_METHODS),
    "bucket": (_BUCKET_ASYNC_METHODS, _BUCKET_ITER_METHODS),
    "config": (_CONFIG_METHODS, []),
}


def create_resource_mock(resource_type: str) -> AsyncMock | None:
    """Create a mock with methods matching the resource interface.

    Async methods are ``AsyncMock`` instances. Sync methods that
    return async iterables (``scan``, ``query``, ``list``,
    ``scan_keys``) are ``MagicMock`` instances that return an empty
    ``MockAsyncIter`` by default — callers set items via::

        ds.scan.return_value = MockAsyncIter([item1, item2])

    Returns ``None`` for resource types that cannot be meaningfully
    mocked (e.g., ``sqlDatabase`` which uses SQLAlchemy's fluent API).
    """
    spec = _RESOURCE_SPEC.get(resource_type)
    if spec is None:
        return None

    async_methods, iter_methods = spec
    mock = AsyncMock()
    for method in async_methods:
        setattr(mock, method, AsyncMock())
    for method in iter_methods:
        setattr(mock, method, MagicMock(return_value=MockAsyncIter()))
    return mock


def create_mocks_for_tokens(
    tokens: list[ResourceTokenInfo],
) -> dict[str, Any]:
    """Create mock objects for all discovered resource tokens.

    Returns a dict of ``token_string → mock``. Tokens for unmockable
    resource types are omitted.
    """
    mocks: dict[str, Any] = {}
    for info in tokens:
        mock = create_resource_mock(info.type)
        if mock is not None:
            mocks[info.token] = mock
    return mocks
