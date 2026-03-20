"""DynamoDB paginated item listing (async iterator)."""

from __future__ import annotations

import base64
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from celerity.resources.datastore.types import ItemListing


class DynamoDBItemListing(ItemListing):
    """AsyncIterator that lazily paginates through DynamoDB query/scan results.

    Each page fetches up to ``limit`` items (or DynamoDB's default page size).
    The cursor is a base64-encoded JSON serialization of DynamoDB's
    ``ExclusiveStartKey``, allowing pagination to be resumed across requests.
    """

    def __init__(
        self,
        client: Any,
        operation: str,
        request_params: dict[str, Any],
        limit: int | None,
        initial_cursor: str | None,
    ) -> None:
        self._client = client
        self._operation = operation
        self._request_params = request_params
        self._limit = limit
        self._buffer: list[dict[str, Any]] = []
        self._buffer_index: int = 0
        self._last_evaluated_key: dict[str, Any] | None = (
            _decode_cursor(initial_cursor) if initial_cursor else None
        )
        self._exhausted: bool = False

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self

    async def __anext__(self) -> dict[str, Any]:
        if self._buffer_index < len(self._buffer):
            item = self._buffer[self._buffer_index]
            self._buffer_index += 1
            return item
        if self._exhausted:
            raise StopAsyncIteration
        await self._fetch_page()
        if not self._buffer:
            raise StopAsyncIteration
        self._buffer_index = 1
        return self._buffer[0]

    async def items(self) -> list[dict[str, Any]]:
        """Fetch all remaining items into a list."""
        result: list[dict[str, Any]] = []
        async for item in self:
            result.append(item)
        return result

    def cursor(self) -> str | None:
        """Return the cursor for resuming pagination, or ``None`` if exhausted."""
        if self._exhausted:
            return None
        if self._last_evaluated_key:
            return _encode_cursor(self._last_evaluated_key)
        return None

    async def _fetch_page(self) -> None:
        """Fetch the next page of results from DynamoDB."""
        params = {**self._request_params}
        if self._last_evaluated_key:
            params["ExclusiveStartKey"] = self._last_evaluated_key
        if self._limit:
            params["Limit"] = self._limit

        response = await getattr(self._client, self._operation)(**params)
        self._buffer = response.get("Items", [])
        self._buffer_index = 0
        self._last_evaluated_key = response.get("LastEvaluatedKey")
        if not self._last_evaluated_key:
            self._exhausted = True


def _encode_cursor(key: dict[str, Any]) -> str:
    """Base64-encode a DynamoDB LastEvaluatedKey for use as a cursor."""
    return base64.urlsafe_b64encode(json.dumps(key).encode()).decode()


def _decode_cursor(cursor: str) -> dict[str, Any]:
    """Decode a cursor back to a DynamoDB ExclusiveStartKey."""
    return json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())  # type: ignore[no-any-return]
