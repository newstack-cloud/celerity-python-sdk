"""Tests for DynamoDB paginated item listing."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from celerity.resources.datastore.providers.dynamodb.listing import (
    DynamoDBItemListing,
    _decode_cursor,
    _encode_cursor,
)


def _make_listing(
    pages: list[dict[str, Any]],
    limit: int | None = None,
    initial_cursor: str | None = None,
) -> DynamoDBItemListing:
    """Create a listing backed by a mock that returns pre-defined pages.

    Each page dict should have "Items" and optionally "LastEvaluatedKey".
    """
    mock_client = AsyncMock()
    call_count = 0

    async def fake_op(**_kwargs: dict[str, Any]) -> dict[str, Any]:
        nonlocal call_count
        idx = call_count
        call_count += 1
        if idx < len(pages):
            return pages[idx]
        return {"Items": []}

    mock_client.execute = fake_op

    return DynamoDBItemListing(
        client=mock_client,
        operation="execute",
        request_params={},
        limit=limit,
        initial_cursor=initial_cursor,
    )


class TestDynamoDBItemListing:
    @pytest.mark.asyncio
    async def test_single_page(self) -> None:
        listing = _make_listing(
            [
                {"Items": [{"pk": "1"}, {"pk": "2"}]},
            ]
        )
        items = await listing.items()
        assert items == [{"pk": "1"}, {"pk": "2"}]

    @pytest.mark.asyncio
    async def test_multiple_pages(self) -> None:
        listing = _make_listing(
            [
                {"Items": [{"pk": "1"}], "LastEvaluatedKey": {"pk": "1"}},
                {"Items": [{"pk": "2"}], "LastEvaluatedKey": {"pk": "2"}},
                {"Items": [{"pk": "3"}]},
            ]
        )
        items = await listing.items()
        assert items == [{"pk": "1"}, {"pk": "2"}, {"pk": "3"}]

    @pytest.mark.asyncio
    async def test_empty_result(self) -> None:
        listing = _make_listing([{"Items": []}])
        items = await listing.items()
        assert items == []

    @pytest.mark.asyncio
    async def test_async_iteration(self) -> None:
        listing = _make_listing(
            [
                {"Items": [{"pk": "a"}, {"pk": "b"}]},
            ]
        )
        collected = []
        async for item in listing:
            collected.append(item)
        assert collected == [{"pk": "a"}, {"pk": "b"}]

    @pytest.mark.asyncio
    async def test_cursor_returns_encoded_key(self) -> None:
        listing = _make_listing(
            [
                {"Items": [{"pk": "1"}], "LastEvaluatedKey": {"pk": {"S": "1"}}},
            ]
        )
        # Consume first page
        await listing.__anext__()
        cursor = listing.cursor()
        assert cursor is not None
        decoded = _decode_cursor(cursor)
        assert decoded == {"pk": {"S": "1"}}

    @pytest.mark.asyncio
    async def test_cursor_none_when_exhausted(self) -> None:
        listing = _make_listing([{"Items": [{"pk": "1"}]}])
        await listing.items()
        assert listing.cursor() is None

    @pytest.mark.asyncio
    async def test_resume_from_cursor(self) -> None:
        cursor = _encode_cursor({"pk": {"S": "5"}})
        mock_client = AsyncMock()
        mock_client.execute.return_value = {"Items": [{"pk": "6"}]}

        listing = DynamoDBItemListing(
            client=mock_client,
            operation="execute",
            request_params={"TableName": "test"},
            limit=None,
            initial_cursor=cursor,
        )
        items = await listing.items()
        assert items == [{"pk": "6"}]

        # Verify ExclusiveStartKey was passed
        call_kwargs = mock_client.execute.call_args.kwargs
        assert call_kwargs["ExclusiveStartKey"] == {"pk": {"S": "5"}}

    @pytest.mark.asyncio
    async def test_limit_passed_to_request(self) -> None:
        mock_client = AsyncMock()
        mock_client.execute.return_value = {"Items": [{"pk": "1"}]}

        listing = DynamoDBItemListing(
            client=mock_client,
            operation="execute",
            request_params={"TableName": "test"},
            limit=5,
            initial_cursor=None,
        )
        await listing.items()
        call_kwargs = mock_client.execute.call_args.kwargs
        assert call_kwargs["Limit"] == 5


class TestCursorEncoding:
    def test_round_trip(self) -> None:
        key = {"pk": {"S": "user-1"}, "sk": {"S": "profile"}}
        encoded = _encode_cursor(key)
        decoded = _decode_cursor(encoded)
        assert decoded == key

    def test_encoded_is_string(self) -> None:
        encoded = _encode_cursor({"pk": {"S": "1"}})
        assert isinstance(encoded, str)
