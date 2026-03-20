"""Tests for S3ObjectListing async iterator."""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

import pytest

from celerity.resources.bucket.errors import BucketError
from celerity.resources.bucket.providers.s3.listing import S3ObjectListing

NOW = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)


def _make_entry(key: str, size: int = 100) -> dict[str, Any]:
    return {
        "Key": key,
        "Size": size,
        "LastModified": NOW,
        "ETag": '"etag"',
    }


def _make_response(
    keys: list[str],
    is_truncated: bool = False,
    next_token: str | None = None,
) -> dict[str, Any]:
    resp: dict[str, Any] = {
        "Contents": [_make_entry(k) for k in keys],
        "IsTruncated": is_truncated,
    }
    if next_token:
        resp["NextContinuationToken"] = next_token
    return resp


@pytest.fixture
def mock_client() -> AsyncMock:
    return AsyncMock()


def _make_listing(
    mock_client: AsyncMock,
    prefix: str | None = None,
    max_keys: int | None = None,
    cursor: str | None = None,
    tracer: AsyncMock | None = None,
) -> S3ObjectListing:
    async def provider() -> AsyncMock:
        return mock_client

    return S3ObjectListing(
        client_provider=provider,
        bucket_name="test-bucket",
        prefix=prefix,
        max_keys=max_keys,
        cursor=cursor,
        tracer=tracer,
    )


class TestSinglePage:
    @pytest.mark.asyncio
    async def test_iterates_all_items(self, mock_client: AsyncMock) -> None:
        mock_client.list_objects_v2.return_value = _make_response(["a.txt", "b.txt", "c.txt"])
        listing = _make_listing(mock_client)
        items = [item async for item in listing]
        assert len(items) == 3
        assert items[0].key == "a.txt"
        assert items[1].key == "b.txt"
        assert items[2].key == "c.txt"

    @pytest.mark.asyncio
    async def test_object_info_fields(self, mock_client: AsyncMock) -> None:
        mock_client.list_objects_v2.return_value = _make_response(["file.txt"])
        listing = _make_listing(mock_client)
        items = [item async for item in listing]
        info = items[0]
        assert info.key == "file.txt"
        assert info.size == 100
        assert info.etag == "etag"
        assert info.last_modified == NOW
        assert info.metadata == {}
        assert info.content_type == ""


class TestMultiPage:
    @pytest.mark.asyncio
    async def test_fetches_next_page(self, mock_client: AsyncMock) -> None:
        mock_client.list_objects_v2.side_effect = [
            _make_response(["a.txt", "b.txt"], is_truncated=True, next_token="tok1"),
            _make_response(["c.txt"]),
        ]
        listing = _make_listing(mock_client)
        items = [item async for item in listing]
        assert len(items) == 3
        assert mock_client.list_objects_v2.await_count == 2

    @pytest.mark.asyncio
    async def test_continuation_token_passed(self, mock_client: AsyncMock) -> None:
        mock_client.list_objects_v2.side_effect = [
            _make_response(["a.txt"], is_truncated=True, next_token="page2"),
            _make_response(["b.txt"]),
        ]
        listing = _make_listing(mock_client)
        _ = [item async for item in listing]
        second_call_kwargs = mock_client.list_objects_v2.call_args_list[1].kwargs
        assert second_call_kwargs["ContinuationToken"] == "page2"


class TestEmpty:
    @pytest.mark.asyncio
    async def test_empty_result(self, mock_client: AsyncMock) -> None:
        mock_client.list_objects_v2.return_value = {"Contents": [], "IsTruncated": False}
        listing = _make_listing(mock_client)
        items = [item async for item in listing]
        assert items == []


class TestPrefixFiltering:
    @pytest.mark.asyncio
    async def test_prefix_passed(self, mock_client: AsyncMock) -> None:
        mock_client.list_objects_v2.return_value = _make_response(["images/a.png"])
        listing = _make_listing(mock_client, prefix="images/")
        _ = [item async for item in listing]
        call_kwargs = mock_client.list_objects_v2.call_args.kwargs
        assert call_kwargs["Prefix"] == "images/"


class TestMaxKeys:
    @pytest.mark.asyncio
    async def test_max_keys_passed(self, mock_client: AsyncMock) -> None:
        mock_client.list_objects_v2.return_value = _make_response(["a.txt"])
        listing = _make_listing(mock_client, max_keys=5)
        _ = [item async for item in listing]
        call_kwargs = mock_client.list_objects_v2.call_args.kwargs
        assert call_kwargs["MaxKeys"] == 5


class TestCursorResume:
    @pytest.mark.asyncio
    async def test_initial_cursor_decoded_to_s3_token(self, mock_client: AsyncMock) -> None:
        """A base64-encoded cursor is decoded back to the raw S3 token."""
        encoded = base64.urlsafe_b64encode(b"resume-token").decode()
        mock_client.list_objects_v2.return_value = _make_response(["a.txt"])
        listing = _make_listing(mock_client, cursor=encoded)
        _ = [item async for item in listing]
        call_kwargs = mock_client.list_objects_v2.call_args.kwargs
        assert call_kwargs["ContinuationToken"] == "resume-token"

    @pytest.mark.asyncio
    async def test_cursor_property_is_base64_encoded(self, mock_client: AsyncMock) -> None:
        mock_client.list_objects_v2.return_value = _make_response(
            ["a.txt"], is_truncated=True, next_token="next-page"
        )
        listing = _make_listing(mock_client)
        _ = await listing.__anext__()
        cursor = listing.cursor
        assert cursor is not None
        assert base64.urlsafe_b64decode(cursor.encode()).decode() == "next-page"

    @pytest.mark.asyncio
    async def test_cursor_none_when_exhausted(self, mock_client: AsyncMock) -> None:
        mock_client.list_objects_v2.return_value = _make_response(["a.txt"])
        listing = _make_listing(mock_client)
        _ = [item async for item in listing]
        assert listing.cursor is None


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_wraps_error(self, mock_client: AsyncMock) -> None:
        mock_client.list_objects_v2.side_effect = Exception("network error")
        listing = _make_listing(mock_client)
        with pytest.raises(BucketError, match="list_objects_v2"):
            _ = [item async for item in listing]


class TestTracer:
    @pytest.mark.asyncio
    async def test_span_per_page(self, mock_client: AsyncMock) -> None:
        mock_tracer = AsyncMock()

        async def with_span_impl(
            name: str,
            fn: Callable[..., Awaitable[Any]],
            attributes: dict[str, Any] | None = None,
        ) -> Any:
            return await fn(None)

        mock_tracer.with_span = AsyncMock(side_effect=with_span_impl)
        mock_client.list_objects_v2.side_effect = [
            _make_response(["a.txt"], is_truncated=True, next_token="tok"),
            _make_response(["b.txt"]),
        ]
        listing = _make_listing(mock_client, tracer=mock_tracer)
        _ = [item async for item in listing]
        assert mock_tracer.with_span.await_count == 2
        for call in mock_tracer.with_span.call_args_list:
            assert call[0][0] == "celerity.bucket.list_page"
            assert call[1]["attributes"]["bucket.name"] == "test-bucket"

    @pytest.mark.asyncio
    async def test_no_tracer(self, mock_client: AsyncMock) -> None:
        mock_client.list_objects_v2.return_value = _make_response(["a.txt"])
        listing = _make_listing(mock_client)
        items = [item async for item in listing]
        assert len(items) == 1
