"""Tests for S3Bucket provider — error wrapping, instrumentation, and wire-level params.

Happy-path behavior is covered by integration/test_bucket_s3.py.
This file tests what integration tests cannot: error wrapping into BucketError,
tracer span creation with correct names/attributes, and exact S3 API parameter
formatting (Range headers, MetadataDirective, ClientMethod mapping).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from celerity.resources.bucket.errors import BucketError
from celerity.resources.bucket.providers.s3.client import S3Bucket
from celerity.resources.bucket.types import (
    CopyObjectOptions,
    GetObjectOptions,
    PutObjectOptions,
    SignUrlOptions,
)

NOW = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)


def _make_head_response(
    size: int = 100,
    content_type: str = "text/plain",
    etag: str = '"abc123"',
    metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "ContentLength": size,
        "ContentType": content_type,
        "LastModified": NOW,
        "ETag": etag,
        "Metadata": metadata or {},
    }


def _make_get_response(body_data: bytes = b"hello") -> dict[str, Any]:
    body = AsyncMock()

    async def aiter_helper() -> AsyncIterator[bytes]:
        for c in [body_data]:
            yield c

    body.__aiter__ = lambda self: aiter_helper()
    resp = _make_head_response()
    resp["Body"] = body
    return resp


def _not_found_error() -> Exception:
    """Create an exception that mimics a botocore ClientError 404."""
    exc = Exception("Not Found")
    exc.response = {  # type: ignore[attr-defined]
        "Error": {"Code": "NoSuchKey"},
        "ResponseMetadata": {"HTTPStatusCode": 404},
    }
    return exc


def _access_denied_error() -> Exception:
    exc = Exception("Access Denied")
    exc.response = {  # type: ignore[attr-defined]
        "Error": {"Code": "AccessDenied"},
        "ResponseMetadata": {"HTTPStatusCode": 403},
    }
    return exc


@pytest.fixture
def mock_client() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def bucket(mock_client: AsyncMock) -> S3Bucket:
    async def provider() -> AsyncMock:
        return mock_client

    return S3Bucket(client_provider=provider, bucket_name="test-bucket")


@pytest.fixture
def mock_tracer() -> AsyncMock:
    tracer = AsyncMock()

    async def with_span_impl(
        name: str,
        fn: Callable[..., Awaitable[Any]],
        attributes: dict[str, Any] | None = None,
    ) -> Any:
        return await fn(None)

    tracer.with_span = AsyncMock(side_effect=with_span_impl)
    return tracer


@pytest.fixture
def traced_bucket(mock_client: AsyncMock, mock_tracer: AsyncMock) -> S3Bucket:
    async def provider() -> AsyncMock:
        return mock_client

    return S3Bucket(client_provider=provider, bucket_name="test-bucket", tracer=mock_tracer)


class TestErrorWrapping:
    @pytest.mark.asyncio
    async def test_get_not_found_returns_none(
        self, bucket: S3Bucket, mock_client: AsyncMock
    ) -> None:
        mock_client.get_object.side_effect = _not_found_error()
        assert await bucket.get("missing.txt") is None

    @pytest.mark.asyncio
    async def test_get_non_404_raises(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.get_object.side_effect = _access_denied_error()
        with pytest.raises(BucketError, match="get_object"):
            await bucket.get("test.txt")

    @pytest.mark.asyncio
    async def test_put(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.put_object.side_effect = Exception("boom")
        with pytest.raises(BucketError, match="put_object"):
            await bucket.put("test.txt", "data")

    @pytest.mark.asyncio
    async def test_delete(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.delete_object.side_effect = Exception("boom")
        with pytest.raises(BucketError, match="delete_object"):
            await bucket.delete("test.txt")

    @pytest.mark.asyncio
    async def test_info_not_found_returns_none(
        self, bucket: S3Bucket, mock_client: AsyncMock
    ) -> None:
        mock_client.head_object.side_effect = _not_found_error()
        assert await bucket.info("missing.txt") is None

    @pytest.mark.asyncio
    async def test_info_non_404_raises(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.head_object.side_effect = _access_denied_error()
        with pytest.raises(BucketError, match="head_object"):
            await bucket.info("test.txt")

    @pytest.mark.asyncio
    async def test_copy(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.copy_object.side_effect = Exception("boom")
        with pytest.raises(BucketError, match="copy_object"):
            await bucket.copy("src.txt", "dest.txt")

    @pytest.mark.asyncio
    async def test_sign_url(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.generate_presigned_url.side_effect = Exception("boom")
        with pytest.raises(BucketError, match="generate_presigned_url"):
            await bucket.sign_url("test.txt")


class TestTracerSpans:
    @pytest.mark.asyncio
    async def test_get(
        self, traced_bucket: S3Bucket, mock_client: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        mock_client.get_object.return_value = _make_get_response()
        await traced_bucket.get("test.txt")
        mock_tracer.with_span.assert_awaited_once()
        call_args = mock_tracer.with_span.call_args
        assert call_args[0][0] == "celerity.bucket.get"
        assert call_args[1]["attributes"]["bucket.name"] == "test-bucket"
        assert call_args[1]["attributes"]["bucket.key"] == "test.txt"

    @pytest.mark.asyncio
    async def test_put(
        self, traced_bucket: S3Bucket, mock_client: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        await traced_bucket.put("test.txt", "data")
        mock_tracer.with_span.assert_awaited_once()
        call_args = mock_tracer.with_span.call_args
        assert call_args[0][0] == "celerity.bucket.put"
        assert call_args[1]["attributes"]["bucket.key"] == "test.txt"

    @pytest.mark.asyncio
    async def test_delete(
        self, traced_bucket: S3Bucket, mock_client: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        await traced_bucket.delete("test.txt")
        mock_tracer.with_span.assert_awaited_once()
        assert mock_tracer.with_span.call_args[0][0] == "celerity.bucket.delete"

    @pytest.mark.asyncio
    async def test_info(
        self, traced_bucket: S3Bucket, mock_client: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        mock_client.head_object.return_value = _make_head_response()
        await traced_bucket.info("test.txt")
        mock_tracer.with_span.assert_awaited_once()
        assert mock_tracer.with_span.call_args[0][0] == "celerity.bucket.info"

    @pytest.mark.asyncio
    async def test_copy(
        self, traced_bucket: S3Bucket, mock_client: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        await traced_bucket.copy("src.txt", "dest.txt")
        mock_tracer.with_span.assert_awaited_once()
        call_args = mock_tracer.with_span.call_args
        assert call_args[0][0] == "celerity.bucket.copy"
        attrs = call_args[1]["attributes"]
        assert attrs["bucket.key"] == "src.txt"
        assert attrs["bucket.dest_key"] == "dest.txt"
        assert attrs["bucket.dest_bucket"] == "test-bucket"

    @pytest.mark.asyncio
    async def test_sign_url(
        self, traced_bucket: S3Bucket, mock_client: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        mock_client.generate_presigned_url.return_value = "https://url"
        await traced_bucket.sign_url("test.txt")
        mock_tracer.with_span.assert_awaited_once()
        assert mock_tracer.with_span.call_args[0][0] == "celerity.bucket.sign_url"


class TestRangeHeader:
    @pytest.mark.asyncio
    async def test_start_and_end(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.get_object.return_value = _make_get_response()
        await bucket.get("f.bin", GetObjectOptions(range_start=10, range_end=19))
        assert mock_client.get_object.call_args.kwargs["Range"] == "bytes=10-19"

    @pytest.mark.asyncio
    async def test_start_only(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.get_object.return_value = _make_get_response()
        await bucket.get("f.bin", GetObjectOptions(range_start=10))
        assert mock_client.get_object.call_args.kwargs["Range"] == "bytes=10-"

    @pytest.mark.asyncio
    async def test_end_only(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.get_object.return_value = _make_get_response()
        await bucket.get("f.bin", GetObjectOptions(range_end=19))
        assert mock_client.get_object.call_args.kwargs["Range"] == "bytes=0-19"

    @pytest.mark.asyncio
    async def test_no_range(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.get_object.return_value = _make_get_response()
        await bucket.get("f.bin")
        assert "Range" not in mock_client.get_object.call_args.kwargs


class TestCopyParams:
    @pytest.mark.asyncio
    async def test_copy_source_format(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        await bucket.copy("src.txt", "dest.txt")
        kwargs = mock_client.copy_object.call_args.kwargs
        assert kwargs["CopySource"] == "test-bucket/src.txt"
        assert kwargs["Bucket"] == "test-bucket"

    @pytest.mark.asyncio
    async def test_cross_bucket_dest(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        await bucket.copy("src.txt", "dest.txt", CopyObjectOptions(dest_bucket="other"))
        assert mock_client.copy_object.call_args.kwargs["Bucket"] == "other"

    @pytest.mark.asyncio
    async def test_metadata_directive_on_metadata(
        self, bucket: S3Bucket, mock_client: AsyncMock
    ) -> None:
        await bucket.copy("s.txt", "d.txt", CopyObjectOptions(metadata={"k": "v"}))
        kwargs = mock_client.copy_object.call_args.kwargs
        assert kwargs["MetadataDirective"] == "REPLACE"
        assert kwargs["Metadata"] == {"k": "v"}

    @pytest.mark.asyncio
    async def test_metadata_directive_on_content_type(
        self, bucket: S3Bucket, mock_client: AsyncMock
    ) -> None:
        await bucket.copy("s.txt", "d.txt", CopyObjectOptions(content_type="image/png"))
        kwargs = mock_client.copy_object.call_args.kwargs
        assert kwargs["MetadataDirective"] == "REPLACE"
        assert kwargs["ContentType"] == "image/png"


class TestPutBodyEncoding:
    @pytest.mark.asyncio
    async def test_string_encoded_to_utf8(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        await bucket.put("test.txt", "hello")
        assert mock_client.put_object.call_args.kwargs["Body"] == b"hello"

    @pytest.mark.asyncio
    async def test_bytes_passed_directly(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        await bucket.put("test.bin", b"\x00\x01")
        assert mock_client.put_object.call_args.kwargs["Body"] == b"\x00\x01"

    @pytest.mark.asyncio
    async def test_content_type_param(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        await bucket.put("f.png", b"data", PutObjectOptions(content_type="image/png"))
        assert mock_client.put_object.call_args.kwargs["ContentType"] == "image/png"

    @pytest.mark.asyncio
    async def test_metadata_param(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        await bucket.put("f.txt", "data", PutObjectOptions(metadata={"k": "v"}))
        assert mock_client.put_object.call_args.kwargs["Metadata"] == {"k": "v"}


class TestSignUrlParams:
    @pytest.mark.asyncio
    async def test_get_method(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.generate_presigned_url.return_value = "https://url"
        await bucket.sign_url("f.txt")
        kwargs = mock_client.generate_presigned_url.call_args.kwargs
        assert kwargs["ClientMethod"] == "get_object"
        assert kwargs["ExpiresIn"] == 3600

    @pytest.mark.asyncio
    async def test_put_method(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.generate_presigned_url.return_value = "https://url"
        await bucket.sign_url("f.txt", SignUrlOptions(method="put"))
        assert mock_client.generate_presigned_url.call_args.kwargs["ClientMethod"] == "put_object"

    @pytest.mark.asyncio
    async def test_custom_expiry(self, bucket: S3Bucket, mock_client: AsyncMock) -> None:
        mock_client.generate_presigned_url.return_value = "https://url"
        await bucket.sign_url("f.txt", SignUrlOptions(expires_in=600))
        assert mock_client.generate_presigned_url.call_args.kwargs["ExpiresIn"] == 600
