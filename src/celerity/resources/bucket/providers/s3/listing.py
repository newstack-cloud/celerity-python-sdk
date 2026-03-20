"""S3 paginated object listing (async iterator)."""

from __future__ import annotations

import base64
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from celerity.resources.bucket.errors import BucketError
from celerity.resources.bucket.types import ObjectInfo, ObjectListing

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from types_aiobotocore_s3.client import S3Client

    from celerity.types.telemetry import CelerityTracer

logger = logging.getLogger("celerity.bucket.s3")


class S3ObjectListing(ObjectListing):
    """Async iterator over S3 ListObjectsV2 pages.

    Lazily fetches pages using ContinuationToken for cursor-based
    pagination. Each page yields individual ObjectInfo items.
    """

    def __init__(
        self,
        client_provider: Callable[[], Awaitable[S3Client]],
        bucket_name: str,
        prefix: str | None,
        max_keys: int | None,
        cursor: str | None,
        tracer: CelerityTracer | None,
    ) -> None:
        self._client_provider = client_provider
        self._bucket_name = bucket_name
        self._prefix = prefix
        self._max_keys = max_keys
        self._continuation_token = _decode_cursor(cursor) if cursor else None
        self._tracer = tracer
        self._done = False
        self._buffer: list[ObjectInfo] = []
        self._buffer_index = 0

    def __aiter__(self) -> AsyncIterator[ObjectInfo]:
        return self

    async def __anext__(self) -> ObjectInfo:
        if self._buffer_index < len(self._buffer):
            item = self._buffer[self._buffer_index]
            self._buffer_index += 1
            return item

        if self._done:
            raise StopAsyncIteration

        await self._fetch_page()

        if not self._buffer:
            raise StopAsyncIteration

        self._buffer_index = 1
        return self._buffer[0]

    @property
    def cursor(self) -> str | None:
        """Base64-encoded continuation token for resuming pagination."""
        if self._done:
            return None
        if self._continuation_token:
            return _encode_cursor(self._continuation_token)
        return None

    async def _fetch_page(self) -> None:
        """Call S3 ListObjectsV2 and populate the buffer."""
        if self._tracer:
            await self._tracer.with_span(
                "celerity.bucket.list_page",
                lambda _span: self._do_fetch_page(),
                attributes={
                    "bucket.name": self._bucket_name,
                    "bucket.prefix": self._prefix or "",
                },
            )
        else:
            await self._do_fetch_page()

    async def _do_fetch_page(self) -> None:
        """Execute the actual ListObjectsV2 call."""
        logger.debug("list_page %s prefix=%s", self._bucket_name, self._prefix)
        params: dict[str, Any] = {"Bucket": self._bucket_name}
        if self._prefix is not None:
            params["Prefix"] = self._prefix
        if self._max_keys is not None:
            params["MaxKeys"] = self._max_keys
        if self._continuation_token is not None:
            params["ContinuationToken"] = self._continuation_token

        try:
            client = await self._client_provider()
            response = await client.list_objects_v2(**params)
        except Exception as exc:
            raise BucketError(f"S3 list_objects_v2 failed: {exc}", cause=exc) from exc

        contents = response.get("Contents", [])
        self._buffer = [_parse_list_entry(entry) for entry in contents]
        self._buffer_index = 0

        self._continuation_token = response.get("NextContinuationToken")
        if not response.get("IsTruncated", False):
            self._done = True


def _encode_cursor(token: str) -> str:
    """Base64-encode an S3 ContinuationToken for use as an opaque cursor."""
    return base64.urlsafe_b64encode(token.encode()).decode()


def _decode_cursor(cursor: str) -> str:
    """Decode a base64 cursor back to an S3 ContinuationToken."""
    return base64.urlsafe_b64decode(cursor.encode()).decode()


def _parse_list_entry(entry: Any) -> ObjectInfo:
    """Map an S3 Contents entry to ObjectInfo."""
    return ObjectInfo(
        key=entry["Key"],
        size=entry.get("Size", 0),
        content_type="",
        last_modified=entry.get("LastModified") or datetime.now(tz=UTC),
        etag=entry.get("ETag", "").strip('"'),
        metadata={},
    )
