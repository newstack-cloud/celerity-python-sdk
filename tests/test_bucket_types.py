"""Tests for bucket types, errors, and option dataclasses."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

from celerity.resources.bucket.errors import BucketError
from celerity.resources.bucket.types import (
    CopyObjectOptions,
    GetObjectOptions,
    GetObjectResult,
    ListObjectsOptions,
    ObjectInfo,
    PutObjectOptions,
    SignUrlOptions,
)


class TestBucketError:
    def test_message(self) -> None:
        err = BucketError("something went wrong")
        assert str(err) == "something went wrong"
        assert err.resource is None
        assert err.__cause__ is None

    def test_resource(self) -> None:
        err = BucketError("fail", resource="my-bucket")
        assert err.resource == "my-bucket"

    def test_cause(self) -> None:
        cause = RuntimeError("original")
        err = BucketError("wrapped", cause=cause)
        assert err.__cause__ is cause


class TestObjectInfo:
    def test_construction(self) -> None:
        now = datetime.now(tz=UTC)
        info = ObjectInfo(
            key="test.txt",
            size=100,
            content_type="text/plain",
            last_modified=now,
            etag="abc123",
            metadata={"tag": "value"},
        )
        assert info.key == "test.txt"
        assert info.size == 100
        assert info.content_type == "text/plain"
        assert info.last_modified == now
        assert info.etag == "abc123"
        assert info.metadata == {"tag": "value"}

    def test_frozen(self) -> None:
        now = datetime.now(tz=UTC)
        info = ObjectInfo(key="k", size=0, content_type="", last_modified=now, etag="", metadata={})
        with pytest.raises(AttributeError):
            info.key = "changed"  # type: ignore[misc]


class TestGetObjectResult:
    @pytest.mark.asyncio
    async def test_read(self) -> None:
        async def body() -> AsyncGenerator[bytes]:
            yield b"hello "
            yield b"world"

        now = datetime.now(tz=UTC)
        info = ObjectInfo(
            key="k",
            size=11,
            content_type="text/plain",
            last_modified=now,
            etag="e",
            metadata={},
        )
        result = GetObjectResult(body=body(), info=info)
        data = await result.read()
        assert data == b"hello world"

    @pytest.mark.asyncio
    async def test_read_text(self) -> None:
        async def body() -> AsyncGenerator[bytes]:
            yield "héllo".encode()

        now = datetime.now(tz=UTC)
        info = ObjectInfo(
            key="k",
            size=6,
            content_type="text/plain",
            last_modified=now,
            etag="e",
            metadata={},
        )
        result = GetObjectResult(body=body(), info=info)
        text = await result.read_text()
        assert text == "héllo"


class TestGetObjectOptions:
    def test_defaults(self) -> None:
        opts = GetObjectOptions()
        assert opts.range_start is None
        assert opts.range_end is None

    def test_range(self) -> None:
        opts = GetObjectOptions(range_start=10, range_end=20)
        assert opts.range_start == 10
        assert opts.range_end == 20


class TestPutObjectOptions:
    def test_defaults(self) -> None:
        opts = PutObjectOptions()
        assert opts.content_type is None
        assert opts.metadata is None

    def test_with_values(self) -> None:
        opts = PutObjectOptions(content_type="image/png", metadata={"owner": "me"})
        assert opts.content_type == "image/png"
        assert opts.metadata == {"owner": "me"}


class TestListObjectsOptions:
    def test_defaults(self) -> None:
        opts = ListObjectsOptions()
        assert opts.prefix is None
        assert opts.max_keys is None
        assert opts.cursor is None

    def test_with_values(self) -> None:
        opts = ListObjectsOptions(prefix="images/", max_keys=10, cursor="abc")
        assert opts.prefix == "images/"
        assert opts.max_keys == 10
        assert opts.cursor == "abc"


class TestCopyObjectOptions:
    def test_defaults(self) -> None:
        opts = CopyObjectOptions()
        assert opts.dest_bucket is None
        assert opts.metadata is None
        assert opts.content_type is None

    def test_with_values(self) -> None:
        opts = CopyObjectOptions(
            dest_bucket="other", metadata={"k": "v"}, content_type="text/plain"
        )
        assert opts.dest_bucket == "other"
        assert opts.metadata == {"k": "v"}
        assert opts.content_type == "text/plain"


class TestSignUrlOptions:
    def test_defaults(self) -> None:
        opts = SignUrlOptions()
        assert opts.expires_in == 3600
        assert opts.method == "get"

    def test_custom(self) -> None:
        opts = SignUrlOptions(expires_in=600, method="put")
        assert opts.expires_in == 600
        assert opts.method == "put"
