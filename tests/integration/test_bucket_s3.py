"""Integration tests for S3 bucket against LocalStack."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import aioboto3
import pytest

from celerity.resources.bucket.providers.s3.client import S3ObjectStorage
from celerity.resources.bucket.providers.s3.types import S3ObjectStorageConfig
from celerity.resources.bucket.types import (
    CopyObjectOptions,
    GetObjectOptions,
    ListObjectsOptions,
    PutObjectOptions,
    SignUrlOptions,
)

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from collections.abc import AsyncGenerator, Generator

    from celerity.resources.bucket.types import Bucket

ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"
TEST_BUCKET = "celerity-test-bucket"
COPY_DEST_BUCKET = "celerity-copy-dest-bucket"


@pytest.fixture(scope="module")
def event_loop() -> Generator[AbstractEventLoop]:
    """Module-scoped event loop for async fixtures."""
    import asyncio

    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def s3_buckets() -> AsyncGenerator[tuple[str, str]]:
    """Create test buckets, seed data, and clean up after."""
    session = aioboto3.Session()
    async with session.client(
        "s3",
        region_name=REGION,
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ) as client:
        # Create buckets (ignore if they already exist)
        for bucket_name in (TEST_BUCKET, COPY_DEST_BUCKET):
            try:  # noqa: SIM105
                await client.create_bucket(Bucket=bucket_name)
            except client.exceptions.BucketAlreadyOwnedByYou:
                pass

        # Seed: hello.txt
        await client.put_object(
            Bucket=TEST_BUCKET,
            Key="hello.txt",
            Body=b"Hello, World!",
            ContentType="text/plain",
            Metadata={"greeting": "hello"},
        )

        # Seed: 15 list-test items
        for i in range(15):
            await client.put_object(
                Bucket=TEST_BUCKET,
                Key=f"list-test/item-{i:02d}.txt",
                Body=f"item-{i}".encode(),
                ContentType="text/plain",
            )

        # Seed: range-test.bin (256 bytes: 0x00..0xFF)
        await client.put_object(
            Bucket=TEST_BUCKET,
            Key="range-test.bin",
            Body=bytes(range(256)),
            ContentType="application/octet-stream",
        )

        yield TEST_BUCKET, COPY_DEST_BUCKET

        # Teardown: delete all objects and buckets
        for bucket_name in (TEST_BUCKET, COPY_DEST_BUCKET):
            try:
                response = await client.list_objects_v2(Bucket=bucket_name)
                for obj in response.get("Contents", []):
                    await client.delete_object(Bucket=bucket_name, Key=obj["Key"])
                await client.delete_bucket(Bucket=bucket_name)
            except Exception:
                pass


@pytest.fixture
async def bucket(s3_buckets: tuple[str, str]) -> AsyncGenerator[Bucket]:
    """Create an S3Bucket handle for the test bucket."""
    config = S3ObjectStorageConfig(
        region=REGION,
        endpoint_url=ENDPOINT_URL,
        force_path_style=True,
    )
    session = aioboto3.Session()
    storage = S3ObjectStorage(session=session, config=config, resource_ids={"test": s3_buckets[0]})
    yield storage.bucket("test")
    await storage.close()


@pytest.fixture
async def dest_bucket(s3_buckets: tuple[str, str]) -> AsyncGenerator[Bucket]:
    """Create an S3Bucket handle for the copy destination bucket."""
    config = S3ObjectStorageConfig(
        region=REGION,
        endpoint_url=ENDPOINT_URL,
        force_path_style=True,
    )
    session = aioboto3.Session()
    storage = S3ObjectStorage(session=session, config=config, resource_ids={"dest": s3_buckets[1]})
    yield storage.bucket("dest")
    await storage.close()


class TestGet:
    @pytest.mark.asyncio
    async def test_data_and_metadata(self, bucket: Bucket) -> None:
        result = await bucket.get("hello.txt")
        assert result is not None
        text = await result.read_text()
        assert text == "Hello, World!"
        assert result.info.content_type == "text/plain"
        assert result.info.metadata.get("greeting") == "hello"

    @pytest.mark.asyncio
    async def test_missing_key(self, bucket: Bucket) -> None:
        result = await bucket.get("nonexistent.txt")
        assert result is None

    @pytest.mark.asyncio
    async def test_range_read(self, bucket: Bucket) -> None:
        result = await bucket.get(
            "range-test.bin",
            GetObjectOptions(range_start=10, range_end=19),
        )
        assert result is not None
        data = await result.read()
        assert len(data) == 10
        assert data == bytes(range(10, 20))


class TestPut:
    @pytest.mark.asyncio
    async def test_string_body(self, bucket: Bucket) -> None:
        await bucket.put(
            "put-test.txt", "test content", PutObjectOptions(content_type="text/plain")
        )
        result = await bucket.get("put-test.txt")
        assert result is not None
        assert await result.read_text() == "test content"

    @pytest.mark.asyncio
    async def test_bytes_body(self, bucket: Bucket) -> None:
        data = b"\x00\x01\x02\x03"
        await bucket.put(
            "put-binary.bin",
            data,
            PutObjectOptions(content_type="application/octet-stream"),
        )
        result = await bucket.get("put-binary.bin")
        assert result is not None
        assert await result.read() == data

    @pytest.mark.asyncio
    async def test_content_type_and_metadata(self, bucket: Bucket) -> None:
        await bucket.put(
            "put-meta.txt",
            "data",
            PutObjectOptions(content_type="text/csv", metadata={"source": "test"}),
        )
        info = await bucket.info("put-meta.txt")
        assert info is not None
        assert info.content_type == "text/csv"
        assert info.metadata.get("source") == "test"


class TestDelete:
    @pytest.mark.asyncio
    async def test_delete(self, bucket: Bucket) -> None:
        await bucket.put("delete-me.txt", "bye")
        await bucket.delete("delete-me.txt")
        result = await bucket.get("delete-me.txt")
        assert result is None


class TestInfo:
    @pytest.mark.asyncio
    async def test_existing(self, bucket: Bucket) -> None:
        info = await bucket.info("hello.txt")
        assert info is not None
        assert info.key == "hello.txt"
        assert info.size == len(b"Hello, World!")
        assert info.content_type == "text/plain"
        assert info.last_modified is not None
        assert info.etag != ""
        assert info.metadata.get("greeting") == "hello"

    @pytest.mark.asyncio
    async def test_missing(self, bucket: Bucket) -> None:
        info = await bucket.info("nonexistent.txt")
        assert info is None


class TestExists:
    @pytest.mark.asyncio
    async def test_true(self, bucket: Bucket) -> None:
        assert await bucket.exists("hello.txt") is True

    @pytest.mark.asyncio
    async def test_false(self, bucket: Bucket) -> None:
        assert await bucket.exists("nonexistent.txt") is False


class TestList:
    @pytest.mark.asyncio
    async def test_full_prefix(self, bucket: Bucket) -> None:
        items = [item async for item in bucket.list(ListObjectsOptions(prefix="list-test/"))]
        assert len(items) == 15

    @pytest.mark.asyncio
    async def test_pagination(self, bucket: Bucket) -> None:
        items = [
            item async for item in bucket.list(ListObjectsOptions(prefix="list-test/", max_keys=5))
        ]
        assert len(items) == 15

    @pytest.mark.asyncio
    async def test_cursor_resume(self, bucket: Bucket) -> None:
        listing = bucket.list(ListObjectsOptions(prefix="list-test/", max_keys=5))
        first_batch: list[Any] = []
        count = 0
        async for item in listing:
            first_batch.append(item)
            count += 1
            if count >= 5:
                break

        cursor = listing.cursor
        assert cursor is not None

        # Resume from cursor
        remaining = [
            item
            async for item in bucket.list(
                ListObjectsOptions(prefix="list-test/", max_keys=5, cursor=cursor)
            )
        ]
        assert len(remaining) > 0

        # No duplicates
        first_keys = {item.key for item in first_batch}
        remaining_keys = {item.key for item in remaining}
        assert first_keys.isdisjoint(remaining_keys)

    @pytest.mark.asyncio
    async def test_empty_prefix(self, bucket: Bucket) -> None:
        items = [
            item async for item in bucket.list(ListObjectsOptions(prefix="nonexistent-prefix/"))
        ]
        assert items == []


class TestCopy:
    @pytest.mark.asyncio
    async def test_same_bucket(self, bucket: Bucket) -> None:
        await bucket.copy("hello.txt", "hello-copy.txt")
        result = await bucket.get("hello-copy.txt")
        assert result is not None
        assert await result.read_text() == "Hello, World!"

    @pytest.mark.asyncio
    async def test_cross_bucket(self, bucket: Bucket, dest_bucket: Bucket) -> None:
        await bucket.copy(
            "hello.txt",
            "hello-cross.txt",
            CopyObjectOptions(dest_bucket=COPY_DEST_BUCKET),
        )
        result = await dest_bucket.get("hello-cross.txt")
        assert result is not None
        assert await result.read_text() == "Hello, World!"

    @pytest.mark.asyncio
    async def test_metadata_override(self, bucket: Bucket) -> None:
        await bucket.copy(
            "hello.txt",
            "hello-meta.txt",
            CopyObjectOptions(metadata={"custom": "value"}),
        )
        info = await bucket.info("hello-meta.txt")
        assert info is not None
        assert info.metadata.get("custom") == "value"

        # Body preserved
        result = await bucket.get("hello-meta.txt")
        assert result is not None
        assert await result.read_text() == "Hello, World!"


class TestSignUrl:
    @pytest.mark.asyncio
    async def test_read(self, bucket: Bucket) -> None:
        url = await bucket.sign_url("hello.txt", SignUrlOptions(method="get"))
        assert "hello.txt" in url
        assert url.startswith("http")

    @pytest.mark.asyncio
    async def test_write(self, bucket: Bucket) -> None:
        url = await bucket.sign_url("signed-upload.txt", SignUrlOptions(method="put"))
        assert "signed-upload.txt" in url
        assert url.startswith("http")
