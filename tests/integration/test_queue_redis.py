"""Integration tests for RedisQueue against Docker Valkey."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from redis.asyncio import Redis

from celerity.resources.queue.providers.redis.client import RedisQueue
from celerity.resources.queue.types import (
    BatchSendEntry,
    SendMessageOptions,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

STREAM_PREFIX = "celerity:queue:inttest"


@pytest.fixture
async def redis_client() -> AsyncGenerator[Redis[bytes]]:
    """Standalone async Redis client for Valkey on port 6399."""
    client: Redis[bytes] = Redis(host="localhost", port=6399, decode_responses=False)
    yield client
    await client.close()


@pytest.fixture
async def queue(redis_client: Redis[bytes]) -> RedisQueue:
    """A RedisQueue instance for integration testing."""
    return RedisQueue(client=redis_client, stream_key=f"{STREAM_PREFIX}:test")


@pytest.fixture(autouse=True)
async def cleanup(redis_client: Redis[bytes]) -> AsyncGenerator[None]:
    """Clean test streams before and after each test."""
    await _flush_test_streams(redis_client)
    yield
    await _flush_test_streams(redis_client)


async def _flush_test_streams(client: Redis[bytes]) -> None:
    """Delete all streams with the test prefix."""
    keys: list[bytes] = []
    async for key in client.scan_iter(match=f"{STREAM_PREFIX}:*".encode()):
        keys.append(key)
    if keys:
        await client.delete(*keys)


class TestSendMessage:
    @pytest.mark.asyncio
    async def test_send_and_verify(self, queue: RedisQueue, redis_client: Redis[bytes]) -> None:
        msg_id = await queue.send_message("Hello, Redis Streams!")
        assert msg_id

        entries = await redis_client.xrange(f"{STREAM_PREFIX}:test")
        assert len(entries) == 1
        _id, fields = entries[0]
        assert fields[b"body"] == b"Hello, Redis Streams!"

    @pytest.mark.asyncio
    async def test_stream_fields_always_present(
        self, queue: RedisQueue, redis_client: Redis[bytes]
    ) -> None:
        await queue.send_message("test-body")
        entries = await redis_client.xrange(f"{STREAM_PREFIX}:test")
        _, fields = entries[0]
        assert b"body" in fields
        assert b"timestamp" in fields
        assert b"message_type" in fields
        assert fields[b"message_type"] == b"0"

    @pytest.mark.asyncio
    async def test_group_id_and_dedup_id(
        self, queue: RedisQueue, redis_client: Redis[bytes]
    ) -> None:
        await queue.send_message(
            "fifo-msg",
            SendMessageOptions(group_id="grp-1", deduplication_id="dup-1"),
        )
        entries = await redis_client.xrange(f"{STREAM_PREFIX}:test")
        _, fields = entries[0]
        assert fields[b"group_id"] == b"grp-1"
        assert fields[b"dedup_id"] == b"dup-1"

    @pytest.mark.asyncio
    async def test_attributes_json(self, queue: RedisQueue, redis_client: Redis[bytes]) -> None:
        await queue.send_message(
            "attr-msg",
            SendMessageOptions(attributes={"env": "test", "tier": "free"}),
        )
        entries = await redis_client.xrange(f"{STREAM_PREFIX}:test")
        _, fields = entries[0]
        parsed = json.loads(fields[b"attributes"])
        assert parsed == {"env": "test", "tier": "free"}

    @pytest.mark.asyncio
    async def test_optional_fields_absent(
        self, queue: RedisQueue, redis_client: Redis[bytes]
    ) -> None:
        await queue.send_message("plain-msg")
        entries = await redis_client.xrange(f"{STREAM_PREFIX}:test")
        _, fields = entries[0]
        assert b"group_id" not in fields
        assert b"dedup_id" not in fields
        assert b"attributes" not in fields

    @pytest.mark.asyncio
    async def test_stream_key_format(self, redis_client: Redis[bytes]) -> None:
        q = RedisQueue(client=redis_client, stream_key=f"{STREAM_PREFIX}:custom-name")
        await q.send_message("verify-key")
        entries = await redis_client.xrange(f"{STREAM_PREFIX}:custom-name")
        assert len(entries) == 1


class TestSendMessageBatch:
    @pytest.mark.asyncio
    async def test_batch_all_in_stream(self, queue: RedisQueue, redis_client: Redis[bytes]) -> None:
        entries = [BatchSendEntry(id=f"e{i}", body=f"batch-{i}") for i in range(5)]
        result = await queue.send_message_batch(entries)
        assert len(result.successful) == 5
        assert result.failed == []

        stream_entries = await redis_client.xrange(f"{STREAM_PREFIX}:test")
        assert len(stream_entries) == 5

    @pytest.mark.asyncio
    async def test_batch_count_matches(self, queue: RedisQueue, redis_client: Redis[bytes]) -> None:
        entries = [BatchSendEntry(id=f"e{i}", body=f"count-{i}") for i in range(8)]
        await queue.send_message_batch(entries)

        stream_entries = await redis_client.xrange(f"{STREAM_PREFIX}:test")
        assert len(stream_entries) == 8

    @pytest.mark.asyncio
    async def test_batch_entry_fields(self, queue: RedisQueue, redis_client: Redis[bytes]) -> None:
        entries = [
            BatchSendEntry(id="e1", body="msg1", group_id="g1"),
            BatchSendEntry(id="e2", body="msg2"),
        ]
        await queue.send_message_batch(entries)

        stream_entries = await redis_client.xrange(f"{STREAM_PREFIX}:test")
        _, first_fields = stream_entries[0]
        assert first_fields[b"body"] == b"msg1"
        assert first_fields[b"group_id"] == b"g1"

        _, second_fields = stream_entries[1]
        assert second_fields[b"body"] == b"msg2"
        assert b"group_id" not in second_fields
