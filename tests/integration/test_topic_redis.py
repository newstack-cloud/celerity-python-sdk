"""Integration tests for RedisTopic against Docker Valkey."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from redis.asyncio import Redis

from celerity.resources.topic.providers.redis.client import RedisTopic
from celerity.resources.topic.types import (
    BatchPublishEntry,
    PublishOptions,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

CHANNEL = "celerity:topic:channel:inttest"


@pytest.fixture
async def redis_client() -> AsyncGenerator[Redis[bytes]]:
    """Standalone async Redis client for Valkey on port 6399."""
    client: Redis[bytes] = Redis(host="localhost", port=6399, decode_responses=False)
    yield client
    await client.close()


@pytest.fixture
async def topic(redis_client: Redis[bytes]) -> RedisTopic:
    """A RedisTopic instance for integration testing."""
    return RedisTopic(client=redis_client, channel=CHANNEL)


class TestPublish:
    @pytest.mark.asyncio
    async def test_publish_received_by_subscriber(
        self, topic: RedisTopic, redis_client: Redis[bytes]
    ) -> None:
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL)
        # Consume the subscription confirmation message
        await pubsub.get_message(timeout=1)

        await topic.publish("Hello, Redis Pub/Sub!")

        msg = await pubsub.get_message(timeout=3)
        assert msg is not None
        assert msg["type"] == "message"
        envelope = json.loads(msg["data"])
        assert envelope["body"] == "Hello, Redis Pub/Sub!"
        assert "messageId" in envelope

        await pubsub.unsubscribe(CHANNEL)
        await pubsub.aclose()

    @pytest.mark.asyncio
    async def test_publish_envelope_has_valid_uuid(
        self, topic: RedisTopic, redis_client: Redis[bytes]
    ) -> None:
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL)
        await pubsub.get_message(timeout=1)

        msg_id = await topic.publish("uuid-check")

        msg = await pubsub.get_message(timeout=3)
        assert msg is not None
        envelope = json.loads(msg["data"])
        assert envelope["messageId"] == msg_id
        assert len(msg_id) == 36

        await pubsub.unsubscribe(CHANNEL)
        await pubsub.aclose()

    @pytest.mark.asyncio
    async def test_publish_with_subject(
        self, topic: RedisTopic, redis_client: Redis[bytes]
    ) -> None:
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL)
        await pubsub.get_message(timeout=1)

        await topic.publish("with-subject", PublishOptions(subject="Greetings"))

        msg = await pubsub.get_message(timeout=3)
        assert msg is not None
        envelope = json.loads(msg["data"])
        assert envelope["subject"] == "Greetings"

        await pubsub.unsubscribe(CHANNEL)
        await pubsub.aclose()

    @pytest.mark.asyncio
    async def test_publish_with_attributes(
        self, topic: RedisTopic, redis_client: Redis[bytes]
    ) -> None:
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL)
        await pubsub.get_message(timeout=1)

        await topic.publish(
            "with-attrs", PublishOptions(attributes={"env": "test", "tier": "free"})
        )

        msg = await pubsub.get_message(timeout=3)
        assert msg is not None
        envelope = json.loads(msg["data"])
        assert envelope["attributes"] == {"env": "test", "tier": "free"}

        await pubsub.unsubscribe(CHANNEL)
        await pubsub.aclose()


class TestPublishBatch:
    @pytest.mark.asyncio
    async def test_batch_all_received(self, topic: RedisTopic, redis_client: Redis[bytes]) -> None:
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL)
        await pubsub.get_message(timeout=1)

        entries = [BatchPublishEntry(id=f"e{i}", body=f"batch-{i}") for i in range(3)]
        result = await topic.publish_batch(entries)
        assert len(result.successful) == 3
        assert result.failed == []

        # Collect all messages
        received: list[dict[str, str]] = []
        for _ in range(3):
            msg = await pubsub.get_message(timeout=3)
            if msg and msg["type"] == "message":
                received.append(json.loads(msg["data"]))

        assert len(received) == 3
        bodies = {e["body"] for e in received}
        assert bodies == {"batch-0", "batch-1", "batch-2"}

        await pubsub.unsubscribe(CHANNEL)
        await pubsub.aclose()

    @pytest.mark.asyncio
    async def test_batch_unique_message_ids(
        self, topic: RedisTopic, redis_client: Redis[bytes]
    ) -> None:
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(CHANNEL)
        await pubsub.get_message(timeout=1)

        entries = [BatchPublishEntry(id=f"e{i}", body=f"unique-{i}") for i in range(3)]
        result = await topic.publish_batch(entries)
        ids = {s.message_id for s in result.successful}
        assert len(ids) == 3

        # Drain messages
        for _ in range(3):
            await pubsub.get_message(timeout=1)

        await pubsub.unsubscribe(CHANNEL)
        await pubsub.aclose()
