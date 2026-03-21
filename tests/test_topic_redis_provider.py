"""Tests for RedisTopic provider — envelope, error wrapping, and instrumentation."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

import pytest

from celerity.resources.topic.errors import TopicError
from celerity.resources.topic.providers.redis.client import RedisTopic
from celerity.resources.topic.types import (
    BatchPublishEntry,
    PublishOptions,
)


@pytest.fixture
def mock_redis() -> AsyncMock:
    client = AsyncMock()
    client.publish = AsyncMock(return_value=1)
    mock_pipe = AsyncMock()
    mock_pipe.publish = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[1, 1])
    client.pipeline = MagicMock(return_value=mock_pipe)
    return client


@pytest.fixture
def topic(mock_redis: AsyncMock) -> RedisTopic:
    return RedisTopic(client=mock_redis, channel="celerity:topic:channel:test-t")


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
def traced_topic(mock_redis: AsyncMock, mock_tracer: AsyncMock) -> RedisTopic:
    return RedisTopic(
        client=mock_redis,
        channel="celerity:topic:channel:test-t",
        tracer=mock_tracer,
    )


class TestPublish:
    @pytest.mark.asyncio
    async def test_calls_publish_on_correct_channel(
        self, topic: RedisTopic, mock_redis: AsyncMock
    ) -> None:
        await topic.publish("hello")
        mock_redis.publish.assert_awaited_once()
        assert mock_redis.publish.call_args[0][0] == "celerity:topic:channel:test-t"

    @pytest.mark.asyncio
    async def test_envelope_contains_body_and_message_id(
        self, topic: RedisTopic, mock_redis: AsyncMock
    ) -> None:
        await topic.publish("hello")
        raw = mock_redis.publish.call_args[0][1]
        envelope = json.loads(raw)
        assert envelope["body"] == "hello"
        assert "messageId" in envelope
        assert len(envelope["messageId"]) == 36  # UUID4

    @pytest.mark.asyncio
    async def test_envelope_with_subject(self, topic: RedisTopic, mock_redis: AsyncMock) -> None:
        await topic.publish("hello", PublishOptions(subject="Hi"))
        envelope = json.loads(mock_redis.publish.call_args[0][1])
        assert envelope["subject"] == "Hi"

    @pytest.mark.asyncio
    async def test_envelope_with_attributes(self, topic: RedisTopic, mock_redis: AsyncMock) -> None:
        await topic.publish("hello", PublishOptions(attributes={"env": "prod"}))
        envelope = json.loads(mock_redis.publish.call_args[0][1])
        assert envelope["attributes"] == {"env": "prod"}

    @pytest.mark.asyncio
    async def test_envelope_without_options(self, topic: RedisTopic, mock_redis: AsyncMock) -> None:
        await topic.publish("hello")
        envelope = json.loads(mock_redis.publish.call_args[0][1])
        assert "subject" not in envelope
        assert "attributes" not in envelope

    @pytest.mark.asyncio
    async def test_ignores_group_id(self, topic: RedisTopic, mock_redis: AsyncMock) -> None:
        await topic.publish("hello", PublishOptions(group_id="grp"))
        envelope = json.loads(mock_redis.publish.call_args[0][1])
        assert "group_id" not in envelope

    @pytest.mark.asyncio
    async def test_ignores_deduplication_id(self, topic: RedisTopic, mock_redis: AsyncMock) -> None:
        await topic.publish("hello", PublishOptions(deduplication_id="dup"))
        envelope = json.loads(mock_redis.publish.call_args[0][1])
        assert "deduplication_id" not in envelope

    @pytest.mark.asyncio
    async def test_returns_message_id(self, topic: RedisTopic, mock_redis: AsyncMock) -> None:
        result = await topic.publish("hello")
        assert len(result) == 36  # UUID4


class TestPublishBatch:
    @pytest.mark.asyncio
    async def test_uses_pipeline(self, topic: RedisTopic, mock_redis: AsyncMock) -> None:
        entries = [
            BatchPublishEntry(id="e1", body="msg1"),
            BatchPublishEntry(id="e2", body="msg2"),
        ]
        await topic.publish_batch(entries)
        mock_redis.pipeline.assert_called_once()
        pipe = mock_redis.pipeline.return_value
        assert pipe.publish.call_count == 2
        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_each_entry_gets_unique_message_id(
        self, topic: RedisTopic, mock_redis: AsyncMock
    ) -> None:
        entries = [
            BatchPublishEntry(id="e1", body="msg1"),
            BatchPublishEntry(id="e2", body="msg2"),
        ]
        result = await topic.publish_batch(entries)
        ids = {s.message_id for s in result.successful}
        assert len(ids) == 2  # all unique

    @pytest.mark.asyncio
    async def test_returns_all_successful(self, topic: RedisTopic, mock_redis: AsyncMock) -> None:
        entries = [
            BatchPublishEntry(id="e1", body="msg1"),
            BatchPublishEntry(id="e2", body="msg2"),
        ]
        result = await topic.publish_batch(entries)
        assert len(result.successful) == 2
        assert result.successful[0].id == "e1"
        assert result.successful[1].id == "e2"
        assert result.failed == []


class TestErrorWrapping:
    @pytest.mark.asyncio
    async def test_publish(self, topic: RedisTopic, mock_redis: AsyncMock) -> None:
        mock_redis.publish.side_effect = Exception("redis down")
        with pytest.raises(TopicError, match="PUBLISH"):
            await topic.publish("hello")

    @pytest.mark.asyncio
    async def test_publish_batch(self, topic: RedisTopic, mock_redis: AsyncMock) -> None:
        pipe = mock_redis.pipeline.return_value
        pipe.execute.side_effect = Exception("pipeline fail")
        with pytest.raises(TopicError, match="pipeline"):
            await topic.publish_batch([BatchPublishEntry(id="e1", body="hi")])


class TestTracerSpans:
    @pytest.mark.asyncio
    async def test_publish(
        self, traced_topic: RedisTopic, mock_redis: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        await traced_topic.publish("hello")
        mock_tracer.with_span.assert_awaited_once()
        call_args = mock_tracer.with_span.call_args
        assert call_args[0][0] == "celerity.topic.publish"
        assert call_args[1]["attributes"]["topic.channel"] == "celerity:topic:channel:test-t"

    @pytest.mark.asyncio
    async def test_publish_batch(
        self, traced_topic: RedisTopic, mock_redis: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        entries = [BatchPublishEntry(id="e1", body="msg1")]
        await traced_topic.publish_batch(entries)
        mock_tracer.with_span.assert_awaited_once()
        call_args = mock_tracer.with_span.call_args
        assert call_args[0][0] == "celerity.topic.publish_batch"
        attrs = call_args[1]["attributes"]
        assert attrs["topic.channel"] == "celerity:topic:channel:test-t"
        assert attrs["topic.batch_size"] == 1

    @pytest.mark.asyncio
    async def test_no_tracer(self, topic: RedisTopic, mock_redis: AsyncMock) -> None:
        result = await topic.publish("hello")
        assert len(result) == 36  # UUID4
