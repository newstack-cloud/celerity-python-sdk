"""Tests for RedisQueue provider — stream fields, error wrapping, and instrumentation.

Happy-path behavior is covered by integration/test_queue_redis.py.
This file tests XADD field construction, pipeline usage, error wrapping,
and tracer span creation.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

import pytest

from celerity.resources.queue.errors import QueueError
from celerity.resources.queue.providers.redis.client import RedisQueue
from celerity.resources.queue.types import (
    BatchSendEntry,
    SendMessageOptions,
)


@pytest.fixture
def mock_redis() -> AsyncMock:
    client = AsyncMock()
    client.xadd = AsyncMock(return_value=b"1234567890-0")
    mock_pipe = AsyncMock()
    mock_pipe.xadd = MagicMock()
    mock_pipe.execute = AsyncMock(return_value=[b"1234567890-0", b"1234567890-1"])
    client.pipeline = MagicMock(return_value=mock_pipe)
    return client


@pytest.fixture
def queue(mock_redis: AsyncMock) -> RedisQueue:
    return RedisQueue(client=mock_redis, stream_key="celerity:queue:test-q")


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
def traced_queue(mock_redis: AsyncMock, mock_tracer: AsyncMock) -> RedisQueue:
    return RedisQueue(
        client=mock_redis,
        stream_key="celerity:queue:test-q",
        tracer=mock_tracer,
    )


class TestSendMessage:
    @pytest.mark.asyncio
    async def test_calls_xadd_on_correct_key(
        self, queue: RedisQueue, mock_redis: AsyncMock
    ) -> None:
        await queue.send_message("hello")
        mock_redis.xadd.assert_awaited_once()
        assert mock_redis.xadd.call_args[0][0] == "celerity:queue:test-q"

    @pytest.mark.asyncio
    async def test_stream_fields_include_required(
        self, queue: RedisQueue, mock_redis: AsyncMock
    ) -> None:
        await queue.send_message("hello")
        fields = mock_redis.xadd.call_args[0][1]
        assert fields["body"] == "hello"
        assert "timestamp" in fields
        assert fields["message_type"] == "0"

    @pytest.mark.asyncio
    async def test_includes_group_id(self, queue: RedisQueue, mock_redis: AsyncMock) -> None:
        await queue.send_message("hello", SendMessageOptions(group_id="grp-1"))
        fields = mock_redis.xadd.call_args[0][1]
        assert fields["group_id"] == "grp-1"

    @pytest.mark.asyncio
    async def test_includes_dedup_id(self, queue: RedisQueue, mock_redis: AsyncMock) -> None:
        await queue.send_message("hello", SendMessageOptions(deduplication_id="dup-1"))
        fields = mock_redis.xadd.call_args[0][1]
        assert fields["dedup_id"] == "dup-1"

    @pytest.mark.asyncio
    async def test_includes_json_attributes(self, queue: RedisQueue, mock_redis: AsyncMock) -> None:
        await queue.send_message("hello", SendMessageOptions(attributes={"env": "prod"}))
        fields = mock_redis.xadd.call_args[0][1]
        assert json.loads(fields["attributes"]) == {"env": "prod"}

    @pytest.mark.asyncio
    async def test_omits_optional_fields(self, queue: RedisQueue, mock_redis: AsyncMock) -> None:
        await queue.send_message("hello")
        fields = mock_redis.xadd.call_args[0][1]
        assert "group_id" not in fields
        assert "dedup_id" not in fields
        assert "attributes" not in fields

    @pytest.mark.asyncio
    async def test_returns_stream_entry_id(self, queue: RedisQueue, mock_redis: AsyncMock) -> None:
        mock_redis.xadd.return_value = b"9999999999-5"
        result = await queue.send_message("hello")
        assert result == "9999999999-5"


class TestSendMessageBatch:
    @pytest.mark.asyncio
    async def test_uses_pipeline(self, queue: RedisQueue, mock_redis: AsyncMock) -> None:
        entries = [
            BatchSendEntry(id="e1", body="msg1"),
            BatchSendEntry(id="e2", body="msg2"),
        ]
        await queue.send_message_batch(entries)
        mock_redis.pipeline.assert_called_once()
        pipe = mock_redis.pipeline.return_value
        assert pipe.xadd.call_count == 2
        pipe.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_correct_stream_fields_per_entry(
        self, queue: RedisQueue, mock_redis: AsyncMock
    ) -> None:
        entries = [
            BatchSendEntry(id="e1", body="msg1", group_id="g1"),
            BatchSendEntry(id="e2", body="msg2"),
        ]
        await queue.send_message_batch(entries)
        pipe = mock_redis.pipeline.return_value
        first_fields = pipe.xadd.call_args_list[0][0][1]
        assert first_fields["body"] == "msg1"
        assert first_fields["group_id"] == "g1"
        second_fields = pipe.xadd.call_args_list[1][0][1]
        assert second_fields["body"] == "msg2"
        assert "group_id" not in second_fields

    @pytest.mark.asyncio
    async def test_returns_all_successful(self, queue: RedisQueue, mock_redis: AsyncMock) -> None:
        entries = [
            BatchSendEntry(id="e1", body="msg1"),
            BatchSendEntry(id="e2", body="msg2"),
        ]
        result = await queue.send_message_batch(entries)
        assert len(result.successful) == 2
        assert result.successful[0].id == "e1"
        assert result.successful[1].id == "e2"
        assert result.failed == []


class TestErrorWrapping:
    @pytest.mark.asyncio
    async def test_send_message(self, queue: RedisQueue, mock_redis: AsyncMock) -> None:
        mock_redis.xadd.side_effect = Exception("redis down")
        with pytest.raises(QueueError, match="XADD"):
            await queue.send_message("hello")

    @pytest.mark.asyncio
    async def test_send_message_batch(self, queue: RedisQueue, mock_redis: AsyncMock) -> None:
        pipe = mock_redis.pipeline.return_value
        pipe.execute.side_effect = Exception("pipeline fail")
        with pytest.raises(QueueError, match="pipeline"):
            await queue.send_message_batch([BatchSendEntry(id="e1", body="hi")])


class TestTracerSpans:
    @pytest.mark.asyncio
    async def test_send_message(
        self, traced_queue: RedisQueue, mock_redis: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        await traced_queue.send_message("hello")
        mock_tracer.with_span.assert_awaited_once()
        call_args = mock_tracer.with_span.call_args
        assert call_args[0][0] == "celerity.queue.send_message"
        assert call_args[1]["attributes"]["queue.stream_key"] == "celerity:queue:test-q"

    @pytest.mark.asyncio
    async def test_send_message_batch(
        self, traced_queue: RedisQueue, mock_redis: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        entries = [BatchSendEntry(id="e1", body="msg1")]
        await traced_queue.send_message_batch(entries)
        mock_tracer.with_span.assert_awaited_once()
        call_args = mock_tracer.with_span.call_args
        assert call_args[0][0] == "celerity.queue.send_message_batch"
        attrs = call_args[1]["attributes"]
        assert attrs["queue.stream_key"] == "celerity:queue:test-q"
        assert attrs["queue.batch_size"] == 1

    @pytest.mark.asyncio
    async def test_no_tracer(self, queue: RedisQueue, mock_redis: AsyncMock) -> None:
        result = await queue.send_message("hello")
        assert result == "1234567890-0"
