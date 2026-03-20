"""Tests for SQSQueue provider — error wrapping, instrumentation, and wire-level params.

Happy-path behavior is covered by integration/test_queue_sqs.py.
This file tests what integration tests cannot: error wrapping into QueueError,
tracer span creation with correct names/attributes, and exact SQS API parameter
formatting (MessageGroupId, MessageDeduplicationId, MessageAttributes, batch chunking).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

import pytest

from celerity.resources.queue.errors import QueueError
from celerity.resources.queue.providers.sqs.client import SQSQueue
from celerity.resources.queue.types import (
    BatchSendEntry,
    SendMessageOptions,
)


@pytest.fixture
def mock_client() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def queue(mock_client: AsyncMock) -> SQSQueue:
    async def provider() -> AsyncMock:
        return mock_client

    return SQSQueue(
        client_provider=provider, queue_url="https://sqs.us-east-1.amazonaws.com/123/test-queue"
    )


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
def traced_queue(mock_client: AsyncMock, mock_tracer: AsyncMock) -> SQSQueue:
    async def provider() -> AsyncMock:
        return mock_client

    return SQSQueue(
        client_provider=provider,
        queue_url="https://sqs.us-east-1.amazonaws.com/123/test-queue",
        tracer=mock_tracer,
    )


class TestErrorWrapping:
    @pytest.mark.asyncio
    async def test_send_message(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        mock_client.send_message.side_effect = Exception("boom")
        with pytest.raises(QueueError, match="send_message"):
            await queue.send_message("hello")

    @pytest.mark.asyncio
    async def test_send_message_cause(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        cause = RuntimeError("original")
        mock_client.send_message.side_effect = cause
        with pytest.raises(QueueError) as exc_info:
            await queue.send_message("hello")
        assert exc_info.value.__cause__ is cause

    @pytest.mark.asyncio
    async def test_send_message_batch(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        mock_client.send_message_batch.side_effect = Exception("batch boom")
        with pytest.raises(QueueError, match="send_message_batch"):
            await queue.send_message_batch([BatchSendEntry(id="e1", body="hi")])


class TestTracerSpans:
    @pytest.mark.asyncio
    async def test_send_message(
        self, traced_queue: SQSQueue, mock_client: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        mock_client.send_message.return_value = {"MessageId": "msg-1"}
        await traced_queue.send_message("hello")
        mock_tracer.with_span.assert_awaited_once()
        call_args = mock_tracer.with_span.call_args
        assert call_args[0][0] == "celerity.queue.send_message"
        assert call_args[1]["attributes"]["queue.url"] == (
            "https://sqs.us-east-1.amazonaws.com/123/test-queue"
        )

    @pytest.mark.asyncio
    async def test_send_message_batch(
        self, traced_queue: SQSQueue, mock_client: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        mock_client.send_message_batch.return_value = {
            "Successful": [{"Id": "e1", "MessageId": "m1"}],
            "Failed": [],
        }
        await traced_queue.send_message_batch([BatchSendEntry(id="e1", body="hi")])
        mock_tracer.with_span.assert_awaited_once()
        call_args = mock_tracer.with_span.call_args
        assert call_args[0][0] == "celerity.queue.send_message_batch"
        attrs = call_args[1]["attributes"]
        assert attrs["queue.batch_size"] == 1

    @pytest.mark.asyncio
    async def test_no_tracer(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        mock_client.send_message.return_value = {"MessageId": "msg-1"}
        result = await queue.send_message("hello")
        assert result == "msg-1"


class TestWireLevelParams:
    @pytest.mark.asyncio
    async def test_group_id(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        mock_client.send_message.return_value = {"MessageId": "m1"}
        await queue.send_message("body", SendMessageOptions(group_id="grp"))
        kwargs = mock_client.send_message.call_args.kwargs
        assert kwargs["MessageGroupId"] == "grp"

    @pytest.mark.asyncio
    async def test_deduplication_id(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        mock_client.send_message.return_value = {"MessageId": "m1"}
        await queue.send_message("body", SendMessageOptions(deduplication_id="dup1"))
        kwargs = mock_client.send_message.call_args.kwargs
        assert kwargs["MessageDeduplicationId"] == "dup1"

    @pytest.mark.asyncio
    async def test_delay_seconds(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        mock_client.send_message.return_value = {"MessageId": "m1"}
        await queue.send_message("body", SendMessageOptions(delay_seconds=30))
        kwargs = mock_client.send_message.call_args.kwargs
        assert kwargs["DelaySeconds"] == 30

    @pytest.mark.asyncio
    async def test_message_attributes(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        mock_client.send_message.return_value = {"MessageId": "m1"}
        await queue.send_message(
            "body", SendMessageOptions(attributes={"env": "prod", "priority": "high"})
        )
        kwargs = mock_client.send_message.call_args.kwargs
        attrs = kwargs["MessageAttributes"]
        assert attrs["env"] == {"DataType": "String", "StringValue": "prod"}
        assert attrs["priority"] == {"DataType": "String", "StringValue": "high"}

    @pytest.mark.asyncio
    async def test_optional_params_omitted(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        mock_client.send_message.return_value = {"MessageId": "m1"}
        await queue.send_message("body")
        kwargs = mock_client.send_message.call_args.kwargs
        assert "MessageGroupId" not in kwargs
        assert "MessageDeduplicationId" not in kwargs
        assert "DelaySeconds" not in kwargs
        assert "MessageAttributes" not in kwargs


class TestBatchChunking:
    @pytest.mark.asyncio
    async def test_auto_chunks_over_10(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        mock_client.send_message_batch.return_value = {
            "Successful": [],
            "Failed": [],
        }
        entries = [BatchSendEntry(id=f"e{i}", body=f"msg-{i}") for i in range(25)]
        await queue.send_message_batch(entries)
        assert mock_client.send_message_batch.call_count == 3

    @pytest.mark.asyncio
    async def test_chunk_sizes(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        mock_client.send_message_batch.return_value = {
            "Successful": [],
            "Failed": [],
        }
        entries = [BatchSendEntry(id=f"e{i}", body=f"msg-{i}") for i in range(25)]
        await queue.send_message_batch(entries)
        calls = mock_client.send_message_batch.call_args_list
        assert len(calls[0].kwargs["Entries"]) == 10
        assert len(calls[1].kwargs["Entries"]) == 10
        assert len(calls[2].kwargs["Entries"]) == 5

    @pytest.mark.asyncio
    async def test_collects_partial_failures_across_chunks(
        self, queue: SQSQueue, mock_client: AsyncMock
    ) -> None:
        mock_client.send_message_batch.side_effect = [
            {
                "Successful": [{"Id": "e0", "MessageId": "m0"}],
                "Failed": [{"Id": "e1", "Code": "Err", "Message": "fail", "SenderFault": True}],
            },
            {
                "Successful": [{"Id": "e10", "MessageId": "m10"}],
                "Failed": [],
            },
        ]
        entries = [BatchSendEntry(id=f"e{i}", body=f"msg-{i}") for i in range(12)]
        result = await queue.send_message_batch(entries)
        assert len(result.successful) == 2
        assert len(result.failed) == 1
        assert result.failed[0].id == "e1"
        assert result.failed[0].sender_fault is True

    @pytest.mark.asyncio
    async def test_batch_entry_params(self, queue: SQSQueue, mock_client: AsyncMock) -> None:
        mock_client.send_message_batch.return_value = {
            "Successful": [{"Id": "e1", "MessageId": "m1"}],
            "Failed": [],
        }
        entry = BatchSendEntry(
            id="e1",
            body="hello",
            group_id="grp",
            deduplication_id="dup",
            delay_seconds=5,
            attributes={"k": "v"},
        )
        await queue.send_message_batch([entry])
        sqs_entry = mock_client.send_message_batch.call_args.kwargs["Entries"][0]
        assert sqs_entry["MessageGroupId"] == "grp"
        assert sqs_entry["MessageDeduplicationId"] == "dup"
        assert sqs_entry["DelaySeconds"] == 5
        assert sqs_entry["MessageAttributes"]["k"] == {
            "DataType": "String",
            "StringValue": "v",
        }
