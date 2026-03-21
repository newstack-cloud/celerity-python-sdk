"""Tests for SNSTopic provider — error wrapping, instrumentation, and wire-level params."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

import pytest

from celerity.resources.topic.errors import TopicError
from celerity.resources.topic.providers.sns.client import SNSTopic
from celerity.resources.topic.types import (
    BatchPublishEntry,
    PublishOptions,
)


@pytest.fixture
def mock_client() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def topic(mock_client: AsyncMock) -> SNSTopic:
    async def provider() -> AsyncMock:
        return mock_client

    return SNSTopic(
        client_provider=provider,
        topic_arn="arn:aws:sns:us-east-1:123456789:test-topic",
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
def traced_topic(mock_client: AsyncMock, mock_tracer: AsyncMock) -> SNSTopic:
    async def provider() -> AsyncMock:
        return mock_client

    return SNSTopic(
        client_provider=provider,
        topic_arn="arn:aws:sns:us-east-1:123456789:test-topic",
        tracer=mock_tracer,
    )


class TestErrorWrapping:
    @pytest.mark.asyncio
    async def test_publish(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        mock_client.publish.side_effect = Exception("boom")
        with pytest.raises(TopicError, match="publish"):
            await topic.publish("hello")

    @pytest.mark.asyncio
    async def test_publish_cause(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        cause = RuntimeError("original")
        mock_client.publish.side_effect = cause
        with pytest.raises(TopicError) as exc_info:
            await topic.publish("hello")
        assert exc_info.value.__cause__ is cause

    @pytest.mark.asyncio
    async def test_publish_batch(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        mock_client.publish_batch.side_effect = Exception("batch boom")
        with pytest.raises(TopicError, match="publish_batch"):
            await topic.publish_batch([BatchPublishEntry(id="e1", body="hi")])


class TestTracerSpans:
    @pytest.mark.asyncio
    async def test_publish(
        self, traced_topic: SNSTopic, mock_client: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        mock_client.publish.return_value = {"MessageId": "msg-1"}
        await traced_topic.publish("hello")
        mock_tracer.with_span.assert_awaited_once()
        call_args = mock_tracer.with_span.call_args
        assert call_args[0][0] == "celerity.topic.publish"
        assert call_args[1]["attributes"]["topic.arn"] == (
            "arn:aws:sns:us-east-1:123456789:test-topic"
        )

    @pytest.mark.asyncio
    async def test_publish_batch(
        self, traced_topic: SNSTopic, mock_client: AsyncMock, mock_tracer: AsyncMock
    ) -> None:
        mock_client.publish_batch.return_value = {
            "Successful": [{"Id": "e1", "MessageId": "m1"}],
            "Failed": [],
        }
        await traced_topic.publish_batch([BatchPublishEntry(id="e1", body="hi")])
        mock_tracer.with_span.assert_awaited_once()
        call_args = mock_tracer.with_span.call_args
        assert call_args[0][0] == "celerity.topic.publish_batch"
        attrs = call_args[1]["attributes"]
        assert attrs["topic.batch_size"] == 1

    @pytest.mark.asyncio
    async def test_no_tracer(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        mock_client.publish.return_value = {"MessageId": "msg-1"}
        result = await topic.publish("hello")
        assert result == "msg-1"


class TestWireLevelParams:
    @pytest.mark.asyncio
    async def test_subject(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        mock_client.publish.return_value = {"MessageId": "m1"}
        await topic.publish("body", PublishOptions(subject="Hello"))
        kwargs = mock_client.publish.call_args.kwargs
        assert kwargs["Subject"] == "Hello"

    @pytest.mark.asyncio
    async def test_group_id(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        mock_client.publish.return_value = {"MessageId": "m1"}
        await topic.publish("body", PublishOptions(group_id="grp"))
        kwargs = mock_client.publish.call_args.kwargs
        assert kwargs["MessageGroupId"] == "grp"

    @pytest.mark.asyncio
    async def test_deduplication_id(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        mock_client.publish.return_value = {"MessageId": "m1"}
        await topic.publish("body", PublishOptions(deduplication_id="dup1"))
        kwargs = mock_client.publish.call_args.kwargs
        assert kwargs["MessageDeduplicationId"] == "dup1"

    @pytest.mark.asyncio
    async def test_message_attributes(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        mock_client.publish.return_value = {"MessageId": "m1"}
        await topic.publish("body", PublishOptions(attributes={"env": "prod", "priority": "high"}))
        kwargs = mock_client.publish.call_args.kwargs
        attrs = kwargs["MessageAttributes"]
        assert attrs["env"] == {"DataType": "String", "StringValue": "prod"}
        assert attrs["priority"] == {"DataType": "String", "StringValue": "high"}

    @pytest.mark.asyncio
    async def test_optional_params_omitted(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        mock_client.publish.return_value = {"MessageId": "m1"}
        await topic.publish("body")
        kwargs = mock_client.publish.call_args.kwargs
        assert "Subject" not in kwargs
        assert "MessageGroupId" not in kwargs
        assert "MessageDeduplicationId" not in kwargs
        assert "MessageAttributes" not in kwargs


class TestBatchChunking:
    @pytest.mark.asyncio
    async def test_auto_chunks_over_10(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        mock_client.publish_batch.return_value = {"Successful": [], "Failed": []}
        entries = [BatchPublishEntry(id=f"e{i}", body=f"msg-{i}") for i in range(25)]
        await topic.publish_batch(entries)
        assert mock_client.publish_batch.call_count == 3

    @pytest.mark.asyncio
    async def test_chunk_sizes(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        mock_client.publish_batch.return_value = {"Successful": [], "Failed": []}
        entries = [BatchPublishEntry(id=f"e{i}", body=f"msg-{i}") for i in range(25)]
        await topic.publish_batch(entries)
        calls = mock_client.publish_batch.call_args_list
        assert len(calls[0].kwargs["PublishBatchRequestEntries"]) == 10
        assert len(calls[1].kwargs["PublishBatchRequestEntries"]) == 10
        assert len(calls[2].kwargs["PublishBatchRequestEntries"]) == 5

    @pytest.mark.asyncio
    async def test_collects_partial_failures_across_chunks(
        self, topic: SNSTopic, mock_client: AsyncMock
    ) -> None:
        mock_client.publish_batch.side_effect = [
            {
                "Successful": [{"Id": "e0", "MessageId": "m0"}],
                "Failed": [{"Id": "e1", "Code": "Err", "Message": "fail", "SenderFault": True}],
            },
            {"Successful": [{"Id": "e10", "MessageId": "m10"}], "Failed": []},
        ]
        entries = [BatchPublishEntry(id=f"e{i}", body=f"msg-{i}") for i in range(12)]
        result = await topic.publish_batch(entries)
        assert len(result.successful) == 2
        assert len(result.failed) == 1
        assert result.failed[0].id == "e1"
        assert result.failed[0].sender_fault is True

    @pytest.mark.asyncio
    async def test_batch_entry_params(self, topic: SNSTopic, mock_client: AsyncMock) -> None:
        mock_client.publish_batch.return_value = {
            "Successful": [{"Id": "e1", "MessageId": "m1"}],
            "Failed": [],
        }
        entry = BatchPublishEntry(
            id="e1",
            body="hello",
            subject="Subject",
            group_id="grp",
            deduplication_id="dup",
            attributes={"k": "v"},
        )
        await topic.publish_batch([entry])
        sns_entry = mock_client.publish_batch.call_args.kwargs["PublishBatchRequestEntries"][0]
        assert sns_entry["Subject"] == "Subject"
        assert sns_entry["MessageGroupId"] == "grp"
        assert sns_entry["MessageDeduplicationId"] == "dup"
        assert sns_entry["MessageAttributes"]["k"] == {
            "DataType": "String",
            "StringValue": "v",
        }
