"""Integration tests for SQS queue against LocalStack."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import aioboto3
import pytest

from celerity.resources.queue.errors import QueueError
from celerity.resources.queue.providers.sqs.client import SQSQueueClient
from celerity.resources.queue.providers.sqs.types import SQSQueueConfig
from celerity.resources.queue.types import (
    BatchSendEntry,
    Queue,
    SendMessageOptions,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"
TEST_QUEUE_NAME = "celerity-test-queue"
FIFO_QUEUE_NAME = "celerity-test-queue.fifo"


async def _ensure_queues() -> dict[str, str]:
    """Create test queues if they don't exist and return their URLs."""
    session = aioboto3.Session()
    async with session.client(
        "sqs",
        region_name=REGION,
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ) as client:
        standard_resp = await client.create_queue(QueueName=TEST_QUEUE_NAME)
        fifo_resp = await client.create_queue(
            QueueName=FIFO_QUEUE_NAME,
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "false",
            },
        )
        return {
            "standard": standard_resp["QueueUrl"],
            "fifo": fifo_resp["QueueUrl"],
        }


async def _purge_queues(urls: dict[str, str]) -> None:
    """Purge all test queues."""
    session = aioboto3.Session()
    async with session.client(
        "sqs",
        region_name=REGION,
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ) as client:
        for url in urls.values():
            try:  # noqa: SIM105
                await client.purge_queue(QueueUrl=url)
            except Exception:
                pass


async def _receive_messages(
    queue_url: str, max_messages: int = 10, wait_seconds: int = 3
) -> list[dict[str, Any]]:
    """Helper to receive messages from SQS."""
    session = aioboto3.Session()
    async with session.client(
        "sqs",
        region_name=REGION,
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ) as client:
        resp = await client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_seconds,
            MessageAttributeNames=["All"],
        )
        return cast("list[dict[str, Any]]", resp.get("Messages", []))


@pytest.fixture
async def sqs_env() -> AsyncGenerator[tuple[dict[str, str], Queue, Queue]]:
    """Function-scoped: create queues, client, queue handles, and clean up."""
    urls = await _ensure_queues()
    await _purge_queues(urls)

    session = aioboto3.Session()
    config = SQSQueueConfig(region=REGION, endpoint_url=ENDPOINT_URL)
    client = SQSQueueClient(
        session=session,
        config=config,
        resource_ids={"test": urls["standard"], "fifo-test": urls["fifo"]},
    )

    standard_q = client.queue("test")
    fifo_q = client.queue("fifo-test")

    yield urls, standard_q, fifo_q

    await client.close()


class TestSendMessage:
    @pytest.mark.asyncio
    async def test_send_and_receive(self, sqs_env: tuple[dict[str, str], Queue, Queue]) -> None:
        urls, q, _ = sqs_env
        msg_id = await q.send_message("Hello, SQS!")
        assert msg_id

        messages = await _receive_messages(urls["standard"])
        assert len(messages) >= 1
        assert messages[0]["Body"] == "Hello, SQS!"

    @pytest.mark.asyncio
    async def test_send_with_attributes(self, sqs_env: tuple[dict[str, str], Queue, Queue]) -> None:
        urls, q, _ = sqs_env
        await q.send_message(
            "attributed",
            SendMessageOptions(attributes={"env": "test", "priority": "high"}),
        )

        messages = await _receive_messages(urls["standard"])
        assert len(messages) >= 1
        attrs = messages[0].get("MessageAttributes", {})
        assert attrs["env"]["StringValue"] == "test"
        assert attrs["priority"]["StringValue"] == "high"

    @pytest.mark.asyncio
    async def test_send_to_fifo_queue(self, sqs_env: tuple[dict[str, str], Queue, Queue]) -> None:
        urls, _, fifo_q = sqs_env
        msg_id = await fifo_q.send_message(
            "fifo-msg",
            SendMessageOptions(
                group_id="group-1",
                deduplication_id="dedup-unique-1",
            ),
        )
        assert msg_id

        messages = await _receive_messages(urls["fifo"])
        assert len(messages) >= 1
        assert messages[0]["Body"] == "fifo-msg"

    @pytest.mark.asyncio
    async def test_deduplication(self, sqs_env: tuple[dict[str, str], Queue, Queue]) -> None:
        urls, _, fifo_q = sqs_env
        opts = SendMessageOptions(
            group_id="group-dedup",
            deduplication_id="same-dedup-id",
        )
        await fifo_q.send_message("first", opts)
        await fifo_q.send_message("second", opts)

        messages = await _receive_messages(urls["fifo"])
        # Deduplication should produce only one message
        assert len(messages) == 1


class TestSendMessageBatch:
    @pytest.mark.asyncio
    async def test_batch_under_10(self, sqs_env: tuple[dict[str, str], Queue, Queue]) -> None:
        urls, q, _ = sqs_env
        entries = [BatchSendEntry(id=f"e{i}", body=f"batch-msg-{i}") for i in range(5)]
        result = await q.send_message_batch(entries)
        assert len(result.successful) == 5
        assert result.failed == []

        messages = await _receive_messages(urls["standard"])
        assert len(messages) >= 5

    @pytest.mark.asyncio
    async def test_batch_over_10_auto_chunk(
        self, sqs_env: tuple[dict[str, str], Queue, Queue]
    ) -> None:
        _urls, q, _ = sqs_env
        entries = [BatchSendEntry(id=f"e{i}", body=f"chunk-msg-{i}") for i in range(15)]
        result = await q.send_message_batch(entries)
        assert len(result.successful) == 15
        assert result.failed == []


class TestErrorCase:
    @pytest.mark.asyncio
    async def test_send_to_nonexistent_queue(self) -> None:
        session = aioboto3.Session()
        config = SQSQueueConfig(region=REGION, endpoint_url=ENDPOINT_URL)
        client = SQSQueueClient(
            session=session,
            config=config,
            resource_ids={
                "bad": "https://sqs.us-east-1.amazonaws.com/000/nonexistent",
            },
        )
        try:
            q = client.queue("bad")
            with pytest.raises(QueueError):
                await q.send_message("should fail")
        finally:
            await client.close()
