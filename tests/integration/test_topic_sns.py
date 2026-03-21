"""Integration tests for SNS topic against LocalStack."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import aioboto3
import pytest

from celerity.resources.topic.errors import TopicError
from celerity.resources.topic.providers.sns.client import SNSTopicClient
from celerity.resources.topic.providers.sns.types import SNSTopicConfig
from celerity.resources.topic.types import (
    BatchPublishEntry,
    PublishOptions,
    Topic,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from types_aiobotocore_sns.client import SNSClient
    from types_aiobotocore_sqs.client import SQSClient

ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"


async def _create_topic(client: SNSClient, name: str) -> str:
    """Create an SNS topic and return its ARN."""
    resp = await client.create_topic(Name=name)
    return resp["TopicArn"]


async def _create_fifo_topic(client: SNSClient, name: str) -> str:
    """Create a FIFO SNS topic and return its ARN."""
    resp = await client.create_topic(
        Name=name,
        Attributes={"FifoTopic": "true", "ContentBasedDeduplication": "false"},
    )
    return resp["TopicArn"]


async def _create_sqs_queue(client: SQSClient, name: str) -> tuple[str, str]:
    """Create an SQS queue, return (url, arn)."""
    resp = await client.create_queue(QueueName=name)
    url = resp["QueueUrl"]
    attrs = await client.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    arn = attrs["Attributes"]["QueueArn"]
    return url, arn


async def _receive_messages(
    queue_url: str,
    min_messages: int = 1,
    max_messages: int = 10,
    timeout_seconds: float = 10,
) -> list[dict[str, Any]]:
    """Receive messages from SQS, polling until min_messages collected or timeout."""
    import time

    session = aioboto3.Session()
    collected: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout_seconds
    async with session.client(
        "sqs",
        region_name=REGION,
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ) as client:
        while len(collected) < min_messages and time.monotonic() < deadline:
            remaining = max(0.5, min(3.0, deadline - time.monotonic()))
            resp = await client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=int(remaining),
                MessageAttributeNames=["All"],
            )
            collected.extend(resp.get("Messages", []))
    return collected


@pytest.fixture
async def sns_env() -> AsyncGenerator[tuple[Topic, str, str]]:
    """
    Function-scoped: create SNS topic + SQS subscriber,
    return (Topic handle, topic_arn, sqs_url).
    """
    session = aioboto3.Session()
    creds = {
        "region_name": REGION,
        "endpoint_url": ENDPOINT_URL,
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    async with (
        session.client("sns", **creds) as sns,
        session.client("sqs", **creds) as sqs,
    ):
        topic_arn = await _create_topic(sns, "celerity-inttest-topic")
        sqs_url, sqs_arn = await _create_sqs_queue(sqs, "celerity-inttest-sub")
        await sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=sqs_arn)

        # Purge SQS queue
        try:  # noqa: SIM105
            await sqs.purge_queue(QueueUrl=sqs_url)
        except Exception:
            pass

        config = SNSTopicConfig(region=REGION, endpoint_url=ENDPOINT_URL)
        client = SNSTopicClient(
            session=aioboto3.Session(),
            config=config,
            resource_ids={"test": topic_arn},
        )
        t = client.topic("test")

        yield t, topic_arn, sqs_url

        await client.close()
        await sns.delete_topic(TopicArn=topic_arn)
        await sqs.delete_queue(QueueUrl=sqs_url)


class TestPublish:
    @pytest.mark.asyncio
    async def test_publish_and_receive(self, sns_env: tuple[Topic, str, str]) -> None:
        t, _arn, sqs_url = sns_env
        msg_id = await t.publish("Hello, SNS!")
        assert msg_id

        messages = await _receive_messages(sqs_url)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        assert body["Message"] == "Hello, SNS!"

    @pytest.mark.asyncio
    async def test_publish_with_subject(self, sns_env: tuple[Topic, str, str]) -> None:
        t, _arn, sqs_url = sns_env
        await t.publish("with-subject", PublishOptions(subject="Greetings"))

        messages = await _receive_messages(sqs_url)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        assert body["Subject"] == "Greetings"

    @pytest.mark.asyncio
    async def test_publish_with_attributes(self, sns_env: tuple[Topic, str, str]) -> None:
        t, _arn, sqs_url = sns_env
        await t.publish(
            "attributed",
            PublishOptions(attributes={"env": "test"}),
        )

        messages = await _receive_messages(sqs_url)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        attrs = body.get("MessageAttributes", {})
        assert attrs["env"]["Value"] == "test"


class TestPublishBatch:
    @pytest.mark.asyncio
    async def test_batch_under_10(self, sns_env: tuple[Topic, str, str]) -> None:
        t, _arn, sqs_url = sns_env
        entries = [BatchPublishEntry(id=f"e{i}", body=f"batch-msg-{i}") for i in range(5)]
        result = await t.publish_batch(entries)
        assert len(result.successful) == 5
        assert result.failed == []

        messages = await _receive_messages(sqs_url, min_messages=5)
        assert len(messages) >= 5

    @pytest.mark.asyncio
    async def test_batch_over_10_auto_chunk(self, sns_env: tuple[Topic, str, str]) -> None:
        t, _arn, _sqs_url = sns_env
        entries = [BatchPublishEntry(id=f"e{i}", body=f"chunk-msg-{i}") for i in range(15)]
        result = await t.publish_batch(entries)
        assert len(result.successful) == 15
        assert result.failed == []


class TestErrorCase:
    @pytest.mark.asyncio
    async def test_publish_to_nonexistent_topic(self) -> None:
        session = aioboto3.Session()
        config = SNSTopicConfig(region=REGION, endpoint_url=ENDPOINT_URL)
        client = SNSTopicClient(
            session=session,
            config=config,
            resource_ids={"bad": "arn:aws:sns:us-east-1:000:nonexistent"},
        )
        try:
            t = client.topic("bad")
            with pytest.raises(TopicError):
                await t.publish("should fail")
        finally:
            await client.close()
