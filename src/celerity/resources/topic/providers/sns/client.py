"""SNS topic client and per-topic implementation."""

from __future__ import annotations

import logging
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any

from celerity.resources.topic.errors import TopicError
from celerity.resources.topic.types import (
    BatchPublishEntry,
    BatchPublishFailure,
    BatchPublishResult,
    BatchPublishSuccess,
    PublishOptions,
    Topic,
    TopicClient,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    import aioboto3
    from types_aiobotocore_sns.client import SNSClient

    from celerity.resources.topic.providers.sns.types import SNSTopicConfig
    from celerity.types.telemetry import CelerityTracer

logger = logging.getLogger("celerity.topic.sns")

_SNS_BATCH_LIMIT = 10


class SNSTopicClient(TopicClient):
    """TopicClient backed by SNS via aioboto3."""

    def __init__(
        self,
        session: aioboto3.Session,
        config: SNSTopicConfig,
        tracer: CelerityTracer | None = None,
        resource_ids: dict[str, str] | None = None,
    ) -> None:
        self._session = session
        self._config = config
        self._tracer = tracer
        self._resource_ids = resource_ids or {}
        self._exit_stack = AsyncExitStack()
        self._client: SNSClient | None = None

    async def _ensure_client(self) -> SNSClient:
        """Lazily create the SNS client on first use."""
        if self._client is None:
            kwargs: dict[str, Any] = {}
            if self._config.region:
                kwargs["region_name"] = self._config.region
            if self._config.endpoint_url:
                kwargs["endpoint_url"] = self._config.endpoint_url
            self._client = await self._exit_stack.enter_async_context(
                self._session.client("sns", **kwargs)
            )
            logger.debug(
                "created SNS client region=%s endpoint=%s",
                self._config.region,
                self._config.endpoint_url,
            )
        return self._client

    def topic(self, name: str) -> Topic:
        topic_arn = self._resource_ids.get(name, "")
        if not topic_arn:
            raise TopicError(f"No topic ARN configured for resource {name!r}")
        return SNSTopic(
            client_provider=self._ensure_client,
            topic_arn=topic_arn,
            tracer=self._tracer,
        )

    async def close(self) -> None:
        """Close the underlying client session."""
        await self._exit_stack.aclose()
        self._client = None


class SNSTopic(Topic):
    """Per-topic SNS implementation."""

    def __init__(
        self,
        client_provider: Callable[[], Awaitable[SNSClient]],
        topic_arn: str,
        tracer: CelerityTracer | None = None,
    ) -> None:
        self._client_provider = client_provider
        self._topic_arn = topic_arn
        self._tracer = tracer

    async def _traced(
        self,
        name: str,
        fn: Callable[..., Awaitable[Any]],
        attributes: dict[str, Any] | None = None,
    ) -> Any:
        """Execute *fn* within a tracer span if a tracer is available."""
        if not self._tracer:
            return await fn()
        return await self._tracer.with_span(name, lambda _span: fn(), attributes=attributes)

    async def publish(
        self,
        body: str,
        options: PublishOptions | None = None,
    ) -> str:
        result: str = await self._traced(
            "celerity.topic.publish",
            lambda: self._publish(body, options),
            attributes={"topic.arn": self._topic_arn},
        )
        return result

    async def publish_batch(
        self,
        entries: list[BatchPublishEntry],
    ) -> BatchPublishResult:
        result: BatchPublishResult = await self._traced(
            "celerity.topic.publish_batch",
            lambda: self._publish_batch(entries),
            attributes={
                "topic.arn": self._topic_arn,
                "topic.batch_size": len(entries),
            },
        )
        return result

    async def _publish(
        self,
        body: str,
        options: PublishOptions | None,
    ) -> str:
        logger.debug("publish %s", self._topic_arn)
        client = await self._client_provider()
        params: dict[str, Any] = {
            "TopicArn": self._topic_arn,
            "Message": body,
        }
        if options:
            if options.subject is not None:
                params["Subject"] = options.subject
            if options.group_id is not None:
                params["MessageGroupId"] = options.group_id
            if options.deduplication_id is not None:
                params["MessageDeduplicationId"] = options.deduplication_id
            if options.attributes:
                params["MessageAttributes"] = _build_message_attributes(options.attributes)

        try:
            response = await client.publish(**params)
        except Exception as exc:
            raise TopicError(f"SNS publish failed: {exc}", cause=exc) from exc

        message_id: str = response["MessageId"]
        return message_id

    async def _publish_batch(
        self,
        entries: list[BatchPublishEntry],
    ) -> BatchPublishResult:
        logger.debug("publish_batch %s count=%d", self._topic_arn, len(entries))
        client = await self._client_provider()

        all_successful: list[BatchPublishSuccess] = []
        all_failed: list[BatchPublishFailure] = []

        # Auto-chunk into groups of 10 (SNS batch limit).
        for i in range(0, len(entries), _SNS_BATCH_LIMIT):
            chunk = entries[i : i + _SNS_BATCH_LIMIT]
            sns_entries = [_build_batch_entry(e) for e in chunk]

            try:
                response = await client.publish_batch(
                    TopicArn=self._topic_arn,
                    PublishBatchRequestEntries=sns_entries,  # type: ignore[arg-type]
                )
            except Exception as exc:
                raise TopicError(f"SNS publish_batch failed: {exc}", cause=exc) from exc

            for success in response.get("Successful", []):
                all_successful.append(
                    BatchPublishSuccess(id=success["Id"], message_id=success["MessageId"])
                )
            for failure in response.get("Failed", []):
                all_failed.append(
                    BatchPublishFailure(
                        id=failure["Id"],
                        code=failure["Code"],
                        message=failure.get("Message", ""),
                        sender_fault=failure.get("SenderFault", False),
                    )
                )

        return BatchPublishResult(successful=all_successful, failed=all_failed)


def _build_message_attributes(
    attributes: dict[str, str],
) -> dict[str, dict[str, str]]:
    """Convert a flat dict to SNS MessageAttributes format."""
    return {key: {"DataType": "String", "StringValue": value} for key, value in attributes.items()}


def _build_batch_entry(entry: BatchPublishEntry) -> dict[str, Any]:
    """Convert a BatchPublishEntry to an SNS PublishBatchRequestEntry."""
    item: dict[str, Any] = {
        "Id": entry.id,
        "Message": entry.body,
    }
    if entry.subject is not None:
        item["Subject"] = entry.subject
    if entry.group_id is not None:
        item["MessageGroupId"] = entry.group_id
    if entry.deduplication_id is not None:
        item["MessageDeduplicationId"] = entry.deduplication_id
    if entry.attributes:
        item["MessageAttributes"] = _build_message_attributes(entry.attributes)
    return item
