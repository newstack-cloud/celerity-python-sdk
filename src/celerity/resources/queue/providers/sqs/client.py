"""SQS queue client and per-queue implementation."""

from __future__ import annotations

import logging
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any

from celerity.resources.queue.errors import QueueError
from celerity.resources.queue.types import (
    BatchSendEntry,
    BatchSendFailure,
    BatchSendResult,
    BatchSendSuccess,
    Queue,
    QueueClient,
    SendMessageOptions,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    import aioboto3
    from types_aiobotocore_sqs.client import SQSClient

    from celerity.resources.queue.providers.sqs.types import SQSQueueConfig
    from celerity.types.telemetry import CelerityTracer

logger = logging.getLogger("celerity.queue.sqs")

_SQS_BATCH_LIMIT = 10


class SQSQueueClient(QueueClient):
    """QueueClient backed by SQS via aioboto3."""

    def __init__(
        self,
        session: aioboto3.Session,
        config: SQSQueueConfig,
        tracer: CelerityTracer | None = None,
        resource_ids: dict[str, str] | None = None,
    ) -> None:
        self._session = session
        self._config = config
        self._tracer = tracer
        self._resource_ids = resource_ids or {}
        self._exit_stack = AsyncExitStack()
        self._client: SQSClient | None = None

    async def _ensure_client(self) -> SQSClient:
        """Lazily create the SQS client on first use."""
        if self._client is None:
            kwargs: dict[str, Any] = {}
            if self._config.region:
                kwargs["region_name"] = self._config.region
            if self._config.endpoint_url:
                kwargs["endpoint_url"] = self._config.endpoint_url
            self._client = await self._exit_stack.enter_async_context(
                self._session.client("sqs", **kwargs)
            )
            logger.debug(
                "created SQS client region=%s endpoint=%s",
                self._config.region,
                self._config.endpoint_url,
            )
        return self._client

    def queue(self, name: str) -> Queue:
        queue_url = self._resource_ids.get(name, "")
        if not queue_url:
            raise QueueError(f"No queue URL configured for resource {name!r}")
        return SQSQueue(
            client_provider=self._ensure_client,
            queue_url=queue_url,
            tracer=self._tracer,
        )

    async def close(self) -> None:
        """Close the underlying client session."""
        await self._exit_stack.aclose()
        self._client = None


class SQSQueue(Queue):
    """Per-queue SQS implementation."""

    def __init__(
        self,
        client_provider: Callable[[], Awaitable[SQSClient]],
        queue_url: str,
        tracer: CelerityTracer | None = None,
    ) -> None:
        self._client_provider = client_provider
        self._queue_url = queue_url
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

    async def send_message(
        self,
        body: str,
        options: SendMessageOptions | None = None,
    ) -> str:
        result: str = await self._traced(
            "celerity.queue.send_message",
            lambda: self._send_message(body, options),
            attributes={"queue.url": self._queue_url},
        )
        return result

    async def send_message_batch(
        self,
        entries: list[BatchSendEntry],
    ) -> BatchSendResult:
        result: BatchSendResult = await self._traced(
            "celerity.queue.send_message_batch",
            lambda: self._send_message_batch(entries),
            attributes={
                "queue.url": self._queue_url,
                "queue.batch_size": len(entries),
            },
        )
        return result

    async def _send_message(
        self,
        body: str,
        options: SendMessageOptions | None,
    ) -> str:
        logger.debug("send_message %s", self._queue_url)
        client = await self._client_provider()
        params: dict[str, Any] = {
            "QueueUrl": self._queue_url,
            "MessageBody": body,
        }
        if options:
            if options.group_id is not None:
                params["MessageGroupId"] = options.group_id
            if options.deduplication_id is not None:
                params["MessageDeduplicationId"] = options.deduplication_id
            if options.delay_seconds is not None:
                params["DelaySeconds"] = options.delay_seconds
            if options.attributes:
                params["MessageAttributes"] = _build_message_attributes(options.attributes)

        try:
            response = await client.send_message(**params)
        except Exception as exc:
            raise QueueError(f"SQS send_message failed: {exc}", cause=exc) from exc

        message_id: str = response["MessageId"]
        return message_id

    async def _send_message_batch(
        self,
        entries: list[BatchSendEntry],
    ) -> BatchSendResult:
        logger.debug("send_message_batch %s count=%d", self._queue_url, len(entries))
        client = await self._client_provider()

        all_successful: list[BatchSendSuccess] = []
        all_failed: list[BatchSendFailure] = []

        # Auto-chunk into groups of 10 (SQS batch limit).
        for i in range(0, len(entries), _SQS_BATCH_LIMIT):
            chunk = entries[i : i + _SQS_BATCH_LIMIT]
            sqs_entries = [_build_batch_entry(e) for e in chunk]

            try:
                response = await client.send_message_batch(
                    QueueUrl=self._queue_url,
                    Entries=sqs_entries,  # type: ignore[arg-type]
                )
            except Exception as exc:
                raise QueueError(f"SQS send_message_batch failed: {exc}", cause=exc) from exc

            for success in response.get("Successful", []):
                all_successful.append(
                    BatchSendSuccess(id=success["Id"], message_id=success["MessageId"])
                )
            for failure in response.get("Failed", []):
                all_failed.append(
                    BatchSendFailure(
                        id=failure["Id"],
                        code=failure["Code"],
                        message=failure.get("Message", ""),
                        sender_fault=failure.get("SenderFault", False),
                    )
                )

        return BatchSendResult(successful=all_successful, failed=all_failed)


def _build_message_attributes(
    attributes: dict[str, str],
) -> dict[str, dict[str, str]]:
    """Convert a flat dict to SQS MessageAttributes format."""
    return {key: {"DataType": "String", "StringValue": value} for key, value in attributes.items()}


def _build_batch_entry(entry: BatchSendEntry) -> dict[str, Any]:
    """Convert a BatchSendEntry to an SQS SendMessageBatchRequestEntry."""
    item: dict[str, Any] = {
        "Id": entry.id,
        "MessageBody": entry.body,
    }
    if entry.group_id is not None:
        item["MessageGroupId"] = entry.group_id
    if entry.deduplication_id is not None:
        item["MessageDeduplicationId"] = entry.deduplication_id
    if entry.delay_seconds is not None:
        item["DelaySeconds"] = entry.delay_seconds
    if entry.attributes:
        item["MessageAttributes"] = _build_message_attributes(entry.attributes)
    return item
