"""Redis Pub/Sub topic client and per-topic implementation."""

from __future__ import annotations

import json
import logging
import uuid
from typing import TYPE_CHECKING, Any

from celerity.resources.topic.errors import TopicError
from celerity.resources.topic.types import (
    BatchPublishEntry,
    BatchPublishResult,
    BatchPublishSuccess,
    PublishOptions,
    Topic,
    TopicClient,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from redis.asyncio import Redis

    from celerity.resources.topic.providers.redis.types import RedisTopicConfig
    from celerity.types.telemetry import CelerityTracer

logger = logging.getLogger("celerity.topic.redis")


class RedisTopicClient(TopicClient):
    """TopicClient backed by Redis Pub/Sub for local development.

    Connects to the Redis instance specified by CELERITY_REDIS_ENDPOINT.
    Uses simple PUBLISH commands -- no cluster mode, TLS, or IAM.
    """

    def __init__(
        self,
        client: Redis[bytes],
        config: RedisTopicConfig,
        tracer: CelerityTracer | None = None,
    ) -> None:
        self._client = client
        self._config = config
        self._tracer = tracer

    def topic(self, name: str) -> Topic:
        channel = f"celerity:topic:channel:{name}"
        return RedisTopic(
            client=self._client,
            channel=channel,
            tracer=self._tracer,
        )

    async def close(self) -> None:
        await self._client.aclose()  # type: ignore[attr-defined]


class RedisTopic(Topic):
    """Topic handle backed by Redis PUBLISH.

    Publishes to channel: ``celerity:topic:channel:{name}``

    All operations are instrumented with tracer spans via ``_traced()``.
    Debug logging on each operation via ``logger.debug()``.
    All exceptions wrapped in TopicError.

    Message envelope is JSON::

        {
            "body": "<message body>",
            "messageId": "<uuid4>",
            "subject": "<optional subject>",
            "attributes": {<optional attributes>}
        }

    ``group_id`` and ``deduplication_id`` are ignored for Redis
    as FIFO semantics are not supported in the local dev provider.
    """

    def __init__(
        self,
        client: Redis[bytes],
        channel: str,
        tracer: CelerityTracer | None = None,
    ) -> None:
        self._client = client
        self._channel = channel
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
            attributes={"topic.channel": self._channel},
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
                "topic.channel": self._channel,
                "topic.batch_size": len(entries),
            },
        )
        return result

    async def _publish(
        self,
        body: str,
        options: PublishOptions | None,
    ) -> str:
        logger.debug("publish %s", self._channel)
        message_id = str(uuid.uuid4())
        envelope = _build_envelope(body, message_id, options)
        try:
            await self._client.publish(self._channel, json.dumps(envelope))
        except Exception as exc:
            raise TopicError(f"Redis PUBLISH failed: {exc}", cause=exc) from exc
        return message_id

    async def _publish_batch(
        self,
        entries: list[BatchPublishEntry],
    ) -> BatchPublishResult:
        logger.debug("publish_batch %s count=%d", self._channel, len(entries))
        successful: list[BatchPublishSuccess] = []
        try:
            pipe = self._client.pipeline()
            message_ids: list[str] = []
            for entry in entries:
                message_id = str(uuid.uuid4())
                message_ids.append(message_id)
                opts = PublishOptions(
                    subject=entry.subject,
                    attributes=entry.attributes,
                )
                envelope = _build_envelope(entry.body, message_id, opts)
                pipe.publish(self._channel, json.dumps(envelope))
            await pipe.execute()
        except Exception as exc:
            raise TopicError(f"Redis pipeline PUBLISH failed: {exc}", cause=exc) from exc

        for i, entry in enumerate(entries):
            successful.append(BatchPublishSuccess(id=entry.id, message_id=message_ids[i]))
        return BatchPublishResult(successful=successful, failed=[])


def _build_envelope(
    body: str,
    message_id: str,
    options: PublishOptions | None,
) -> dict[str, Any]:
    """Build the JSON envelope for a Redis PUBLISH message."""
    envelope: dict[str, Any] = {
        "body": body,
        "messageId": message_id,
    }
    if options:
        if options.subject is not None:
            envelope["subject"] = options.subject
        if options.attributes:
            envelope["attributes"] = options.attributes
    return envelope
