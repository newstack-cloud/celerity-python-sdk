"""Redis Streams queue client and per-queue implementation."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from celerity.resources.queue.errors import QueueError
from celerity.resources.queue.types import (
    BatchSendEntry,
    BatchSendResult,
    BatchSendSuccess,
    Queue,
    QueueClient,
    SendMessageOptions,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from redis.asyncio import Redis

    from celerity.resources.queue.providers.redis.types import RedisQueueConfig
    from celerity.types.telemetry import CelerityTracer

logger = logging.getLogger("celerity.queue.redis")


class RedisQueueClient(QueueClient):
    """QueueClient backed by Redis Streams for local development.

    Simple, single-node Redis -- no cluster mode, TLS, or IAM.
    Designed exclusively for local development and testing.

    Redis URL sourced from CELERITY_REDIS_ENDPOINT env var,
    defaulting to redis://localhost:6379.
    """

    def __init__(
        self,
        client: Redis[bytes],
        config: RedisQueueConfig,
        tracer: CelerityTracer | None = None,
    ) -> None:
        self._client = client
        self._config = config
        self._tracer = tracer

    def queue(self, name: str) -> Queue:
        stream_key = f"celerity:queue:{name}"
        return RedisQueue(
            client=self._client,
            stream_key=stream_key,
            tracer=self._tracer,
        )

    async def close(self) -> None:
        await self._client.aclose()  # type: ignore[attr-defined]


class RedisQueue(Queue):
    """Queue implementation using Redis Streams.

    Stream key format: ``celerity:queue:{name}``

    All operations are instrumented with tracer spans via ``_traced()``.
    Debug logging on each operation via ``logger.debug()``.
    All exceptions wrapped in QueueError.
    """

    def __init__(
        self,
        client: Redis[bytes],
        stream_key: str,
        tracer: CelerityTracer | None = None,
    ) -> None:
        self._client = client
        self._stream_key = stream_key
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
            attributes={"queue.stream_key": self._stream_key},
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
                "queue.stream_key": self._stream_key,
                "queue.batch_size": len(entries),
            },
        )
        return result

    async def _send_message(
        self,
        body: str,
        options: SendMessageOptions | None,
    ) -> str:
        logger.debug("send_message %s", self._stream_key)
        fields = _build_stream_fields(body, options)
        try:
            entry_id = await self._client.xadd(self._stream_key, fields)
        except Exception as exc:
            raise QueueError(f"Redis XADD failed: {exc}", cause=exc) from exc
        return entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)

    async def _send_message_batch(
        self,
        entries: list[BatchSendEntry],
    ) -> BatchSendResult:
        logger.debug("send_message_batch %s count=%d", self._stream_key, len(entries))
        try:
            pipe = self._client.pipeline()
            for entry in entries:
                opts = SendMessageOptions(
                    group_id=entry.group_id,
                    deduplication_id=entry.deduplication_id,
                    delay_seconds=entry.delay_seconds,
                    attributes=entry.attributes,
                )
                fields = _build_stream_fields(entry.body, opts)
                pipe.xadd(self._stream_key, fields)
            results = await pipe.execute()
        except Exception as exc:
            raise QueueError(f"Redis pipeline XADD failed: {exc}", cause=exc) from exc

        successful = [
            BatchSendSuccess(
                id=entries[i].id,
                message_id=(
                    results[i].decode() if isinstance(results[i], bytes) else str(results[i])
                ),
            )
            for i in range(len(entries))
        ]
        return BatchSendResult(successful=successful, failed=[])


def _build_stream_fields(
    body: str,
    options: SendMessageOptions | None,
) -> dict[str, str]:
    """Build the XADD field dict.

    Always includes: body, timestamp, message_type.
    Conditionally includes: group_id, dedup_id, attributes.
    """
    fields: dict[str, str] = {
        "body": body,
        "timestamp": datetime.now(tz=UTC).isoformat(),
        "message_type": "standard",
    }
    if options:
        if options.group_id is not None:
            fields["group_id"] = options.group_id
        if options.deduplication_id is not None:
            fields["dedup_id"] = options.deduplication_id
        if options.attributes:
            fields["attributes"] = json.dumps(options.attributes)
    return fields
