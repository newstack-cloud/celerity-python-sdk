"""Queue client factory with platform-based provider selection."""

from __future__ import annotations

from typing import TYPE_CHECKING

from celerity.resources._common import detect_platform
from celerity.resources.queue.errors import QueueError

if TYPE_CHECKING:
    from celerity.resources._common import Platform
    from celerity.resources.queue.providers.sqs.types import SQSQueueConfig
    from celerity.resources.queue.types import QueueClient
    from celerity.types.telemetry import CelerityTracer


def create_queue_client(
    config: SQSQueueConfig | None = None,
    tracer: CelerityTracer | None = None,
    provider: Platform | None = None,
    resource_ids: dict[str, str] | None = None,
) -> QueueClient:
    """Create a QueueClient for the detected platform.

    Provider selection is based on ``CELERITY_PLATFORM``:

    - ``"aws"`` → SQS
    - ``"local"`` → Redis Streams (always Redis regardless of deploy target)
    - ``"gcp"`` → Pub/Sub (not yet implemented)
    - ``"azure"`` → Service Bus (not yet implemented)

    Args:
        config: Optional provider-specific config. If ``None``, captured
            from environment variables.
        tracer: Optional tracer for instrumenting operations.
        provider: Override platform detection (mainly for testing).
        resource_ids: Mapping of logical resource name to physical
            resource identifier (e.g. SQS queue URL, Pub/Sub
            subscription). Resolved by the layer from the config
            service before client creation.
    """
    resolved_provider = provider or detect_platform()

    if resolved_provider == "aws":
        return _create_sqs_client(config, tracer, resource_ids)

    if resolved_provider == "local":
        return _create_redis_client(tracer)

    # Future: "gcp" -> Pub/Sub, "azure" -> Service Bus
    raise QueueError(f"Unsupported queue provider: {resolved_provider!r}")


def _create_sqs_client(
    config: SQSQueueConfig | None,
    tracer: CelerityTracer | None,
    resource_ids: dict[str, str] | None,
) -> QueueClient:
    import aioboto3

    from celerity.resources.queue.providers.sqs.client import SQSQueueClient
    from celerity.resources.queue.providers.sqs.config import capture_sqs_config

    resolved_config = config or capture_sqs_config()
    session = aioboto3.Session()
    return SQSQueueClient(
        session=session,
        config=resolved_config,
        tracer=tracer,
        resource_ids=resource_ids,
    )


def _create_redis_client(
    tracer: CelerityTracer | None,
) -> QueueClient:
    import os

    from redis.asyncio import Redis

    from celerity.resources.queue.providers.redis.client import RedisQueueClient
    from celerity.resources.queue.providers.redis.types import RedisQueueConfig

    url = os.environ.get("CELERITY_REDIS_ENDPOINT", "redis://localhost:6379")
    config = RedisQueueConfig(url=url)
    client: Redis[bytes] = Redis.from_url(url, decode_responses=False)
    return RedisQueueClient(client=client, config=config, tracer=tracer)
