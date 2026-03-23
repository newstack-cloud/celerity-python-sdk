"""Topic client factory with platform-based provider selection."""

from __future__ import annotations

from typing import TYPE_CHECKING

from celerity.resources._common import detect_platform
from celerity.resources.topic.errors import TopicError

if TYPE_CHECKING:
    from celerity.resources._common import Platform
    from celerity.resources.topic.providers.sns.types import SNSTopicConfig
    from celerity.resources.topic.types import TopicClient
    from celerity.types.telemetry import CelerityTracer


def create_topic_client(
    config: SNSTopicConfig | None = None,
    tracer: CelerityTracer | None = None,
    provider: Platform | None = None,
    resource_ids: dict[str, str] | None = None,
) -> TopicClient:
    """Create a TopicClient for the detected platform.

    Provider selection is based on ``CELERITY_PLATFORM``:

    - ``"aws"`` → SNS
    - ``"local"`` → Redis Pub/Sub (always Redis regardless of deploy target)
    - ``"gcp"`` → Pub/Sub (not yet implemented)
    - ``"azure"`` → Service Bus Topics (not yet implemented)

    Args:
        config: Optional provider-specific config. If ``None``, captured
            from environment variables.
        tracer: Optional tracer for instrumenting operations.
        provider: Override platform detection (mainly for testing).
        resource_ids: Mapping of logical resource name to physical
            resource identifier (e.g. SNS topic ARN). Resolved by
            the layer from the config service before client creation.
    """
    resolved_provider = provider or detect_platform()

    if resolved_provider == "aws":
        return _create_sns_client(config, tracer, resource_ids)

    if resolved_provider == "local":
        return _create_redis_client(tracer, resource_ids)

    # Future: "gcp" -> Pub/Sub, "azure" -> Service Bus Topics
    raise TopicError(f"Unsupported topic provider: {resolved_provider!r}")


def _create_sns_client(
    config: SNSTopicConfig | None,
    tracer: CelerityTracer | None,
    resource_ids: dict[str, str] | None,
) -> TopicClient:
    import aioboto3

    from celerity.resources.topic.providers.sns.client import SNSTopicClient
    from celerity.resources.topic.providers.sns.config import capture_sns_config

    resolved_config = config or capture_sns_config()
    session = aioboto3.Session()
    return SNSTopicClient(
        session=session,
        config=resolved_config,
        tracer=tracer,
        resource_ids=resource_ids,
    )


def _create_redis_client(
    tracer: CelerityTracer | None,
    resource_ids: dict[str, str] | None = None,
) -> TopicClient:
    import os

    from redis.asyncio import Redis

    from celerity.resources.topic.providers.redis.client import RedisTopicClient
    from celerity.resources.topic.providers.redis.types import RedisTopicConfig

    url = os.environ.get("CELERITY_REDIS_ENDPOINT", "redis://localhost:6379")
    config = RedisTopicConfig(url=url)
    client: Redis[bytes] = Redis.from_url(url, decode_responses=False)
    return RedisTopicClient(client=client, config=config, tracer=tracer, resource_ids=resource_ids)
