"""Tests for queue client factory."""

from __future__ import annotations

import pytest

from celerity.resources.queue.errors import QueueError
from celerity.resources.queue.factory import create_queue_client


class TestCreateQueueClient:
    def test_aws_provider(self) -> None:
        client = create_queue_client(provider="aws")
        from celerity.resources.queue.providers.sqs.client import SQSQueueClient

        assert isinstance(client, SQSQueueClient)

    def test_local_provider(self) -> None:
        client = create_queue_client(provider="local")
        from celerity.resources.queue.providers.redis.client import RedisQueueClient

        assert isinstance(client, RedisQueueClient)

    def test_unsupported_provider(self) -> None:
        with pytest.raises(QueueError, match="Unsupported"):
            create_queue_client(provider="gcp")
