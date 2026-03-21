"""Tests for topic client factory."""

from __future__ import annotations

import pytest

from celerity.resources.topic.errors import TopicError
from celerity.resources.topic.factory import create_topic_client


class TestCreateTopicClient:
    def test_aws_provider(self) -> None:
        client = create_topic_client(provider="aws")
        from celerity.resources.topic.providers.sns.client import SNSTopicClient

        assert isinstance(client, SNSTopicClient)

    def test_local_provider(self) -> None:
        client = create_topic_client(provider="local")
        from celerity.resources.topic.providers.redis.client import RedisTopicClient

        assert isinstance(client, RedisTopicClient)

    def test_unsupported_provider(self) -> None:
        with pytest.raises(TopicError, match="Unsupported"):
            create_topic_client(provider="gcp")
