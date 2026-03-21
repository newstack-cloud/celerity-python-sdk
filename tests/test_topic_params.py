"""Tests for topic DI parameter types and helpers."""

from __future__ import annotations

from typing import Annotated, get_args, get_type_hints

from celerity.resources.topic.params import (
    DEFAULT_TOPIC_TOKEN,
    TopicParam,
    TopicResource,
    topic_token,
)
from celerity.resources.topic.types import Topic


class TestTopicParam:
    def test_default(self) -> None:
        param = TopicParam()
        assert param.resource_type == "topic"
        assert param.resource_name is None

    def test_named(self) -> None:
        param = TopicParam("orders")
        assert param.resource_type == "topic"
        assert param.resource_name == "orders"


class TestTopicToken:
    def test_topic_token(self) -> None:
        assert topic_token("orders") == "celerity:topic:orders"

    def test_topic_token_notifications(self) -> None:
        assert topic_token("notifications") == "celerity:topic:notifications"

    def test_default_topic_token(self) -> None:
        assert DEFAULT_TOPIC_TOKEN == "celerity:topic:default"


class TestTopicResource:
    def test_alias_resolves_to_annotated(self) -> None:
        args = get_args(TopicResource)
        assert args[0] is Topic
        assert isinstance(args[1], TopicParam)
        assert args[1].resource_name is None

    def test_named_annotated(self) -> None:
        orders_topic = Annotated[Topic, TopicParam("orders")]
        args = get_args(orders_topic)
        assert args[0] is Topic
        assert isinstance(args[1], TopicParam)
        assert args[1].resource_name == "orders"

    def test_type_hint_resolution(self) -> None:
        class Service:
            def __init__(self, topic: TopicResource) -> None:
                self.topic = topic

        hints = get_type_hints(Service.__init__, include_extras=True)
        args = get_args(hints["topic"])
        assert args[0] is Topic
