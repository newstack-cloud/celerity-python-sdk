"""Tests for queue DI parameter types and helpers."""

from __future__ import annotations

from typing import Annotated, get_args, get_type_hints

from celerity.resources.queue.params import (
    DEFAULT_QUEUE_TOKEN,
    QueueParam,
    QueueResource,
    queue_token,
)
from celerity.resources.queue.types import Queue


class TestQueueParam:
    def test_default(self) -> None:
        param = QueueParam()
        assert param.resource_type == "queue"
        assert param.resource_name is None

    def test_named(self) -> None:
        param = QueueParam("orders")
        assert param.resource_type == "queue"
        assert param.resource_name == "orders"


class TestQueueToken:
    def test_queue_token(self) -> None:
        assert queue_token("orders") == "celerity:queue:orders"

    def test_queue_token_notifications(self) -> None:
        assert queue_token("notifications") == "celerity:queue:notifications"

    def test_default_queue_token(self) -> None:
        assert DEFAULT_QUEUE_TOKEN == "celerity:queue:default"


class TestQueueResource:
    def test_alias_resolves_to_annotated(self) -> None:
        args = get_args(QueueResource)
        assert args[0] is Queue
        assert isinstance(args[1], QueueParam)
        assert args[1].resource_name is None

    def test_named_annotated(self) -> None:
        orders_queue = Annotated[Queue, QueueParam("orders")]
        args = get_args(orders_queue)
        assert args[0] is Queue
        assert isinstance(args[1], QueueParam)
        assert args[1].resource_name == "orders"

    def test_type_hint_resolution(self) -> None:
        class Service:
            def __init__(self, queue: QueueResource) -> None:
                self.queue = queue

        hints = get_type_hints(Service.__init__, include_extras=True)
        args = get_args(hints["queue"])
        assert args[0] is Queue
