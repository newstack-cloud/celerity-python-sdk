"""Tests for celerity.functions factories."""

from celerity.functions.consumer import create_consumer_handler
from celerity.functions.custom import create_custom_handler
from celerity.functions.guard import create_guard
from celerity.functions.http import (
    create_http_handler,
    http_delete,
    http_get,
    http_patch,
    http_post,
    http_put,
)
from celerity.functions.schedule import create_schedule_handler
from celerity.functions.websocket import create_websocket_handler


async def _noop(*_args: object, **_kwargs: object) -> None:
    pass


class TestHttpHandlerFactory:
    def test_create_http_handler(self) -> None:
        defn = create_http_handler(path="/orders", method="POST", handler=_noop)
        assert defn.type == "http"
        assert defn.metadata["path"] == "/orders"
        assert defn.metadata["method"] == "POST"
        assert defn.handler is _noop

    def test_http_get(self) -> None:
        defn = http_get("/orders", _noop)
        assert defn.type == "http"
        assert defn.metadata["method"] == "GET"
        assert defn.metadata["path"] == "/orders"

    def test_http_post(self) -> None:
        defn = http_post("/orders", _noop)
        assert defn.metadata["method"] == "POST"

    def test_http_put(self) -> None:
        defn = http_put("/orders/1", _noop)
        assert defn.metadata["method"] == "PUT"

    def test_http_patch(self) -> None:
        defn = http_patch("/orders/1", _noop)
        assert defn.metadata["method"] == "PATCH"

    def test_http_delete(self) -> None:
        defn = http_delete("/orders/1", _noop)
        assert defn.metadata["method"] == "DELETE"

    def test_with_custom_metadata(self) -> None:
        defn = create_http_handler(path="/", handler=_noop, metadata={"version": "v2"})
        assert defn.metadata["version"] == "v2"


class TestGuardFactory:
    def test_create_guard(self) -> None:
        defn = create_guard(name="admin", handler=_noop)
        assert defn.name == "admin"
        assert defn.handler is _noop


class TestWebSocketHandlerFactory:
    def test_create_websocket_handler(self) -> None:
        defn = create_websocket_handler(route="chat", handler=_noop)
        assert defn.type == "websocket"
        assert defn.metadata["route"] == "chat"

    def test_default_route(self) -> None:
        defn = create_websocket_handler(handler=_noop)
        assert defn.metadata["route"] == "$default"

    def test_with_protected_by(self) -> None:
        defn = create_websocket_handler(route="admin-chat", protected_by=["admin"], handler=_noop)
        assert defn.metadata["protected_by"] == ["admin"]


class TestConsumerHandlerFactory:
    def test_create_consumer_handler(self) -> None:
        defn = create_consumer_handler(handler_tag="orders-queue", handler=_noop)
        assert defn.type == "consumer"
        assert defn.metadata["handler_tag"] == "orders-queue"

    def test_with_route(self) -> None:
        defn = create_consumer_handler(handler_tag="q", route="priority", handler=_noop)
        assert defn.metadata["route"] == "priority"


class TestScheduleHandlerFactory:
    def test_with_schedule_expression(self) -> None:
        defn = create_schedule_handler(schedule="rate(1 day)", handler=_noop)
        assert defn.type == "schedule"
        assert defn.metadata["schedule"] == "rate(1 day)"

    def test_with_source(self) -> None:
        defn = create_schedule_handler(source="daily-cleanup", handler=_noop)
        assert defn.metadata["source"] == "daily-cleanup"
        assert defn.metadata["handler_tag"] == "daily-cleanup"


class TestCustomHandlerFactory:
    def test_create_custom_handler(self) -> None:
        defn = create_custom_handler(name="processOrder", handler=_noop)
        assert defn.type == "custom"
        assert defn.metadata["name"] == "processOrder"
