"""Tests for celerity.handlers.registry."""

from celerity.handlers.registry import HandlerRegistry
from celerity.types.handler import (
    ResolvedConsumerHandler,
    ResolvedCustomHandler,
    ResolvedGuard,
    ResolvedHttpHandler,
    ResolvedScheduleHandler,
    ResolvedWebSocketHandler,
)


def _http(method: str, path: str) -> ResolvedHttpHandler:
    return ResolvedHttpHandler(handler_fn=lambda: None, method=method, path=path)


class TestHttpRegistration:
    def test_register_and_lookup(self) -> None:
        registry = HandlerRegistry()
        handler = _http("GET", "/orders/{id}")
        registry.register(handler)
        found = registry.get_handler("http", "GET /orders/123")
        assert found is handler

    def test_path_pattern_matching(self) -> None:
        registry = HandlerRegistry()
        registry.register(_http("GET", "/orders/{order_id}/items/{item_id}"))
        found = registry.get_handler("http", "GET /orders/abc/items/xyz")
        assert found is not None

    def test_method_mismatch(self) -> None:
        registry = HandlerRegistry()
        registry.register(_http("GET", "/orders"))
        found = registry.get_handler("http", "POST /orders")
        assert found is None

    def test_path_mismatch(self) -> None:
        registry = HandlerRegistry()
        registry.register(_http("GET", "/orders/{id}"))
        found = registry.get_handler("http", "GET /users/123")
        assert found is None

    def test_extract_path_params(self) -> None:
        registry = HandlerRegistry()
        registry.register(_http("GET", "/orders/{id}"))
        params = registry.extract_path_params("GET /orders/123")
        assert params == {"id": "123"}

    def test_extract_multiple_path_params(self) -> None:
        registry = HandlerRegistry()
        registry.register(_http("GET", "/orders/{order_id}/items/{item_id}"))
        params = registry.extract_path_params("GET /orders/abc/items/xyz")
        assert params == {"order_id": "abc", "item_id": "xyz"}

    def test_no_match_returns_empty_params(self) -> None:
        registry = HandlerRegistry()
        params = registry.extract_path_params("GET /nonexistent")
        assert params == {}

    def test_static_path(self) -> None:
        registry = HandlerRegistry()
        registry.register(_http("GET", "/health"))
        found = registry.get_handler("http", "GET /health")
        assert found is not None


class TestNonHttpRegistration:
    def test_websocket_lookup(self) -> None:
        registry = HandlerRegistry()
        handler = ResolvedWebSocketHandler(handler_fn=lambda: None, route="chat")
        registry.register(handler)
        found = registry.get_handler("websocket", "chat")
        assert found is handler

    def test_consumer_lookup(self) -> None:
        registry = HandlerRegistry()
        handler = ResolvedConsumerHandler(handler_fn=lambda: None, handler_tag="orders-queue")
        registry.register(handler)
        found = registry.get_handler("consumer", "orders-queue")
        assert found is handler

    def test_schedule_lookup(self) -> None:
        registry = HandlerRegistry()
        handler = ResolvedScheduleHandler(handler_fn=lambda: None, handler_tag="daily-cleanup")
        registry.register(handler)
        found = registry.get_handler("schedule", "daily-cleanup")
        assert found is handler

    def test_custom_lookup(self) -> None:
        registry = HandlerRegistry()
        handler = ResolvedCustomHandler(handler_fn=lambda: None, name="processOrder")
        registry.register(handler)
        found = registry.get_handler("custom", "processOrder")
        assert found is handler


class TestGuardRegistration:
    def test_register_and_lookup(self) -> None:
        registry = HandlerRegistry()
        guard = ResolvedGuard(name="admin", handler_fn=lambda: None)
        registry.register_guard(guard)
        found = registry.get_guard("admin")
        assert found is guard

    def test_unknown_guard(self) -> None:
        registry = HandlerRegistry()
        assert registry.get_guard("nonexistent") is None

    def test_get_all_guards(self) -> None:
        registry = HandlerRegistry()
        registry.register_guard(ResolvedGuard(name="a", handler_fn=lambda: None))
        registry.register_guard(ResolvedGuard(name="b", handler_fn=lambda: None))
        assert len(registry.get_all_guards()) == 2


class TestBulkQueries:
    def test_get_handlers_by_type(self) -> None:
        registry = HandlerRegistry()
        registry.register(_http("GET", "/a"))
        registry.register(_http("POST", "/b"))
        registry.register(ResolvedConsumerHandler(handler_fn=lambda: None, handler_tag="q"))
        http_handlers = registry.get_handlers_by_type("http")
        assert len(http_handlers) == 2

    def test_get_all_handlers(self) -> None:
        registry = HandlerRegistry()
        registry.register(_http("GET", "/a"))
        registry.register(ResolvedConsumerHandler(handler_fn=lambda: None, handler_tag="q"))
        assert len(registry.get_all_handlers()) == 2

    def test_get_handler_by_id(self) -> None:
        registry = HandlerRegistry()
        handler = ResolvedHttpHandler(handler_fn=lambda: None, method="GET", path="/a", id="h1")
        registry.register(handler)
        found = registry.get_handler_by_id("http", "h1")
        assert found is handler

    def test_get_handler_by_id_not_found(self) -> None:
        registry = HandlerRegistry()
        assert registry.get_handler_by_id("http", "missing") is None
