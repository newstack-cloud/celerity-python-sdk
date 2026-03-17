"""Tests for celerity.types."""

import pytest
from pydantic import BaseModel

from celerity.types.consumer import (
    ConsumerEventInput,
    ConsumerMessage,
    EventResult,
    MessageProcessingFailure,
    SourceType,
)
from celerity.types.guard import GuardInput, GuardResult
from celerity.types.handler import (
    HandlerType,
    ParamMetadata,
    ResolvedHttpHandler,
)
from celerity.types.http import HandlerResponse, HttpRequest, HttpResponse
from celerity.types.websocket import WebSocketEventType, WebSocketMessageType


class TestHttpTypes:
    def test_http_request_minimal(self) -> None:
        req = HttpRequest(method="GET", path="/orders")
        assert req.method == "GET"
        assert req.path == "/orders"
        assert req.path_params == {}
        assert req.query == {}
        assert req.headers == {}
        assert req.text_body is None
        assert req.auth is None

    def test_http_request_full(self) -> None:
        req = HttpRequest(
            method="POST",
            path="/orders",
            path_params={"id": "123"},
            query={"page": "1"},
            headers={"content-type": "application/json"},
            cookies={"session": "abc"},
            text_body='{"name": "test"}',
            content_type="application/json",
            request_id="req-1",
            auth={"sub": "user1"},
            client_ip="127.0.0.1",
            user_agent="test-agent",
        )
        assert req.path_params["id"] == "123"
        assert req.auth is not None
        assert req.auth["sub"] == "user1"

    def test_http_response(self) -> None:
        resp = HttpResponse(status=200, body='{"ok": true}')
        assert resp.status == 200
        assert resp.headers is None

    def test_handler_response_is_http_response(self) -> None:
        assert HandlerResponse is HttpResponse


class TestWebSocketTypes:
    def test_event_type_values(self) -> None:
        assert WebSocketEventType.CONNECT == "connect"
        assert WebSocketEventType.MESSAGE == "message"
        assert WebSocketEventType.DISCONNECT == "disconnect"

    def test_message_type_values(self) -> None:
        assert WebSocketMessageType.JSON == "json"
        assert WebSocketMessageType.BINARY == "binary"

    def test_str_enum_comparison(self) -> None:
        assert WebSocketEventType.CONNECT == "connect"
        assert WebSocketEventType.MESSAGE == "message"


class TestConsumerTypes:
    def test_source_type_values(self) -> None:
        assert SourceType.QUEUE == "queue"
        assert SourceType.TOPIC == "topic"
        assert SourceType.BUCKET == "bucket"
        assert SourceType.DATASTORE == "datastore"

    def test_consumer_message(self) -> None:
        msg = ConsumerMessage(
            message_id="msg-1",
            body='{"key": "value"}',
            source="orders-queue",
        )
        assert msg.message_id == "msg-1"
        assert msg.vendor is None

    def test_consumer_event_input(self) -> None:
        event = ConsumerEventInput(
            handler_tag="process-orders",
            messages=[
                ConsumerMessage(message_id="1", body="a", source="q"),
                ConsumerMessage(message_id="2", body="b", source="q"),
            ],
        )
        assert len(event.messages) == 2

    def test_event_result_success(self) -> None:
        result = EventResult(success=True)
        assert result.success is True
        assert result.failures is None

    def test_event_result_with_failures(self) -> None:
        result = EventResult(
            success=False,
            failures=[
                MessageProcessingFailure(message_id="1", error_message="bad data"),
            ],
        )
        assert result.success is False
        assert result.failures is not None
        assert len(result.failures) == 1
        assert result.failures[0].message_id == "1"


class TestGuardTypes:
    def test_guard_input(self) -> None:
        gi = GuardInput(
            token="Bearer abc",
            method="GET",
            path="/admin",
            headers={},
            query={},
            cookies={},
            body=None,
            request_id="req-1",
            client_ip="127.0.0.1",
            auth={},
        )
        assert gi.token == "Bearer abc"

    def test_guard_result_allow(self) -> None:
        result = GuardResult.allow(auth={"sub": "user1"})
        assert result.allowed is True
        assert result.auth == {"sub": "user1"}
        assert result.status_code is None

    def test_guard_result_forbidden(self) -> None:
        result = GuardResult.forbidden("Admin required")
        assert result.allowed is False
        assert result.status_code == 403
        assert result.message == "Admin required"

    def test_guard_result_unauthorized(self) -> None:
        result = GuardResult.unauthorized()
        assert result.allowed is False
        assert result.status_code == 401
        assert result.message == ""


class TestHandlerTypes:
    def test_handler_type_str_enum(self) -> None:
        assert HandlerType.HTTP == "http"
        assert HandlerType.WEBSOCKET == "websocket"
        assert HandlerType.CONSUMER == "consumer"
        assert HandlerType.SCHEDULE == "schedule"
        assert HandlerType.CUSTOM == "custom"

    def test_param_metadata(self) -> None:
        pm = ParamMetadata(index=1, type="body")
        assert pm.index == 1
        assert pm.key is None

    def test_resolved_http_handler(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            path="/orders/{id}",
            method="GET",
        )
        assert handler.type == HandlerType.HTTP
        assert handler.path == "/orders/{id}"
        assert handler.protected_by == []
        assert handler.is_public is False


class TestSchema:
    """Verify that Schema[T] protocol matches Pydantic v2 models."""

    def test_pydantic_model_validates_data(self) -> None:
        class OrderInput(BaseModel):
            name: str
            quantity: int

        result = OrderInput.model_validate({"name": "Widget", "quantity": 5})
        assert isinstance(result, OrderInput)
        assert result.name == "Widget"
        assert result.quantity == 5

    def test_pydantic_model_rejects_invalid_data(self) -> None:
        class OrderInput(BaseModel):
            name: str
            quantity: int

        with pytest.raises(Exception):  # noqa: B017
            OrderInput.model_validate({"name": "Widget"})  # missing quantity
