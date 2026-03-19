"""Extended tests for param extraction across all handler types."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from celerity.handlers.param_extractor import _extract_single_param
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.consumer import ConsumerEventInput, ConsumerMessage
from celerity.types.context import (
    ConsumerHandlerContext,
    HttpHandlerContext,
    ScheduleHandlerContext,
    WebSocketHandlerContext,
)
from celerity.types.handler import ParamMetadata
from celerity.types.http import HttpRequest
from celerity.types.schedule import ScheduleEventInput
from celerity.types.websocket import (
    WebSocketEventType,
    WebSocketMessage,
    WebSocketMessageType,
)

# ---------------------------------------------------------------------------
# HTTP validated param extraction (ValidationLayer integration)
# ---------------------------------------------------------------------------


class TestValidatedParamExtraction:
    def test_returns_validated_body_from_metadata(self) -> None:
        """When ValidationLayer has pre-validated the body, extraction uses it."""
        metadata = HandlerMetadataStore({})
        metadata.set("validated_body", {"name": "Validated Widget"})
        ctx = HttpHandlerContext(
            request=HttpRequest(method="POST", path="/orders", text_body='{"name": "Raw"}'),
            metadata=metadata,
            container=SimpleNamespace(),  # type: ignore[arg-type]
        )
        meta = ParamMetadata(index=0, type="body")
        result = _extract_single_param(meta, ctx)
        assert result == {"name": "Validated Widget"}

    def test_falls_back_to_raw_body_without_validation(self) -> None:
        """Without validated data, extraction parses the raw JSON body."""
        metadata = HandlerMetadataStore({})
        ctx = HttpHandlerContext(
            request=HttpRequest(method="POST", path="/orders", text_body='{"name": "Raw"}'),
            metadata=metadata,
            container=SimpleNamespace(),  # type: ignore[arg-type]
        )
        meta = ParamMetadata(index=0, type="body")
        result = _extract_single_param(meta, ctx)
        assert result == {"name": "Raw"}

    def test_non_json_body_returned_as_string(self) -> None:
        """If body is not valid JSON, it's returned as-is."""
        metadata = HandlerMetadataStore({})
        ctx = HttpHandlerContext(
            request=HttpRequest(method="POST", path="/data", text_body="plain text"),
            metadata=metadata,
            container=SimpleNamespace(),  # type: ignore[arg-type]
        )
        meta = ParamMetadata(index=0, type="body")
        result = _extract_single_param(meta, ctx)
        assert result == "plain text"


# ---------------------------------------------------------------------------
# HTTP token extraction
# ---------------------------------------------------------------------------


class TestTokenExtraction:
    def test_bearer_token_extracted(self) -> None:
        ctx = _http_context(headers={"authorization": "Bearer abc123"})
        meta = ParamMetadata(index=0, type="token")
        result = _extract_single_param(meta, ctx)
        assert result == "abc123"

    def test_non_bearer_auth_returned_as_is(self) -> None:
        ctx = _http_context(headers={"authorization": "Basic dXNlcjpwYXNz"})
        meta = ParamMetadata(index=0, type="token")
        result = _extract_single_param(meta, ctx)
        assert result == "Basic dXNlcjpwYXNz"

    def test_request_id_extracted(self) -> None:
        ctx = _http_context(request_id="req-42")
        meta = ParamMetadata(index=0, type="requestId")
        result = _extract_single_param(meta, ctx)
        assert result == "req-42"

    def test_full_request_extracted(self) -> None:
        ctx = _http_context()
        meta = ParamMetadata(index=0, type="request")
        result = _extract_single_param(meta, ctx)
        assert isinstance(result, HttpRequest)


# ---------------------------------------------------------------------------
# WebSocket param extraction
# ---------------------------------------------------------------------------


class TestWebSocketParamExtraction:
    def test_connection_id(self) -> None:
        ctx = _ws_context(connection_id="conn-1")
        meta = ParamMetadata(index=0, type="connectionId")
        assert _extract_single_param(meta, ctx) == "conn-1"

    def test_message_body(self) -> None:
        ctx = _ws_context(json_body={"action": "chat"})
        meta = ParamMetadata(index=0, type="messageBody")
        assert _extract_single_param(meta, ctx) == {"action": "chat"}

    def test_message_id(self) -> None:
        ctx = _ws_context(message_id="msg-42")
        meta = ParamMetadata(index=0, type="messageId")
        assert _extract_single_param(meta, ctx) == "msg-42"

    def test_event_type(self) -> None:
        ctx = _ws_context(event_type=WebSocketEventType.CONNECT)
        meta = ParamMetadata(index=0, type="eventType")
        assert _extract_single_param(meta, ctx) == "connect"


# ---------------------------------------------------------------------------
# Consumer param extraction
# ---------------------------------------------------------------------------


class TestConsumerParamExtraction:
    def test_messages(self) -> None:
        messages = [ConsumerMessage(message_id="1", body="data", source="q")]
        ctx = _consumer_context(messages=messages)
        meta = ParamMetadata(index=0, type="messages")
        result = _extract_single_param(meta, ctx)
        assert len(result) == 1
        assert result[0].message_id == "1"

    def test_consumer_event(self) -> None:
        ctx = _consumer_context()
        meta = ParamMetadata(index=0, type="consumerEvent")
        result = _extract_single_param(meta, ctx)
        assert isinstance(result, ConsumerEventInput)

    def test_consumer_vendor(self) -> None:
        ctx = _consumer_context(vendor="aws")
        meta = ParamMetadata(index=0, type="consumerVendor")
        assert _extract_single_param(meta, ctx) == "aws"


# ---------------------------------------------------------------------------
# Schedule param extraction
# ---------------------------------------------------------------------------


class TestScheduleParamExtraction:
    def test_schedule_input(self) -> None:
        ctx = _schedule_context(input={"key": "val"})
        meta = ParamMetadata(index=0, type="scheduleInput")
        assert _extract_single_param(meta, ctx) == {"key": "val"}

    def test_schedule_id(self) -> None:
        ctx = _schedule_context(schedule_id="sched-1")
        meta = ParamMetadata(index=0, type="scheduleId")
        assert _extract_single_param(meta, ctx) == "sched-1"

    def test_schedule_expression(self) -> None:
        ctx = _schedule_context(schedule="rate(1 day)")
        meta = ParamMetadata(index=0, type="scheduleExpression")
        assert _extract_single_param(meta, ctx) == "rate(1 day)"

    def test_schedule_event_input(self) -> None:
        ctx = _schedule_context()
        meta = ParamMetadata(index=0, type="scheduleEventInput")
        result = _extract_single_param(meta, ctx)
        assert isinstance(result, ScheduleEventInput)


# ---------------------------------------------------------------------------
# Custom/invoke param extraction
# ---------------------------------------------------------------------------


class TestCustomParamExtraction:
    def test_payload(self) -> None:
        ctx = SimpleNamespace(
            payload={"order_id": "123"},
            metadata=HandlerMetadataStore({}),
            container=SimpleNamespace(),
        )
        meta = ParamMetadata(index=0, type="payload")
        assert _extract_single_param(meta, ctx) == {"order_id": "123"}  # type: ignore[arg-type]

    def test_invoke_context(self) -> None:
        ctx = SimpleNamespace(
            payload=None,
            metadata=HandlerMetadataStore({}),
            container=SimpleNamespace(),
        )
        meta = ParamMetadata(index=0, type="invokeContext")
        assert _extract_single_param(meta, ctx) is ctx  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _http_context(
    *,
    headers: dict[str, Any] | None = None,
    request_id: str = "req-1",
) -> HttpHandlerContext:
    return HttpHandlerContext(
        request=HttpRequest(
            method="GET",
            path="/",
            headers=headers or {},
            request_id=request_id,
        ),
        metadata=HandlerMetadataStore({}),
        container=SimpleNamespace(),  # type: ignore[arg-type]
    )


def _ws_context(
    *,
    connection_id: str = "conn-1",
    message_id: str = "msg-1",
    json_body: Any = None,
    event_type: WebSocketEventType = WebSocketEventType.MESSAGE,
) -> WebSocketHandlerContext:
    return WebSocketHandlerContext(
        message=WebSocketMessage(
            message_type=WebSocketMessageType.JSON,
            event_type=event_type,
            connection_id=connection_id,
            message_id=message_id,
            json_body=json_body,
        ),
        metadata=HandlerMetadataStore({}),
        container=SimpleNamespace(),  # type: ignore[arg-type]
    )


def _consumer_context(
    *,
    messages: list[ConsumerMessage] | None = None,
    vendor: Any = None,
) -> ConsumerHandlerContext:
    return ConsumerHandlerContext(
        event=ConsumerEventInput(
            handler_tag="test-queue",
            messages=messages or [],
            vendor=vendor,
        ),
        metadata=HandlerMetadataStore({}),
        container=SimpleNamespace(),  # type: ignore[arg-type]
    )


def _schedule_context(
    *,
    schedule_id: str = "sched-1",
    schedule: str = "rate(1 hour)",
    input: Any = None,
) -> ScheduleHandlerContext:
    return ScheduleHandlerContext(
        event=ScheduleEventInput(
            handler_tag="test-schedule",
            schedule_id=schedule_id,
            message_id="msg-1",
            schedule=schedule,
            input=input,
        ),
        metadata=HandlerMetadataStore({}),
        container=SimpleNamespace(),  # type: ignore[arg-type]
    )
