"""Mock factories for creating test fixtures."""

from __future__ import annotations

import json
import uuid
from typing import Any

from celerity.types.consumer import ConsumerEventInput, ConsumerMessage
from celerity.types.http import HttpRequest
from celerity.types.schedule import ScheduleEventInput
from celerity.types.websocket import (
    WebSocketEventType,
    WebSocketMessage,
    WebSocketMessageType,
)


def mock_request(
    method: str = "GET",
    path: str = "/",
    *,
    path_params: dict[str, str] | None = None,
    query: dict[str, str | list[str]] | None = None,
    headers: dict[str, str | list[str]] | None = None,
    cookies: dict[str, str] | None = None,
    body: Any = None,
    auth: dict[str, Any] | None = None,
    request_id: str = "test-request-id",
    client_ip: str = "127.0.0.1",
    user_agent: str = "celerity-test",
) -> HttpRequest:
    """Create a mock ``HttpRequest`` for testing.

    If ``body`` is not a string, it is JSON-serialized automatically.

    Example::

        request = mock_request("POST", "/orders", body={"name": "Widget"})
    """
    if body is not None and not isinstance(body, str):
        text_body = json.dumps(body)
        content_type = "application/json"
    elif isinstance(body, str):
        text_body = body
        content_type = "text/plain"
    else:
        text_body = None
        content_type = None

    return HttpRequest(
        method=method,
        path=path,
        path_params=path_params or {},
        query=query or {},
        headers=headers or {},
        cookies=cookies or {},
        text_body=text_body,
        content_type=content_type,
        auth=auth,
        request_id=request_id,
        client_ip=client_ip,
        user_agent=user_agent,
    )


def mock_websocket_message(
    event_type: str = "message",
    *,
    connection_id: str = "test-conn-id",
    message_id: str | None = None,
    json_body: Any = None,
    binary_body: bytes | None = None,
) -> WebSocketMessage:
    """Create a mock ``WebSocketMessage`` for testing.

    Example::

        msg = mock_websocket_message(json_body={"action": "chat", "text": "hello"})
    """
    msg_type = WebSocketMessageType.BINARY if binary_body else WebSocketMessageType.JSON
    return WebSocketMessage(
        message_type=msg_type,
        event_type=WebSocketEventType(event_type),
        connection_id=connection_id,
        message_id=message_id or f"msg-{uuid.uuid4().hex[:8]}",
        json_body=json_body,
        binary_body=binary_body,
    )


def mock_consumer_event(
    handler_tag: str,
    messages: list[dict[str, Any]],
    *,
    vendor: Any = None,
) -> ConsumerEventInput:
    """Create a mock ``ConsumerEventInput`` for testing.

    Each message dict should have at minimum ``message_id``, ``body``,
    and ``source``. Missing fields are filled with defaults.

    Example::

        event = mock_consumer_event("orders-queue::handle", [
            {"message_id": "1", "body": '{"orderId": "123"}', "source": "orders-queue"},
        ])
    """
    return ConsumerEventInput(
        handler_tag=handler_tag,
        messages=[
            ConsumerMessage(
                message_id=msg.get("message_id", f"msg-{i}"),
                body=msg.get("body", ""),
                source=msg.get("source", "test-source"),
                message_attributes=msg.get("message_attributes"),
                vendor=msg.get("vendor"),
            )
            for i, msg in enumerate(messages)
        ],
        vendor=vendor,
    )


def mock_schedule_event(
    handler_tag: str,
    *,
    schedule_id: str = "test-schedule",
    schedule: str = "rate(1 hour)",
    input: Any = None,
    vendor: Any = None,
) -> ScheduleEventInput:
    """Create a mock ``ScheduleEventInput`` for testing.

    Example::

        event = mock_schedule_event("cleanup", schedule="rate(1 day)")
    """
    return ScheduleEventInput(
        handler_tag=handler_tag,
        schedule_id=schedule_id,
        message_id=f"sched-{uuid.uuid4().hex[:8]}",
        schedule=schedule,
        input=input,
        vendor=vendor,
    )
