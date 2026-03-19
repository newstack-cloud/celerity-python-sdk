"""AWS Lambda event type mapping.

Converts AWS event shapes (API Gateway V2, SQS, EventBridge, WebSocket)
to SDK types and back.
"""

from __future__ import annotations

import base64
import json
import os
from typing import Any
from urllib.parse import unquote_plus

from celerity.types.consumer import ConsumerEventInput, ConsumerMessage
from celerity.types.http import HttpRequest, HttpResponse
from celerity.types.schedule import ScheduleEventInput
from celerity.types.websocket import (
    WebSocketEventType,
    WebSocketMessage,
    WebSocketMessageType,
    WebSocketRequestContext,
)

# ---------------------------------------------------------------------------
# Event type detection
# ---------------------------------------------------------------------------


def detect_event_type(event: dict[str, Any]) -> str:
    """Detect the Lambda event type from its shape.

    Checks ``CELERITY_HANDLER_TYPE`` env var first, then inspects
    the event structure.

    Returns:
        One of ``"http"``, ``"websocket"``, ``"consumer"``,
        ``"schedule"``, ``"custom"``.
    """
    env_type = os.environ.get("CELERITY_HANDLER_TYPE")
    if env_type:
        return env_type

    request_context = event.get("requestContext", {})

    # API Gateway V2 HTTP
    if "http" in request_context:
        return "http"

    # API Gateway V2 WebSocket
    if "connectionId" in request_context and "eventType" in request_context:
        return "websocket"

    # SQS
    records = event.get("Records", [])
    if records and records[0].get("eventSource") == "aws:sqs":
        return "consumer"

    # EventBridge scheduled event
    if "source" in event and "detail-type" in event:
        return "schedule"

    return "custom"


# ---------------------------------------------------------------------------
# API Gateway V2 HTTP
# ---------------------------------------------------------------------------


def map_api_gateway_v2_event(event: dict[str, Any]) -> HttpRequest:
    """Map ``APIGatewayProxyEventV2`` -> SDK ``HttpRequest``."""
    request_context = event.get("requestContext", {})
    http_context = request_context.get("http", {})

    headers = _parse_headers(event.get("headers", {}))
    query = _parse_query_string(event.get("rawQueryString", ""))
    cookies = _parse_cookies(event.get("cookies", []))
    path_params = event.get("pathParameters") or {}

    body = event.get("body")
    is_base64 = event.get("isBase64Encoded", False)
    text_body: str | None = None
    binary_body: bytes | None = None

    if body and is_base64:
        binary_body = base64.b64decode(body)
    elif body:
        text_body = body

    # Extract auth from JWT authorizer claims.
    authorizer = request_context.get("authorizer", {})
    jwt_claims = authorizer.get("jwt", {}).get("claims")
    auth = jwt_claims if jwt_claims else authorizer.get("lambda") or None

    # Extract trace context from X-Amzn-Trace-Id.
    trace_context: dict[str, str] | None = None
    trace_header = headers.get("x-amzn-trace-id")
    if isinstance(trace_header, str) and trace_header:
        trace_context = {"x-amzn-trace-id": trace_header}

    return HttpRequest(
        method=http_context.get("method", "GET").upper(),
        path=http_context.get("path", event.get("rawPath", "/")),
        path_params={k: unquote_plus(v) for k, v in path_params.items()},
        query=query,
        headers=headers,
        cookies=cookies,
        text_body=text_body,
        binary_body=binary_body,
        content_type=ct if isinstance(ct := headers.get("content-type"), str) else None,
        request_id=request_context.get("requestId", ""),
        request_time=request_context.get("time", ""),
        auth=auth,
        client_ip=http_context.get("sourceIp"),
        trace_context=trace_context,
        user_agent=http_context.get("userAgent"),
    )


def map_http_response_to_result(response: HttpResponse) -> dict[str, Any]:
    """Map SDK ``HttpResponse`` -> ``APIGatewayProxyStructuredResultV2``."""
    result: dict[str, Any] = {"statusCode": response.status}

    if response.headers:
        result["headers"] = response.headers

    if response.binary_body:
        result["body"] = base64.b64encode(response.binary_body).decode("ascii")
        result["isBase64Encoded"] = True
    elif response.body:
        result["body"] = response.body

    return result


# ---------------------------------------------------------------------------
# API Gateway V2 WebSocket
# ---------------------------------------------------------------------------


def map_api_gateway_websocket_event(
    event: dict[str, Any],
) -> tuple[WebSocketMessage, str]:
    """Map ``APIGatewayProxyWebsocketEventV2`` -> ``(WebSocketMessage, endpoint_url)``.

    Returns:
        A tuple of ``(message, endpoint_url)`` where endpoint_url can
        be used for the ``ApiGatewayWebSocketSender``.
    """
    request_context = event.get("requestContext", {})
    connection_id = request_context.get("connectionId", "")
    event_type_str = request_context.get("eventType", "MESSAGE")
    request_id = request_context.get("requestId", "")

    # Derive endpoint URL for sending messages back.
    domain = request_context.get("domainName", "")
    stage = request_context.get("stage", "")
    endpoint_url = f"https://{domain}/{stage}" if domain else ""

    # Map event type.
    event_type_map = {
        "CONNECT": "connect",
        "MESSAGE": "message",
        "DISCONNECT": "disconnect",
    }
    ws_event_type = event_type_map.get(event_type_str, "message")

    # Parse body.
    body = event.get("body")
    json_body: Any = None
    binary_body: bytes | None = None
    is_base64 = event.get("isBase64Encoded", False)

    if body and is_base64:
        binary_body = base64.b64decode(body)
    elif body:
        try:
            json_body = json.loads(body)
        except (json.JSONDecodeError, TypeError):
            json_body = body

    headers = _parse_headers(event.get("headers", {}))
    query = _parse_query_string(event.get("rawQueryString", ""))

    ws_request_context: WebSocketRequestContext | None = None
    if headers or query:
        ws_request_context = WebSocketRequestContext(
            request_id=request_id,
            request_time=0.0,
            path=request_context.get("http", {}).get("path", "/"),
            protocol_version="",
            headers=headers,
            query=query,
            cookies=_parse_cookies(event.get("cookies", [])),
            client_ip=request_context.get("http", {}).get("sourceIp", ""),
        )

    msg_type = WebSocketMessageType.JSON if json_body is not None else WebSocketMessageType.BINARY
    message = WebSocketMessage(
        message_type=msg_type,
        event_type=WebSocketEventType(ws_event_type),
        connection_id=connection_id,
        message_id=request_id,
        json_body=json_body,
        binary_body=binary_body,
        request_context=ws_request_context,
        trace_context=None,
    )

    return message, endpoint_url


# ---------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------


def map_sqs_event(
    event: dict[str, Any],
    handler_tag: str,
) -> ConsumerEventInput:
    """Map ``SQSEvent`` -> SDK ``ConsumerEventInput``."""
    records = event.get("Records", [])
    messages = [_map_sqs_record(record) for record in records]
    return ConsumerEventInput(
        handler_tag=handler_tag,
        messages=messages,
        vendor="aws",
    )


def _map_sqs_record(record: dict[str, Any]) -> ConsumerMessage:
    """Map a single SQS record to a ``ConsumerMessage``."""
    return ConsumerMessage(
        message_id=record.get("messageId", ""),
        body=record.get("body", ""),
        source=record.get("eventSourceARN", ""),
        message_attributes=record.get("messageAttributes"),
        vendor="aws",
    )


def map_consumer_result_to_sqs_batch_response(
    failures: list[dict[str, str]] | None,
) -> dict[str, Any]:
    """Map failure list -> ``SQSBatchResponse`` with ``batchItemFailures``."""
    if not failures:
        return {"batchItemFailures": []}
    return {
        "batchItemFailures": [{"itemIdentifier": f.get("message_id", "")} for f in failures],
    }


# ---------------------------------------------------------------------------
# EventBridge (schedule)
# ---------------------------------------------------------------------------


def map_eventbridge_event(
    event: dict[str, Any],
    handler_tag: str,
) -> ScheduleEventInput:
    """Map ``EventBridge ScheduledEvent`` -> SDK ``ScheduleEventInput``."""
    return ScheduleEventInput(
        handler_tag=handler_tag,
        schedule_id=event.get("id", ""),
        message_id=event.get("id", ""),
        schedule=event.get("detail-type", ""),
        input=event.get("detail"),
        vendor={
            "source": event.get("source"),
            "detail_type": event.get("detail-type"),
            "account": event.get("account"),
            "region": event.get("region"),
        },
    )


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _parse_headers(
    headers: dict[str, str] | None,
) -> dict[str, str | list[str]]:
    """Parse headers from API Gateway V2 format."""
    if not headers:
        return {}
    result: dict[str, str | list[str]] = {}
    for key, value in headers.items():
        lower_key = key.lower()
        if "," in value and lower_key not in ("user-agent", "date", "expires"):
            result[lower_key] = [v.strip() for v in value.split(",")]
        else:
            result[lower_key] = value
    return result


def _parse_query_string(
    raw: str,
) -> dict[str, str | list[str]]:
    """Parse query string into a dict, handling multi-value params."""
    if not raw:
        return {}
    result: dict[str, list[str]] = {}
    for pair in raw.split("&"):
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)
        key = unquote_plus(key)
        value = unquote_plus(value)
        result.setdefault(key, []).append(value)
    final: dict[str, str | list[str]] = {}
    for key, values in result.items():
        final[key] = values[0] if len(values) == 1 else values
    return final


def _parse_cookies(cookies: list[str] | None) -> dict[str, str]:
    """Parse cookies from API Gateway V2 cookies array."""
    if not cookies:
        return {}
    result: dict[str, str] = {}
    for cookie in cookies:
        if "=" in cookie:
            name, value = cookie.split("=", 1)
            result[name.strip()] = value.strip()
    return result
