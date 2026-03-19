"""Tests for the AWS Lambda event mappers and adapter."""

from __future__ import annotations

import base64
from typing import Any

from celerity.serverless.aws.event_mapper import (
    detect_event_type,
    map_api_gateway_v2_event,
    map_api_gateway_websocket_event,
    map_consumer_result_to_sqs_batch_response,
    map_eventbridge_event,
    map_http_response_to_result,
    map_sqs_event,
)
from celerity.types.http import HttpResponse

# ---------------------------------------------------------------------------
# Event type detection
# ---------------------------------------------------------------------------


class TestDetectEventType:
    def test_api_gateway_v2_http(self) -> None:
        event = {"requestContext": {"http": {"method": "GET", "path": "/"}}}
        assert detect_event_type(event) == "http"

    def test_api_gateway_websocket(self) -> None:
        event = {
            "requestContext": {
                "connectionId": "abc",
                "eventType": "MESSAGE",
            },
        }
        assert detect_event_type(event) == "websocket"

    def test_sqs_event(self) -> None:
        event = {"Records": [{"eventSource": "aws:sqs"}]}
        assert detect_event_type(event) == "consumer"

    def test_eventbridge_event(self) -> None:
        event = {"source": "aws.events", "detail-type": "Scheduled Event"}
        assert detect_event_type(event) == "schedule"

    def test_custom_event(self) -> None:
        event = {"handlerName": "myHandler", "payload": {}}
        assert detect_event_type(event) == "custom"

    def test_env_override(self, monkeypatch: Any) -> None:
        monkeypatch.setenv("CELERITY_HANDLER_TYPE", "schedule")
        event = {"requestContext": {"http": {"method": "GET"}}}
        assert detect_event_type(event) == "schedule"


# ---------------------------------------------------------------------------
# API Gateway V2 HTTP
# ---------------------------------------------------------------------------


class TestMapApiGatewayV2Event:
    def test_basic_get(self) -> None:
        event = _make_apigw_event(method="GET", path="/orders")
        request = map_api_gateway_v2_event(event)

        assert request.method == "GET"
        assert request.path == "/orders"
        assert request.request_id == "req-123"
        assert request.client_ip == "1.2.3.4"
        assert request.user_agent == "test-agent"

    def test_post_with_body(self) -> None:
        event = _make_apigw_event(
            method="POST",
            path="/orders",
            body='{"name": "Widget"}',
        )
        request = map_api_gateway_v2_event(event)

        assert request.method == "POST"
        assert request.text_body == '{"name": "Widget"}'

    def test_base64_body(self) -> None:
        raw = b"\x00\x01\x02"
        event = _make_apigw_event(
            body=base64.b64encode(raw).decode(),
            is_base64=True,
        )
        request = map_api_gateway_v2_event(event)

        assert request.binary_body == raw
        assert request.text_body is None

    def test_path_params(self) -> None:
        event = _make_apigw_event(path="/orders/123")
        event["pathParameters"] = {"order_id": "123"}
        request = map_api_gateway_v2_event(event)

        assert request.path_params == {"order_id": "123"}

    def test_query_string(self) -> None:
        event = _make_apigw_event()
        event["rawQueryString"] = "page=1&sort=name&sort=date"
        request = map_api_gateway_v2_event(event)

        assert request.query["page"] == "1"
        assert request.query["sort"] == ["name", "date"]

    def test_cookies(self) -> None:
        event = _make_apigw_event()
        event["cookies"] = ["session=abc", "theme=dark"]
        request = map_api_gateway_v2_event(event)

        assert request.cookies == {"session": "abc", "theme": "dark"}

    def test_jwt_auth(self) -> None:
        event = _make_apigw_event()
        event["requestContext"]["authorizer"] = {
            "jwt": {"claims": {"sub": "user-1", "scope": "read"}},
        }
        request = map_api_gateway_v2_event(event)

        assert request.auth == {"sub": "user-1", "scope": "read"}

    def test_trace_context(self) -> None:
        event = _make_apigw_event()
        event["headers"]["x-amzn-trace-id"] = "Root=1-abc-def"
        request = map_api_gateway_v2_event(event)

        assert request.trace_context == {"x-amzn-trace-id": "Root=1-abc-def"}


class TestMapHttpResponseToResult:
    def test_text_response(self) -> None:
        response = HttpResponse(
            status=200,
            body='{"ok": true}',
            headers={"content-type": "application/json"},
        )
        result = map_http_response_to_result(response)

        assert result["statusCode"] == 200
        assert result["body"] == '{"ok": true}'
        assert result["headers"] == {"content-type": "application/json"}
        assert "isBase64Encoded" not in result

    def test_binary_response(self) -> None:
        raw = b"\x00\x01\x02"
        response = HttpResponse(status=200, binary_body=raw)
        result = map_http_response_to_result(response)

        assert result["isBase64Encoded"] is True
        assert base64.b64decode(result["body"]) == raw

    def test_empty_response(self) -> None:
        response = HttpResponse(status=204)
        result = map_http_response_to_result(response)

        assert result["statusCode"] == 204
        assert "body" not in result


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------


class TestMapApiGatewayWebSocketEvent:
    def test_message_event(self) -> None:
        event = {
            "requestContext": {
                "connectionId": "conn-1",
                "routeKey": "chat",
                "eventType": "MESSAGE",
                "requestId": "req-ws-1",
                "domainName": "abc.execute-api.us-east-1.amazonaws.com",
                "stage": "prod",
            },
            "body": '{"action": "chat", "text": "hello"}',
            "isBase64Encoded": False,
        }
        message, endpoint = map_api_gateway_websocket_event(event)

        assert message.connection_id == "conn-1"
        assert message.event_type == "message"
        assert message.json_body == {"action": "chat", "text": "hello"}
        assert "abc.execute-api" in endpoint

    def test_connect_event(self) -> None:
        event = {
            "requestContext": {
                "connectionId": "conn-2",
                "routeKey": "$connect",
                "eventType": "CONNECT",
                "requestId": "req-ws-2",
                "domainName": "abc.example.com",
                "stage": "dev",
            },
        }
        message, _ = map_api_gateway_websocket_event(event)

        assert message.event_type == "connect"
        assert message.connection_id == "conn-2"


# ---------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------


class TestMapSqsEvent:
    def test_basic_sqs(self) -> None:
        event = {
            "Records": [
                {
                    "messageId": "msg-1",
                    "body": '{"orderId": "123"}',
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123:orders",
                    "messageAttributes": {},
                },
                {
                    "messageId": "msg-2",
                    "body": '{"orderId": "456"}',
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123:orders",
                    "messageAttributes": {},
                },
            ],
        }
        result = map_sqs_event(event, "orders-queue::handle")

        assert result.handler_tag == "orders-queue::handle"
        assert len(result.messages) == 2
        assert result.messages[0].message_id == "msg-1"
        assert result.messages[1].body == '{"orderId": "456"}'
        assert result.vendor == "aws"


class TestMapConsumerResultToSqsBatch:
    def test_no_failures(self) -> None:
        result = map_consumer_result_to_sqs_batch_response(None)
        assert result == {"batchItemFailures": []}

    def test_with_failures(self) -> None:
        failures = [
            {"message_id": "msg-1", "error_message": "bad data"},
            {"message_id": "msg-3", "error_message": "timeout"},
        ]
        result = map_consumer_result_to_sqs_batch_response(failures)
        assert len(result["batchItemFailures"]) == 2
        assert result["batchItemFailures"][0]["itemIdentifier"] == "msg-1"


# ---------------------------------------------------------------------------
# EventBridge
# ---------------------------------------------------------------------------


class TestMapEventBridgeEvent:
    def test_scheduled_event(self) -> None:
        event = {
            "id": "event-123",
            "source": "aws.events",
            "detail-type": "Scheduled Event",
            "detail": {"key": "value"},
            "account": "123456789",
            "region": "us-east-1",
        }
        result = map_eventbridge_event(event, "cleanup")

        assert result.handler_tag == "cleanup"
        assert result.schedule_id == "event-123"
        assert result.input == {"key": "value"}
        assert result.vendor["source"] == "aws.events"
        assert result.vendor["region"] == "us-east-1"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_apigw_event(
    method: str = "GET",
    path: str = "/",
    body: str | None = None,
    is_base64: bool = False,
) -> dict[str, Any]:
    """Create a minimal API Gateway V2 event fixture."""
    return {
        "requestContext": {
            "http": {
                "method": method,
                "path": path,
                "sourceIp": "1.2.3.4",
                "userAgent": "test-agent",
            },
            "requestId": "req-123",
            "time": "2026-01-01T00:00:00Z",
            "authorizer": {},
        },
        "rawPath": path,
        "rawQueryString": "",
        "headers": {},
        "cookies": [],
        "body": body,
        "isBase64Encoded": is_base64,
    }
