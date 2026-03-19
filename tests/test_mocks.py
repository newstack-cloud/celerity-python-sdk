"""Tests for the mock factory functions."""

from __future__ import annotations

from celerity.testing.mocks import (
    mock_consumer_event,
    mock_request,
    mock_schedule_event,
    mock_websocket_message,
)
from celerity.types.consumer import ConsumerEventInput
from celerity.types.http import HttpRequest
from celerity.types.schedule import ScheduleEventInput
from celerity.types.websocket import WebSocketMessage


class TestMockRequest:
    def test_defaults(self) -> None:
        req = mock_request()
        assert isinstance(req, HttpRequest)
        assert req.method == "GET"
        assert req.path == "/"
        assert req.request_id == "test-request-id"
        assert req.client_ip == "127.0.0.1"
        assert req.text_body is None

    def test_post_with_dict_body(self) -> None:
        req = mock_request("POST", "/orders", body={"name": "Widget"})
        assert req.method == "POST"
        assert req.path == "/orders"
        assert req.text_body == '{"name": "Widget"}'
        assert req.content_type == "application/json"

    def test_post_with_string_body(self) -> None:
        req = mock_request("POST", "/raw", body="raw text")
        assert req.text_body == "raw text"
        assert req.content_type == "text/plain"

    def test_with_auth(self) -> None:
        req = mock_request(auth={"sub": "user-1"})
        assert req.auth == {"sub": "user-1"}

    def test_with_headers(self) -> None:
        req = mock_request(headers={"x-custom": "val"})
        assert req.headers == {"x-custom": "val"}

    def test_with_query(self) -> None:
        req = mock_request(query={"page": "1"})
        assert req.query == {"page": "1"}

    def test_with_cookies(self) -> None:
        req = mock_request(cookies={"session": "abc"})
        assert req.cookies == {"session": "abc"}

    def test_with_path_params(self) -> None:
        req = mock_request("GET", "/orders/123", path_params={"id": "123"})
        assert req.path_params == {"id": "123"}


class TestMockWebSocketMessage:
    def test_defaults(self) -> None:
        msg = mock_websocket_message()
        assert isinstance(msg, WebSocketMessage)
        assert msg.event_type == "message"
        assert msg.connection_id == "test-conn-id"
        assert msg.message_type == "json"

    def test_with_json_body(self) -> None:
        msg = mock_websocket_message(json_body={"action": "chat"})
        assert msg.json_body == {"action": "chat"}

    def test_connect_event(self) -> None:
        msg = mock_websocket_message("connect")
        assert msg.event_type == "connect"

    def test_binary_message(self) -> None:
        msg = mock_websocket_message(binary_body=b"\x00\x01")
        assert msg.binary_body == b"\x00\x01"
        assert msg.message_type == "binary"


class TestMockConsumerEvent:
    def test_basic(self) -> None:
        event = mock_consumer_event(
            "queue::handle",
            [
                {"message_id": "1", "body": '{"key": "val"}', "source": "queue"},
            ],
        )
        assert isinstance(event, ConsumerEventInput)
        assert event.handler_tag == "queue::handle"
        assert len(event.messages) == 1
        assert event.messages[0].message_id == "1"
        assert event.messages[0].body == '{"key": "val"}'

    def test_defaults_filled(self) -> None:
        event = mock_consumer_event("tag", [{}])
        assert event.messages[0].message_id == "msg-0"
        assert event.messages[0].source == "test-source"

    def test_multiple_messages(self) -> None:
        event = mock_consumer_event(
            "tag",
            [
                {"message_id": "a", "body": "1", "source": "q"},
                {"message_id": "b", "body": "2", "source": "q"},
            ],
        )
        assert len(event.messages) == 2


class TestMockScheduleEvent:
    def test_defaults(self) -> None:
        event = mock_schedule_event("cleanup")
        assert isinstance(event, ScheduleEventInput)
        assert event.handler_tag == "cleanup"
        assert event.schedule_id == "test-schedule"
        assert event.schedule == "rate(1 hour)"

    def test_custom_values(self) -> None:
        event = mock_schedule_event(
            "daily-job",
            schedule_id="sched-1",
            schedule="rate(1 day)",
            input={"key": "val"},
        )
        assert event.schedule_id == "sched-1"
        assert event.schedule == "rate(1 day)"
        assert event.input == {"key": "val"}
