"""Tests for the runtime mapper (PyO3 <-> SDK type mapping)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from celerity.bootstrap.runtime_mapper import (
    flatten_multi_value_record,
    map_runtime_consumer_event,
    map_runtime_guard_input,
    map_runtime_request,
    map_runtime_schedule_event,
    map_runtime_websocket_message,
    map_to_runtime_event_result,
    map_to_runtime_guard_result,
    map_to_runtime_response,
)
from celerity.types.consumer import EventResult, MessageProcessingFailure
from celerity.types.http import HttpResponse


def _ns(**kwargs: Any) -> SimpleNamespace:
    """Create a SimpleNamespace (mock PyO3 object)."""
    return SimpleNamespace(**kwargs)


class TestFlattenMultiValueRecord:
    def test_single_values_flattened(self) -> None:
        result = flatten_multi_value_record({"a": ["1"], "b": ["2"]})
        assert result == {"a": "1", "b": "2"}

    def test_multi_values_kept(self) -> None:
        result = flatten_multi_value_record({"a": ["1", "2"]})
        assert result == {"a": ["1", "2"]}

    def test_mixed(self) -> None:
        result = flatten_multi_value_record({"a": ["1"], "b": ["1", "2"]})
        assert result == {"a": "1", "b": ["1", "2"]}


class TestMapRuntimeRequest:
    def test_basic_request(self) -> None:
        py_request = _ns(
            method="get",
            path="/orders",
            path_params={"id": "123"},
            query={"page": ["1"]},
            headers={"content-type": ["application/json"]},
            cookies={"session": "abc"},
            text_body='{"name": "test"}',
            binary_body=None,
            content_type="application/json",
            user_agent="test-agent",
        )
        py_context = _ns(
            request_id="req-123",
            request_time="2026-01-01T00:00:00Z",
            auth={"sub": "user-1"},
            trace_context=None,
            client_ip="127.0.0.1",
            matched_route="/orders/{id}",
        )
        result = map_runtime_request(py_request, py_context)

        assert result.method == "GET"
        assert result.path == "/orders"
        assert result.path_params == {"id": "123"}
        assert result.query == {"page": "1"}
        assert result.headers == {"content-type": "application/json"}
        assert result.text_body == '{"name": "test"}'
        assert result.request_id == "req-123"
        assert result.auth == {"sub": "user-1"}
        assert result.client_ip == "127.0.0.1"
        assert result.user_agent == "test-agent"


class TestMapRuntimeGuardInput:
    def test_basic_guard_input(self) -> None:
        py_input = _ns(
            token="bearer-token",
            request=_ns(
                method="POST",
                path="/admin",
                headers={"authorization": ["Bearer token"]},
                query={},
                cookies={},
                body='{"action": "delete"}',
                request_id="req-456",
                client_ip="10.0.0.1",
            ),
            auth={"role": "admin"},
            handler_name="createOrder",
        )
        result = map_runtime_guard_input(py_input)

        assert result.token == "bearer-token"
        assert result.method == "POST"
        assert result.path == "/admin"
        assert result.auth == {"role": "admin"}
        assert result.handler_name == "createOrder"


class TestMapRuntimeConsumerEvent:
    def test_basic_consumer_event(self) -> None:
        py_event = _ns(
            handler_tag="orders-queue::handle",
            messages=[
                _ns(
                    message_id="msg-1",
                    body='{"orderId": "123"}',
                    source="arn:aws:sqs:us-east-1:123:orders",
                    source_type=None,
                    source_name=None,
                    event_type=None,
                    message_attributes={},
                    vendor="aws",
                ),
            ],
            vendor="aws",
            trace_context=None,
        )
        result = map_runtime_consumer_event(py_event)

        assert result.handler_tag == "orders-queue::handle"
        assert len(result.messages) == 1
        assert result.messages[0].message_id == "msg-1"
        assert result.vendor == "aws"


class TestMapRuntimeScheduleEvent:
    def test_basic_schedule_event(self) -> None:
        py_event = _ns(
            handler_tag="cleanup",
            schedule_id="sched-1",
            message_id="msg-1",
            schedule="rate(1 day)",
            input=None,
            vendor="aws",
            trace_context=None,
        )
        result = map_runtime_schedule_event(py_event)

        assert result.handler_tag == "cleanup"
        assert result.schedule_id == "sched-1"
        assert result.schedule == "rate(1 day)"


class TestMapRuntimeWebSocketMessage:
    def test_basic_ws_message(self) -> None:
        py_msg = _ns(
            type="JSON",
            event_type="MESSAGE",
            connection_id="conn-1",
            message_id="msg-1",
            json_body={"action": "chat", "text": "hello"},
            binary_body=None,
            request_context=None,
            trace_context=None,
        )
        result = map_runtime_websocket_message(py_msg)

        assert result.connection_id == "conn-1"
        assert result.json_body == {"action": "chat", "text": "hello"}
        assert result.event_type == "message"


class TestMapToRuntimeResponse:
    def test_text_response(self) -> None:
        response = HttpResponse(status=200, body="hello", headers={"x-custom": "val"})
        result = map_to_runtime_response(response)

        assert result.status == 200
        assert result.text_body == "hello"
        assert result.headers["x-custom"] == "val"

    def test_binary_response(self) -> None:
        response = HttpResponse(status=200, binary_body=b"\x00\x01")
        result = map_to_runtime_response(response)

        assert result.binary_body == b"\x00\x01"
        assert result.text_body is None


class TestMapToRuntimeEventResult:
    def test_success(self) -> None:
        result = map_to_runtime_event_result(EventResult(success=True))
        assert result.success is True

    def test_failure_with_message(self) -> None:
        result = map_to_runtime_event_result(
            EventResult(success=False, error_message="boom"),
        )
        assert result.success is False
        assert result.error_message == "boom"

    def test_failure_with_batch_failures(self) -> None:
        result = map_to_runtime_event_result(
            EventResult(
                success=False,
                failures=[MessageProcessingFailure("msg-1", "bad data")],
            ),
        )
        assert result.failures is not None
        assert len(result.failures) == 1
        assert result.failures[0].message_id == "msg-1"
        assert result.failures[0].error_message == "bad data"


class TestMapToRuntimeGuardResult:
    def test_allowed(self) -> None:
        pipeline_result = _ns(allowed=True, auth={"sub": "user-1"}, status_code=None)
        result = map_to_runtime_guard_result(pipeline_result)  # type: ignore[arg-type]
        assert result.status == "allowed"
        assert result.auth == {"sub": "user-1"}

    def test_forbidden(self) -> None:
        pipeline_result = _ns(allowed=False, status_code=403, message="Denied")
        result = map_to_runtime_guard_result(pipeline_result)  # type: ignore[arg-type]
        assert result.status == "forbidden"
        assert result.message == "Denied"

    def test_unauthorised(self) -> None:
        pipeline_result = _ns(allowed=False, status_code=401, message="No token")
        result = map_to_runtime_guard_result(pipeline_result)  # type: ignore[arg-type]
        assert result.status == "unauthorised"
        assert result.message == "No token"
