"""Tests for the AWS Lambda adapter dispatch logic."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

from celerity.serverless.aws.adapter import (
    _derive_consumer_tag,
    _handle_consumer,
    _handle_custom,
    _handle_http,
    _handle_schedule,
)

# ---------------------------------------------------------------------------
# HTTP dispatch
# ---------------------------------------------------------------------------


class TestHandleHttp:
    async def test_dispatches_to_matched_handler(self) -> None:
        """HTTP handler is found by method+path and pipeline executes."""
        handler = AsyncMock()
        registry = _mock_registry({"GET /orders": handler})
        state = _make_state(registry)
        event = _apigw_event("GET", "/orders")

        with patch(
            "celerity.handlers.http_pipeline.execute_http_pipeline",
            new_callable=AsyncMock,
        ) as mock_pipeline:
            from celerity.types.http import HttpResponse

            mock_pipeline.return_value = HttpResponse(status=200, body='{"ok": true}')
            result = await _handle_http(event, state, handler_id=None)

        assert result["statusCode"] == 200
        assert result["body"] == '{"ok": true}'

    async def test_returns_404_for_unknown_route(self) -> None:
        """Unknown routes return 404 without calling the pipeline."""
        registry = _mock_registry({})
        state = _make_state(registry)
        event = _apigw_event("GET", "/nonexistent")

        result = await _handle_http(event, state, handler_id=None)
        assert result["statusCode"] == 404

    async def test_resolves_by_handler_id_first(self) -> None:
        """When handler_id is set, it's tried before route matching."""
        handler = AsyncMock()
        registry = _mock_registry({}, by_id={"my-handler": handler})
        state = _make_state(registry)
        event = _apigw_event("GET", "/orders")

        with patch(
            "celerity.handlers.http_pipeline.execute_http_pipeline",
            new_callable=AsyncMock,
        ) as mock_pipeline:
            from celerity.types.http import HttpResponse

            mock_pipeline.return_value = HttpResponse(status=200)
            result = await _handle_http(event, state, handler_id="my-handler")

        assert result["statusCode"] == 200
        mock_pipeline.assert_called_once()


# ---------------------------------------------------------------------------
# Consumer dispatch
# ---------------------------------------------------------------------------


class TestHandleConsumer:
    async def test_dispatches_sqs_event(self) -> None:
        handler = AsyncMock()
        registry = _mock_registry({"orders": handler})
        state = _make_state(registry)
        event = _sqs_event("arn:aws:sqs:us-east-1:123:orders")

        with patch(
            "celerity.handlers.consumer_pipeline.execute_consumer_pipeline",
            new_callable=AsyncMock,
        ) as mock_pipeline:
            from celerity.types.consumer import EventResult

            mock_pipeline.return_value = EventResult(success=True)
            result = await _handle_consumer(event, state, handler_tag="orders")

        assert result == {"batchItemFailures": []}

    async def test_returns_empty_batch_for_unknown_tag(self) -> None:
        registry = _mock_registry({})
        state = _make_state(registry)
        event = _sqs_event("arn:aws:sqs:us-east-1:123:unknown")

        result = await _handle_consumer(event, state, handler_tag="unknown")
        assert result == {"batchItemFailures": []}


# ---------------------------------------------------------------------------
# Schedule dispatch
# ---------------------------------------------------------------------------


class TestHandleSchedule:
    async def test_dispatches_eventbridge_event(self) -> None:
        handler = AsyncMock()
        registry = _mock_registry({"cleanup": handler})
        state = _make_state(registry)
        event = _eventbridge_event()

        with patch(
            "celerity.handlers.schedule_pipeline.execute_schedule_pipeline",
            new_callable=AsyncMock,
        ) as mock_pipeline:
            from celerity.types.consumer import EventResult

            mock_pipeline.return_value = EventResult(success=True)
            result = await _handle_schedule(event, state, handler_tag="cleanup")

        assert result["success"] is True

    async def test_returns_failure_for_unknown_tag(self) -> None:
        registry = _mock_registry({})
        state = _make_state(registry)
        event = _eventbridge_event()

        result = await _handle_schedule(event, state, handler_tag="unknown")
        assert result["success"] is False


# ---------------------------------------------------------------------------
# Custom dispatch
# ---------------------------------------------------------------------------


class TestHandleCustom:
    async def test_dispatches_to_named_handler(self) -> None:
        handler = AsyncMock()
        registry = _mock_registry({"processOrder": handler})
        state = _make_state(registry)
        event = {"handlerName": "processOrder", "payload": {"id": "1"}}

        with patch(
            "celerity.handlers.custom_pipeline.execute_custom_pipeline",
            new_callable=AsyncMock,
            return_value={"processed": True},
        ):
            result = await _handle_custom(event, state, handler_id="processOrder")

        assert result == {"processed": True}

    async def test_returns_error_for_unknown_handler(self) -> None:
        registry = _mock_registry({})
        registry.get_handlers_by_type = lambda _t: []  # type: ignore[method-assign]
        state = _make_state(registry)
        event = {"handlerName": "unknown"}

        result = await _handle_custom(event, state, handler_id="unknown")
        assert result == {"error": "Custom handler not found"}


# ---------------------------------------------------------------------------
# Consumer tag derivation
# ---------------------------------------------------------------------------


class TestDeriveConsumerTag:
    def test_extracts_queue_name_from_arn(self) -> None:
        event = _sqs_event("arn:aws:sqs:us-east-1:123456:my-queue")
        assert _derive_consumer_tag(event) == "my-queue"

    def test_empty_records(self) -> None:
        assert _derive_consumer_tag({"Records": []}) == ""

    def test_no_records(self) -> None:
        assert _derive_consumer_tag({}) == ""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _apigw_event(method: str = "GET", path: str = "/") -> dict[str, Any]:
    return {
        "requestContext": {
            "http": {
                "method": method,
                "path": path,
                "sourceIp": "1.2.3.4",
                "userAgent": "test",
            },
            "requestId": "req-1",
            "time": "2026-01-01T00:00:00Z",
            "authorizer": {},
        },
        "rawPath": path,
        "rawQueryString": "",
        "headers": {},
        "cookies": [],
        "body": None,
        "isBase64Encoded": False,
    }


def _sqs_event(arn: str) -> dict[str, Any]:
    return {
        "Records": [
            {
                "messageId": "msg-1",
                "body": '{"key": "val"}',
                "eventSourceARN": arn,
                "messageAttributes": {},
            },
        ],
    }


def _eventbridge_event() -> dict[str, Any]:
    return {
        "id": "evt-1",
        "source": "aws.events",
        "detail-type": "Scheduled Event",
        "detail": {},
        "account": "123",
        "region": "us-east-1",
    }


class _MockRegistry:
    def __init__(
        self,
        handlers: dict[str, Any],
        by_id: dict[str, Any] | None = None,
    ) -> None:
        self._handlers = handlers
        self._by_id = by_id or {}

    def get_handler(self, handler_type: str, key: str) -> Any:
        return self._handlers.get(key)

    def get_handler_by_id(self, handler_type: str, handler_id: str) -> Any:
        return self._by_id.get(handler_id)

    def get_handlers_by_type(self, handler_type: str) -> list[Any]:
        return list(self._handlers.values())


def _mock_registry(
    handlers: dict[str, Any],
    by_id: dict[str, Any] | None = None,
) -> _MockRegistry:
    return _MockRegistry(handlers, by_id)


def _make_state(registry: Any) -> dict[str, Any]:
    return {
        "registry": registry,
        "container": None,
        "system_layers": [],
    }
