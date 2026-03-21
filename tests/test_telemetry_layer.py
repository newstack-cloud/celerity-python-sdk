"""Tests for TelemetryLayer."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from celerity.telemetry.helpers import LOGGER_TOKEN, TRACER_TOKEN
from celerity.telemetry.noop import NoopTracer
from celerity.telemetry.request_context import get_request_logger
from celerity.types.context import BaseHandlerContext
from celerity.types.telemetry import CelerityLogger


class FakeContainer:
    def __init__(self) -> None:
        self._registry: dict[str, object] = {}

    def register_value(self, token: str, value: object) -> None:
        self._registry[token] = value

    async def resolve(self, token: str) -> object:
        if token not in self._registry:
            raise KeyError(token)
        return self._registry[token]


def _make_ctx(container: FakeContainer) -> BaseHandlerContext:
    return BaseHandlerContext(
        metadata=MagicMock(),
        container=container,  # type: ignore[arg-type]
    )


class TestTelemetryLayerInit:
    @pytest.mark.asyncio
    async def test_registers_logger_and_tracer(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("CELERITY_TELEMETRY_ENABLED", raising=False)

        from celerity.telemetry.telemetry_layer import TelemetryLayer

        layer = TelemetryLayer()
        container = FakeContainer()
        next_handler = AsyncMock(return_value="result")

        result = await layer.handle(_make_ctx(container), next_handler)

        assert result == "result"
        next_handler.assert_awaited_once()
        assert LOGGER_TOKEN in container._registry
        assert TRACER_TOKEN in container._registry

    @pytest.mark.asyncio
    async def test_noop_tracer_when_tracing_disabled(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("CELERITY_TELEMETRY_ENABLED", raising=False)

        from celerity.telemetry.telemetry_layer import TelemetryLayer

        layer = TelemetryLayer()
        container = FakeContainer()
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(_make_ctx(container), next_handler)

        tracer = container._registry[TRACER_TOKEN]
        assert isinstance(tracer, NoopTracer)

    @pytest.mark.asyncio
    async def test_idempotent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_TELEMETRY_ENABLED", raising=False)

        from celerity.telemetry.telemetry_layer import TelemetryLayer

        layer = TelemetryLayer()
        container = FakeContainer()
        ctx = _make_ctx(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)
        await layer.handle(ctx, next_handler)

        assert layer._initialized is True


class TestTelemetryLayerRequestScope:
    @pytest.mark.asyncio
    async def test_sets_request_logger_during_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("CELERITY_TELEMETRY_ENABLED", raising=False)

        from celerity.telemetry.telemetry_layer import TelemetryLayer

        layer = TelemetryLayer()
        container = FakeContainer()

        captured_logger = None

        async def handler() -> str:
            nonlocal captured_logger
            captured_logger = get_request_logger()
            return "ok"

        await layer.handle(_make_ctx(container), handler)

        assert captured_logger is not None
        assert get_request_logger() is None

    @pytest.mark.asyncio
    async def test_attaches_logger_to_context(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("CELERITY_TELEMETRY_ENABLED", raising=False)

        from celerity.telemetry.telemetry_layer import TelemetryLayer

        layer = TelemetryLayer()
        container = FakeContainer()
        ctx = _make_ctx(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)

        assert ctx.logger is not None

    @pytest.mark.asyncio
    async def test_clears_request_logger_on_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("CELERITY_TELEMETRY_ENABLED", raising=False)

        from celerity.telemetry.telemetry_layer import TelemetryLayer

        layer = TelemetryLayer()
        container = FakeContainer()

        async def failing_handler() -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await layer.handle(_make_ctx(container), failing_handler)

        assert get_request_logger() is None


class TestTelemetryLayerHttpContext:
    @pytest.mark.asyncio
    async def test_http_context_bindings(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("CELERITY_TELEMETRY_ENABLED", raising=False)

        from celerity.telemetry.telemetry_layer import TelemetryLayer
        from celerity.types.context import HttpHandlerContext
        from celerity.types.http import HttpRequest

        layer = TelemetryLayer()
        container = FakeContainer()
        request = HttpRequest(
            method="GET",
            path="/api/items",
            request_id="req-123",
            matched_route="/api/items",
            client_ip="127.0.0.1",
            user_agent="test-agent",
        )
        ctx = HttpHandlerContext(
            metadata=MagicMock(),
            container=container,  # type: ignore[arg-type]
            request=request,
        )

        captured_logger = None

        async def handler() -> str:
            nonlocal captured_logger
            captured_logger = get_request_logger()
            return "ok"

        await layer.handle(ctx, handler)

        assert captured_logger is not None
        assert isinstance(captured_logger, CelerityLogger)


class TestTelemetryLayerConsumerContext:
    @pytest.mark.asyncio
    async def test_consumer_context_bindings(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("CELERITY_TELEMETRY_ENABLED", raising=False)

        from celerity.telemetry.telemetry_layer import TelemetryLayer
        from celerity.types.consumer import ConsumerEventInput, ConsumerMessage
        from celerity.types.context import ConsumerHandlerContext

        layer = TelemetryLayer()
        container = FakeContainer()
        event = ConsumerEventInput(
            handler_tag="orders-queue",
            messages=[
                ConsumerMessage(message_id="m1", body="msg1", source="sqs"),
                ConsumerMessage(message_id="m2", body="msg2", source="sqs"),
            ],
        )
        ctx = ConsumerHandlerContext(
            metadata=MagicMock(),
            container=container,  # type: ignore[arg-type]
            event=event,
        )

        captured_logger = None

        async def handler() -> str:
            nonlocal captured_logger
            captured_logger = get_request_logger()
            return "ok"

        await layer.handle(ctx, handler)

        assert captured_logger is not None
        assert isinstance(captured_logger, CelerityLogger)


class TestTelemetryLayerDispose:
    @pytest.mark.asyncio
    async def test_dispose_no_tracing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_TELEMETRY_ENABLED", raising=False)

        from celerity.telemetry.telemetry_layer import TelemetryLayer

        layer = TelemetryLayer()
        await layer.dispose()
