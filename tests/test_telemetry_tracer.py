"""Tests for OTelTracer and OTelSpan."""

from __future__ import annotations

from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult

from celerity.telemetry.tracer import OTelSpan, OTelTracer
from celerity.types.telemetry import CeleritySpan, CelerityTracer


class _InMemoryExporter(SpanExporter):
    """Simple in-memory span exporter for tests."""

    def __init__(self) -> None:
        self.spans: list[ReadableSpan] = []

    def export(self, spans: Any) -> SpanExportResult:
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 0) -> bool:
        return True


def _setup_tracing() -> tuple[TracerProvider, _InMemoryExporter]:
    """Create a fresh TracerProvider with an in-memory exporter."""
    provider = TracerProvider()
    exporter = _InMemoryExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider, exporter


class TestOTelTracer:
    def test_is_celerity_tracer(self) -> None:
        t = OTelTracer()
        assert isinstance(t, CelerityTracer)

    def test_start_span_returns_celerity_span(self) -> None:
        t = OTelTracer()
        span = t.start_span("test-span")
        assert isinstance(span, CeleritySpan)
        assert isinstance(span, OTelSpan)
        span.end()

    @pytest.mark.asyncio
    async def test_with_span_success(self) -> None:
        provider, exporter = _setup_tracing()
        tracer = provider.get_tracer("test")
        otel_tracer = OTelTracer.__new__(OTelTracer)
        otel_tracer._tracer = tracer

        async def fn(span: CeleritySpan) -> str:
            span.set_attribute("custom", "value")
            return "ok"

        result = await otel_tracer.with_span("test-op", fn, attributes={"initial": "attr"})
        assert result == "ok"

        assert len(exporter.spans) == 1
        assert exporter.spans[0].name == "test-op"
        assert exporter.spans[0].status.status_code == trace.StatusCode.OK

    @pytest.mark.asyncio
    async def test_with_span_error(self) -> None:
        provider, exporter = _setup_tracing()
        tracer = provider.get_tracer("test")
        otel_tracer = OTelTracer.__new__(OTelTracer)
        otel_tracer._tracer = tracer

        async def fn(span: CeleritySpan) -> str:
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            await otel_tracer.with_span("failing-op", fn)

        assert len(exporter.spans) == 1
        assert exporter.spans[0].status.status_code == trace.StatusCode.ERROR

    @pytest.mark.asyncio
    async def test_with_span_passes_attributes(self) -> None:
        provider, exporter = _setup_tracing()
        tracer = provider.get_tracer("test")
        otel_tracer = OTelTracer.__new__(OTelTracer)
        otel_tracer._tracer = tracer

        async def fn(span: CeleritySpan) -> str:
            return "done"

        await otel_tracer.with_span("op", fn, attributes={"db.table": "orders"})

        assert len(exporter.spans) == 1
        assert exporter.spans[0].attributes["db.table"] == "orders"  # type: ignore[index]


class TestOTelSpan:
    def test_set_attribute(self) -> None:
        provider, exporter = _setup_tracing()
        raw_span = provider.get_tracer("test").start_span("test")
        span = OTelSpan(raw_span)
        span.set_attribute("key", "value")
        span.set_attributes({"a": 1, "b": True})
        span.set_ok()
        span.end()

        assert len(exporter.spans) == 1
        assert exporter.spans[0].attributes["key"] == "value"  # type: ignore[index]
        assert exporter.spans[0].attributes["a"] == 1  # type: ignore[index]
        assert exporter.spans[0].attributes["b"] is True  # type: ignore[index]

    def test_record_error(self) -> None:
        provider, exporter = _setup_tracing()
        raw_span = provider.get_tracer("test").start_span("test")
        span = OTelSpan(raw_span)
        span.record_error(RuntimeError("boom"))
        span.end()

        assert len(exporter.spans) == 1
        assert exporter.spans[0].status.status_code == trace.StatusCode.ERROR
        assert "boom" in exporter.spans[0].status.description  # type: ignore[operator]
