"""Tests for NoopTracer and NoopSpan."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from celerity.telemetry.noop import NOOP_SPAN, NoopSpan, NoopTracer

if TYPE_CHECKING:
    from celerity.types.telemetry import CeleritySpan


class TestNoopSpan:
    def test_singleton(self) -> None:
        assert isinstance(NOOP_SPAN, NoopSpan)

    def test_set_attribute_is_noop(self) -> None:
        NOOP_SPAN.set_attribute("key", "value")

    def test_set_attributes_is_noop(self) -> None:
        NOOP_SPAN.set_attributes({"key": "value"})

    def test_record_error_is_noop(self) -> None:
        NOOP_SPAN.record_error(RuntimeError("test"))

    def test_set_ok_is_noop(self) -> None:
        NOOP_SPAN.set_ok()

    def test_end_is_noop(self) -> None:
        NOOP_SPAN.end()


class TestNoopTracer:
    def test_start_span_returns_noop(self) -> None:
        tracer = NoopTracer()
        span = tracer.start_span("test")
        assert span is NOOP_SPAN

    def test_start_span_with_attributes(self) -> None:
        tracer = NoopTracer()
        span = tracer.start_span("test", attributes={"key": "value"})
        assert span is NOOP_SPAN

    @pytest.mark.asyncio
    async def test_with_span_calls_fn(self) -> None:
        tracer = NoopTracer()
        called = False

        async def fn(span: CeleritySpan) -> str:
            nonlocal called
            called = True
            assert span is NOOP_SPAN
            return "result"

        result = await tracer.with_span("test", fn)
        assert called is True
        assert result == "result"

    @pytest.mark.asyncio
    async def test_with_span_propagates_exception(self) -> None:
        tracer = NoopTracer()

        async def fn(span: CeleritySpan) -> None:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await tracer.with_span("test", fn)
