"""Tests for trace context extraction."""

from __future__ import annotations

from celerity.telemetry.context import extract_trace_context


class TestExtractTraceContext:
    def test_returns_none_for_none(self) -> None:
        assert extract_trace_context(None) is None

    def test_returns_none_for_empty_dict(self) -> None:
        assert extract_trace_context({}) is None

    def test_extracts_w3c_traceparent(self) -> None:
        carrier = {
            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        }
        ctx = extract_trace_context(carrier)
        # Should return an OTel Context object (not None)
        assert ctx is not None
