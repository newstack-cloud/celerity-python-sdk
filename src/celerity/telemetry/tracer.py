"""OpenTelemetry tracer wrapper."""

from __future__ import annotations

from typing import Any

from opentelemetry import trace

from celerity.types.telemetry import CeleritySpan, CelerityTracer


class OTelTracer(CelerityTracer):
    """Production tracer backed by OpenTelemetry."""

    def __init__(self, tracer_name: str = "celerity") -> None:
        self._tracer = trace.get_tracer(tracer_name)

    def start_span(self, name: str, attributes: dict[str, Any] | None = None) -> CeleritySpan:
        span = self._tracer.start_span(name, attributes=attributes)
        return OTelSpan(span)

    async def with_span(
        self,
        name: str,
        fn: Any,
        attributes: dict[str, Any] | None = None,
    ) -> Any:
        with self._tracer.start_as_current_span(name, attributes=attributes) as span:
            otel_span = OTelSpan(span)
            try:
                result = await fn(otel_span)
                otel_span.set_ok()
                return result
            except Exception as e:
                otel_span.record_error(e)
                raise


class OTelSpan(CeleritySpan):
    """Wrapper around an OpenTelemetry Span."""

    def __init__(self, span: trace.Span) -> None:
        self._span = span

    def set_attribute(self, key: str, value: str | int | float | bool) -> None:
        self._span.set_attribute(key, value)

    def set_attributes(self, attributes: dict[str, str | int | float | bool]) -> None:
        self._span.set_attributes(attributes)

    def record_error(self, error: Exception) -> None:
        self._span.set_status(trace.StatusCode.ERROR, str(error))
        self._span.record_exception(error)

    def set_ok(self) -> None:
        self._span.set_status(trace.StatusCode.OK)

    def end(self) -> None:
        self._span.end()
