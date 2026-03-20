"""Zero-overhead noop tracer and span for when tracing is disabled."""

from __future__ import annotations

from typing import Any

from celerity.types.telemetry import CeleritySpan, CelerityTracer


class NoopSpan(CeleritySpan):
    """Span that does nothing. All methods are no-ops."""

    def set_attribute(self, key: str, value: str | int | float | bool) -> None:
        pass

    def set_attributes(self, attributes: dict[str, str | int | float | bool]) -> None:
        pass

    def record_error(self, error: Exception) -> None:
        pass

    def set_ok(self) -> None:
        pass

    def end(self) -> None:
        pass


NOOP_SPAN = NoopSpan()
"""Singleton noop span instance."""


class NoopTracer(CelerityTracer):
    """Tracer that creates no real spans.

    Used when ``CELERITY_TELEMETRY_ENABLED`` is not ``true``.
    """

    def start_span(self, name: str, attributes: dict[str, Any] | None = None) -> CeleritySpan:
        return NOOP_SPAN

    async def with_span(
        self,
        name: str,
        fn: Any,
        attributes: dict[str, Any] | None = None,
    ) -> Any:
        return await fn(NOOP_SPAN)
