"""Telemetry type definitions: logger, tracer, and span ABCs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class CelerityLogger(ABC):
    """Structured logger with context enrichment.

    Always available regardless of whether tracing is enabled.
    Request-scoped: each handler invocation gets a logger enriched
    with request metadata (request ID, method, path, trace context).
    """

    @abstractmethod
    def debug(self, message: str, **attributes: Any) -> None: ...

    @abstractmethod
    def info(self, message: str, **attributes: Any) -> None: ...

    @abstractmethod
    def warn(self, message: str, **attributes: Any) -> None: ...

    @abstractmethod
    def error(self, message: str, **attributes: Any) -> None: ...

    @abstractmethod
    def child(self, name: str, **attributes: Any) -> CelerityLogger:
        """Create a child logger with bound attributes."""

    @abstractmethod
    def with_context(self, **attributes: Any) -> CelerityLogger:
        """Create a new logger with additional bound context."""


class CelerityTracer(ABC):
    """Distributed tracer for custom span capture.

    Wraps OpenTelemetry tracing. When tracing is disabled,
    a NoopTracer is used that has zero overhead.
    """

    @abstractmethod
    def start_span(self, name: str, attributes: dict[str, Any] | None = None) -> CeleritySpan: ...

    @abstractmethod
    async def with_span(
        self,
        name: str,
        fn: Any,
        attributes: dict[str, Any] | None = None,
    ) -> Any:
        """Execute ``fn(span)`` within a new span, handling lifecycle and errors."""


class CeleritySpan(ABC):
    """A single trace span."""

    @abstractmethod
    def set_attribute(self, key: str, value: str | int | float | bool) -> None: ...

    @abstractmethod
    def set_attributes(self, attributes: dict[str, str | int | float | bool]) -> None: ...

    @abstractmethod
    def record_error(self, error: Exception) -> None: ...

    @abstractmethod
    def set_ok(self) -> None: ...

    @abstractmethod
    def end(self) -> None: ...
