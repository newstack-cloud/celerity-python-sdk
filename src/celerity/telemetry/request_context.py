"""Request-scoped logger via contextvars.

Uses Python's ``contextvars`` (PEP 567) which is natively supported
by asyncio. Each handler invocation gets its own context automatically.
"""

from __future__ import annotations

import contextvars
from typing import Any

from celerity.types.telemetry import CelerityLogger

_request_logger: contextvars.ContextVar[CelerityLogger | None] = contextvars.ContextVar(
    "celerity_request_logger", default=None
)


def get_request_logger() -> CelerityLogger | None:
    """Get the request-scoped logger for the current async context.

    Returns ``None`` outside of a request handler.
    """
    return _request_logger.get()


def set_request_logger(logger: CelerityLogger) -> contextvars.Token[CelerityLogger | None]:
    """Set the request-scoped logger. Called by TelemetryLayer."""
    return _request_logger.set(logger)


def clear_request_logger() -> None:
    """Clear the request-scoped logger."""
    _request_logger.set(None)


class ContextAwareLogger(CelerityLogger):
    """Logger that delegates to the request-scoped logger if available,
    falling back to the root logger.

    Registered under ``LOGGER_TOKEN`` in the DI container so that services
    injected with the logger automatically get request-scoped logging
    during handler execution and root-level logging during startup.
    """

    def __init__(self, root_logger: CelerityLogger) -> None:
        self._root = root_logger

    def _active(self) -> CelerityLogger:
        return get_request_logger() or self._root

    def debug(self, message: str, **attributes: Any) -> None:
        self._active().debug(message, **attributes)

    def info(self, message: str, **attributes: Any) -> None:
        self._active().info(message, **attributes)

    def warn(self, message: str, **attributes: Any) -> None:
        self._active().warn(message, **attributes)

    def error(self, message: str, **attributes: Any) -> None:
        self._active().error(message, **attributes)

    def child(self, name: str, **attributes: Any) -> CelerityLogger:
        return self._active().child(name, **attributes)

    def with_context(self, **attributes: Any) -> CelerityLogger:
        return self._active().with_context(**attributes)
