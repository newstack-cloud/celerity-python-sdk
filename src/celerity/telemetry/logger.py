"""structlog-based logger implementation.

Uses structlog for structured JSON output (production) and
human-readable colored output (local development).

JSON output uses ``msg`` and ``level`` field names for compatibility
with the Celerity CLI log parser.
"""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, Any

import structlog

from celerity.types.telemetry import CelerityLogger

if TYPE_CHECKING:
    from celerity.telemetry.env import TelemetryConfig

# Map CelerityLogger level names to stdlib logging levels
_LEVEL_MAP: dict[str, int] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class CelerityLoggerImpl(CelerityLogger):
    """Production logger backed by structlog."""

    def __init__(self, logger: structlog.stdlib.BoundLogger) -> None:
        self._logger = logger

    def debug(self, message: str, **attributes: Any) -> None:
        self._logger.debug(message, **attributes)

    def info(self, message: str, **attributes: Any) -> None:
        self._logger.info(message, **attributes)

    def warn(self, message: str, **attributes: Any) -> None:
        self._logger.warning(message, **attributes)

    def error(self, message: str, **attributes: Any) -> None:
        self._logger.error(message, **attributes)

    def child(self, name: str, **attributes: Any) -> CelerityLogger:
        return CelerityLoggerImpl(self._logger.bind(logger_name=name, **attributes))

    def with_context(self, **attributes: Any) -> CelerityLogger:
        return CelerityLoggerImpl(self._logger.bind(**attributes))

    def set_level(self, level: str) -> None:
        """Dynamically change log level at runtime."""
        stdlib_level = _LEVEL_MAP.get(level.lower(), logging.INFO)
        logging.getLogger().setLevel(stdlib_level)


def create_logger(config: TelemetryConfig) -> CelerityLoggerImpl:
    """Create and configure the root logger.

    Output format is determined by ``config.log_format``:

    - ``"json"`` -- structured JSON to stdout (production, CLI-compatible)
    - ``"human"`` -- colored console output (local development)
    """
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if config.log_redact_keys:
        redact_set = set(config.log_redact_keys)
        processors.append(_make_redactor(redact_set))

    # Rename "event" to "msg" for CLI compatibility
    processors.append(structlog.processors.EventRenamer("msg"))

    if config.log_format == "human":
        processors.append(structlog.dev.ConsoleRenderer())
    else:
        processors.append(structlog.processors.JSONRenderer())

    stdlib_level = _LEVEL_MAP.get(config.log_level.lower(), logging.INFO)

    # Configure stdlib logging as the backend
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=stdlib_level,
        force=True,
    )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    return CelerityLoggerImpl(structlog.get_logger("celerity"))


def _make_redactor(keys: set[str]) -> Any:
    """Create a structlog processor that redacts specified keys."""

    def redact(_logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        for key in keys:
            if key in event_dict:
                event_dict[key] = "[REDACTED]"
        return event_dict

    return redact
