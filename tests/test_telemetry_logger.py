"""Tests for structlog-based CelerityLoggerImpl."""

from __future__ import annotations

import io
import json
import logging
from typing import Any, cast

import structlog

from celerity.telemetry.env import TelemetryConfig
from celerity.telemetry.logger import CelerityLoggerImpl, create_logger
from celerity.types.telemetry import CelerityLogger


def _capture_logger(config: TelemetryConfig) -> tuple[CelerityLoggerImpl, io.StringIO]:
    """Create a logger that writes to a StringIO buffer for test capture."""
    buf = io.StringIO()

    # Reset structlog and stdlib logging for test isolation
    structlog.reset_defaults()
    root = logging.getLogger()
    root.handlers.clear()

    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(handler)
    root.setLevel(config.log_level.upper())

    processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.UnicodeDecoder(),
        structlog.processors.EventRenamer("msg"),
    ]

    if config.log_redact_keys:
        from celerity.telemetry.logger import _make_redactor

        processors.append(_make_redactor(set(config.log_redact_keys)))

    processors.append(structlog.processors.JSONRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )

    logger = CelerityLoggerImpl(structlog.get_logger("celerity"))
    return logger, buf


def _get_output(buf: io.StringIO) -> str:
    return buf.getvalue().strip()


def _parse_output(buf: io.StringIO) -> dict[str, Any]:
    line = _get_output(buf)
    # Take the last line if multiple (structlog may output multiple)
    lines = [part for part in line.split("\n") if part.strip()]
    return cast("dict[str, Any]", json.loads(lines[-1]))


class TestCreateLogger:
    def test_returns_celerity_logger(self) -> None:
        config = TelemetryConfig(log_format="json")
        logger = create_logger(config)
        assert isinstance(logger, CelerityLogger)
        assert isinstance(logger, CelerityLoggerImpl)


class TestCelerityLoggerImpl:
    def test_json_output(self) -> None:
        logger, buf = _capture_logger(TelemetryConfig(log_format="json", log_level="info"))
        logger.info("hello world", extra_key="extra_val")
        parsed = _parse_output(buf)
        assert parsed["msg"] == "hello world"
        assert "level" in parsed
        assert parsed["extra_key"] == "extra_val"

    def test_debug_suppressed_at_info_level(self) -> None:
        logger, buf = _capture_logger(TelemetryConfig(log_format="json", log_level="info"))
        logger.debug("should not appear")
        assert _get_output(buf) == ""

    def test_debug_visible_at_debug_level(self) -> None:
        logger, buf = _capture_logger(TelemetryConfig(log_format="json", log_level="debug"))
        logger.debug("should appear")
        assert "should appear" in _get_output(buf)

    def test_child_binds_logger_name(self) -> None:
        logger, buf = _capture_logger(TelemetryConfig(log_format="json", log_level="debug"))
        child = logger.child("orders")
        child.info("order created")
        parsed = _parse_output(buf)
        assert parsed["logger_name"] == "orders"

    def test_with_context_binds_attributes(self) -> None:
        logger, buf = _capture_logger(TelemetryConfig(log_format="json", log_level="debug"))
        ctx_logger = logger.with_context(request_id="abc-123")
        ctx_logger.info("handling request")
        parsed = _parse_output(buf)
        assert parsed["request_id"] == "abc-123"

    def test_warn_method(self) -> None:
        logger, buf = _capture_logger(TelemetryConfig(log_format="json", log_level="debug"))
        logger.warn("something suspicious")
        parsed = _parse_output(buf)
        assert parsed["msg"] == "something suspicious"
        assert parsed["level"] == "warning"

    def test_error_method(self) -> None:
        logger, buf = _capture_logger(TelemetryConfig(log_format="json", log_level="debug"))
        logger.error("something broke")
        parsed = _parse_output(buf)
        assert parsed["msg"] == "something broke"
        assert parsed["level"] == "error"

    def test_msg_field_name(self) -> None:
        """JSON output uses 'msg' not 'event' for CLI compatibility."""
        logger, buf = _capture_logger(TelemetryConfig(log_format="json", log_level="info"))
        logger.info("test message")
        parsed = _parse_output(buf)
        assert "msg" in parsed
        assert "event" not in parsed


class TestRedaction:
    def test_redacts_specified_keys(self) -> None:
        logger, buf = _capture_logger(
            TelemetryConfig(
                log_format="json", log_level="info", log_redact_keys=["password", "secret"]
            )
        )
        logger.info("login", password="hunter2", secret="s3cr3t", username="alice")
        parsed = _parse_output(buf)
        assert parsed["password"] == "[REDACTED]"
        assert parsed["secret"] == "[REDACTED]"
        assert parsed["username"] == "alice"

    def test_non_redacted_keys_unchanged(self) -> None:
        logger, buf = _capture_logger(
            TelemetryConfig(log_format="json", log_level="info", log_redact_keys=["password"])
        )
        logger.info("request", method="GET", path="/api")
        parsed = _parse_output(buf)
        assert parsed["method"] == "GET"
        assert parsed["path"] == "/api"
