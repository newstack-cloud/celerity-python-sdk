"""Telemetry package: logging, tracing, and request-scoped context."""

from celerity.telemetry.context import extract_trace_context
from celerity.telemetry.env import TelemetryConfig, read_telemetry_env
from celerity.telemetry.helpers import LOGGER_TOKEN, TRACER_TOKEN, get_logger, get_tracer
from celerity.telemetry.init import init_telemetry, is_initialized, shutdown_telemetry
from celerity.telemetry.logger import CelerityLoggerImpl, create_logger
from celerity.telemetry.noop import NOOP_SPAN, NoopSpan, NoopTracer
from celerity.telemetry.request_context import (
    ContextAwareLogger,
    clear_request_logger,
    get_request_logger,
    set_request_logger,
)
from celerity.telemetry.telemetry_layer import TelemetryLayer
from celerity.telemetry.tracer import OTelSpan, OTelTracer
from celerity.types.telemetry import CelerityLogger, CeleritySpan, CelerityTracer

__all__ = [
    "LOGGER_TOKEN",
    "NOOP_SPAN",
    "TRACER_TOKEN",
    "CelerityLogger",
    "CelerityLoggerImpl",
    "CeleritySpan",
    "CelerityTracer",
    "ContextAwareLogger",
    "NoopSpan",
    "NoopTracer",
    "OTelSpan",
    "OTelTracer",
    "TelemetryConfig",
    "TelemetryLayer",
    "clear_request_logger",
    "create_logger",
    "extract_trace_context",
    "get_logger",
    "get_request_logger",
    "get_tracer",
    "init_telemetry",
    "is_initialized",
    "read_telemetry_env",
    "set_request_logger",
    "shutdown_telemetry",
]
