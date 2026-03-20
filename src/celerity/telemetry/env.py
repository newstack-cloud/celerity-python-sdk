"""Telemetry configuration from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class TelemetryConfig:
    """Telemetry configuration resolved from environment variables."""

    tracing_enabled: bool = False
    otlp_endpoint: str = "http://otelcollector:4317"
    service_name: str = "celerity-app"
    service_version: str = "0.0.0"
    log_level: str = "info"
    log_format: str = "auto"
    log_file_path: str | None = None
    log_redact_keys: list[str] = field(default_factory=list)


def read_telemetry_env() -> TelemetryConfig:
    """Read telemetry configuration from environment variables.

    Environment variables:

    - ``tracing_enabled`` -- CELERITY_TELEMETRY_ENABLED (default: False)
    - ``otlp_endpoint`` -- OTEL_EXPORTER_OTLP_ENDPOINT or
      CELERITY_TRACE_OTLP_COLLECTOR_ENDPOINT (default: http://otelcollector:4317)
    - ``service_name`` -- OTEL_SERVICE_NAME (default: "celerity-app")
    - ``service_version`` -- OTEL_SERVICE_VERSION (default: "0.0.0")
    - ``log_level`` -- CELERITY_LOG_LEVEL (default: "info")
    - ``log_format`` -- CELERITY_LOG_FORMAT (default: "auto")
    - ``log_file_path`` -- CELERITY_LOG_FILE_PATH (default: None)
    - ``log_redact_keys`` -- CELERITY_LOG_REDACT_KEYS, comma-separated (default: [])

    Log format auto-detection resolves ``"auto"`` to ``"human"`` only
    when ``CELERITY_PLATFORM`` is ``"local"`` and ``CELERITY_LOG_FORMAT``
    is not explicitly set; otherwise it resolves to ``"json"``.
    """
    tracing_enabled = os.environ.get("CELERITY_TELEMETRY_ENABLED", "").lower() == "true"

    otlp_endpoint = (
        os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        or os.environ.get("CELERITY_TRACE_OTLP_COLLECTOR_ENDPOINT")
        or "http://otelcollector:4317"
    )

    log_format = os.environ.get("CELERITY_LOG_FORMAT", "auto")
    if log_format == "auto":
        platform = os.environ.get("CELERITY_PLATFORM", "")
        log_format = "human" if platform == "local" else "json"

    redact_raw = os.environ.get("CELERITY_LOG_REDACT_KEYS", "")
    redact_keys = [k.strip() for k in redact_raw.split(",") if k.strip()] if redact_raw else []

    return TelemetryConfig(
        tracing_enabled=tracing_enabled,
        otlp_endpoint=otlp_endpoint,
        service_name=os.environ.get("OTEL_SERVICE_NAME", "celerity-app"),
        service_version=os.environ.get("OTEL_SERVICE_VERSION", "0.0.0"),
        log_level=os.environ.get("CELERITY_LOG_LEVEL", "info"),
        log_format=log_format,
        log_file_path=os.environ.get("CELERITY_LOG_FILE_PATH"),
        log_redact_keys=redact_keys,
    )
