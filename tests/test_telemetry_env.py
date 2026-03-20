"""Tests for telemetry environment configuration."""

from __future__ import annotations

import pytest

from celerity.telemetry.env import read_telemetry_env


class TestReadTelemetryEnv:
    def test_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_TELEMETRY_ENABLED", raising=False)
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
        monkeypatch.delenv("CELERITY_TRACE_OTLP_COLLECTOR_ENDPOINT", raising=False)
        monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)
        monkeypatch.delenv("OTEL_SERVICE_VERSION", raising=False)
        monkeypatch.delenv("CELERITY_LOG_LEVEL", raising=False)
        monkeypatch.delenv("CELERITY_LOG_FORMAT", raising=False)
        monkeypatch.delenv("CELERITY_LOG_FILE_PATH", raising=False)
        monkeypatch.delenv("CELERITY_LOG_REDACT_KEYS", raising=False)
        monkeypatch.delenv("CELERITY_PLATFORM", raising=False)

        config = read_telemetry_env()
        assert config.tracing_enabled is False
        assert config.otlp_endpoint == "http://otelcollector:4317"
        assert config.service_name == "celerity-app"
        assert config.service_version == "0.0.0"
        assert config.log_level == "info"
        assert config.log_format == "json"  # auto resolves to json when not local
        assert config.log_file_path is None
        assert config.log_redact_keys == []

    def test_tracing_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_TELEMETRY_ENABLED", "true")
        config = read_telemetry_env()
        assert config.tracing_enabled is True

    def test_tracing_enabled_case_insensitive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_TELEMETRY_ENABLED", "True")
        config = read_telemetry_env()
        assert config.tracing_enabled is True

    def test_tracing_disabled_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_TELEMETRY_ENABLED", raising=False)
        config = read_telemetry_env()
        assert config.tracing_enabled is False

    def test_otlp_endpoint_from_otel_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://custom:4317")
        config = read_telemetry_env()
        assert config.otlp_endpoint == "http://custom:4317"

    def test_otlp_endpoint_from_celerity_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
        monkeypatch.setenv("CELERITY_TRACE_OTLP_COLLECTOR_ENDPOINT", "http://celerity:4317")
        config = read_telemetry_env()
        assert config.otlp_endpoint == "http://celerity:4317"

    def test_otel_endpoint_takes_precedence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel:4317")
        monkeypatch.setenv("CELERITY_TRACE_OTLP_COLLECTOR_ENDPOINT", "http://celerity:4317")
        config = read_telemetry_env()
        assert config.otlp_endpoint == "http://otel:4317"

    def test_service_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTEL_SERVICE_NAME", "my-service")
        config = read_telemetry_env()
        assert config.service_name == "my-service"

    def test_service_version(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("OTEL_SERVICE_VERSION", "1.2.3")
        config = read_telemetry_env()
        assert config.service_version == "1.2.3"

    def test_log_level(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_LOG_LEVEL", "debug")
        config = read_telemetry_env()
        assert config.log_level == "debug"

    def test_log_format_json_explicit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_LOG_FORMAT", "json")
        config = read_telemetry_env()
        assert config.log_format == "json"

    def test_log_format_human_explicit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_LOG_FORMAT", "human")
        config = read_telemetry_env()
        assert config.log_format == "human"

    def test_log_format_auto_local_platform(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_LOG_FORMAT", raising=False)
        monkeypatch.setenv("CELERITY_PLATFORM", "local")
        config = read_telemetry_env()
        assert config.log_format == "human"

    def test_log_format_auto_aws_platform(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_LOG_FORMAT", raising=False)
        monkeypatch.setenv("CELERITY_PLATFORM", "aws")
        config = read_telemetry_env()
        assert config.log_format == "json"

    def test_log_file_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_LOG_FILE_PATH", "/var/log/app.log")
        config = read_telemetry_env()
        assert config.log_file_path == "/var/log/app.log"

    def test_redact_keys(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_LOG_REDACT_KEYS", "password,secret,token")
        config = read_telemetry_env()
        assert config.log_redact_keys == ["password", "secret", "token"]

    def test_redact_keys_strips_whitespace(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_LOG_REDACT_KEYS", " password , secret ")
        config = read_telemetry_env()
        assert config.log_redact_keys == ["password", "secret"]

    def test_redact_keys_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_LOG_REDACT_KEYS", raising=False)
        config = read_telemetry_env()
        assert config.log_redact_keys == []
