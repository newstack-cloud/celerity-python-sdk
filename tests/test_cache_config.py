"""Tests for cache connection configuration."""

from __future__ import annotations

from celerity.resources.cache.config import (
    FUNCTIONS_CONNECTION,
    RUNTIME_CONNECTION,
    resolve_connection_config,
)


class TestConnectionPresets:
    def test_functions_preset(self) -> None:
        assert FUNCTIONS_CONNECTION.connect_timeout_ms == 5_000
        assert FUNCTIONS_CONNECTION.command_timeout_ms == 5_000
        assert FUNCTIONS_CONNECTION.keep_alive_ms == 0
        assert FUNCTIONS_CONNECTION.max_retries == 2
        assert FUNCTIONS_CONNECTION.retry_delay_ms == 100
        assert FUNCTIONS_CONNECTION.lazy_connect is True

    def test_runtime_preset(self) -> None:
        assert RUNTIME_CONNECTION.connect_timeout_ms == 10_000
        assert RUNTIME_CONNECTION.command_timeout_ms == 0
        assert RUNTIME_CONNECTION.keep_alive_ms == 30_000
        assert RUNTIME_CONNECTION.max_retries == 10
        assert RUNTIME_CONNECTION.retry_delay_ms == 500
        assert RUNTIME_CONNECTION.lazy_connect is False


class TestResolveConnectionConfig:
    def test_functions_no_overrides(self) -> None:
        config = resolve_connection_config("functions")
        assert config == FUNCTIONS_CONNECTION

    def test_runtime_no_overrides(self) -> None:
        config = resolve_connection_config("runtime")
        assert config == RUNTIME_CONNECTION

    def test_with_overrides(self) -> None:
        config = resolve_connection_config(
            "functions",
            overrides={
                "connectTimeoutMs": "3000",
                "maxRetries": "5",
            },
        )
        assert config.connect_timeout_ms == 3000
        assert config.max_retries == 5
        # Unchanged fields from preset
        assert config.command_timeout_ms == 5_000
        assert config.lazy_connect is True

    def test_lazy_connect_override(self) -> None:
        config = resolve_connection_config(
            "functions",
            overrides={"lazyConnect": "false"},
        )
        assert config.lazy_connect is False

    def test_empty_overrides(self) -> None:
        config = resolve_connection_config("functions", overrides={})
        assert config == FUNCTIONS_CONNECTION

    def test_unknown_override_key_ignored(self) -> None:
        config = resolve_connection_config(
            "functions",
            overrides={"unknownKey": "123"},
        )
        assert config == FUNCTIONS_CONNECTION
