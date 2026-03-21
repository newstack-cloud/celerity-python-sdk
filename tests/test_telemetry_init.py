"""Tests for OTel SDK initialization."""

from __future__ import annotations

import pytest

from celerity.telemetry.init import is_initialized


class TestIsInitialized:
    def test_initially_false(self) -> None:
        # Reset module state for clean test
        import celerity.telemetry.init as init_mod

        original = init_mod._initialized
        try:
            init_mod._initialized = False
            assert is_initialized() is False
        finally:
            init_mod._initialized = original

    @pytest.mark.asyncio
    async def test_init_and_shutdown_cycle(self) -> None:
        """Test init/shutdown without a real OTLP endpoint."""
        import celerity.telemetry.init as init_mod
        from celerity.telemetry.env import TelemetryConfig
        from celerity.telemetry.init import init_telemetry, shutdown_telemetry

        original_init = init_mod._initialized
        original_provider = init_mod._tracer_provider
        try:
            init_mod._initialized = False
            init_mod._tracer_provider = None

            config = TelemetryConfig(
                tracing_enabled=True,
                otlp_endpoint="http://localhost:9999",
                service_name="test-service",
                service_version="1.0.0",
            )

            await init_telemetry(config)
            assert is_initialized() is True

            await shutdown_telemetry()
            assert is_initialized() is False
        finally:
            init_mod._initialized = original_init
            init_mod._tracer_provider = original_provider

    @pytest.mark.asyncio
    async def test_init_idempotent(self) -> None:
        """Calling init_telemetry twice does not re-initialise."""
        import celerity.telemetry.init as init_mod
        from celerity.telemetry.env import TelemetryConfig
        from celerity.telemetry.init import init_telemetry, shutdown_telemetry

        original_init = init_mod._initialized
        original_provider = init_mod._tracer_provider
        try:
            init_mod._initialized = False
            init_mod._tracer_provider = None

            config = TelemetryConfig(
                tracing_enabled=True,
                otlp_endpoint="http://localhost:9999",
            )

            await init_telemetry(config)
            provider_after_first = init_mod._tracer_provider

            await init_telemetry(config)
            # Same provider, not re-created
            assert init_mod._tracer_provider is provider_after_first

            await shutdown_telemetry()
        finally:
            init_mod._initialized = original_init
            init_mod._tracer_provider = original_provider
