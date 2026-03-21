"""Tests for auto-instrumentation loading."""

from __future__ import annotations

from celerity.telemetry.instrumentations import load_instrumentations


class TestLoadInstrumentations:
    def test_returns_list(self) -> None:
        result = load_instrumentations()
        assert isinstance(result, list)

    def test_skips_missing_instrumentations(self) -> None:
        # Should not raise even if instrumentation packages are not installed
        result = load_instrumentations()
        # All items in result should be strings
        assert all(isinstance(name, str) for name in result)
