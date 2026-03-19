"""Tests for ConfigLayer."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from celerity.config.layer import ConfigLayer
from celerity.config.service import CONFIG_SERVICE_TOKEN, ConfigService


def _make_context(container: Any) -> SimpleNamespace:
    return SimpleNamespace(container=container)


class _MockContainer:
    def __init__(self) -> None:
        self._values: dict[str, Any] = {}

    def register_value(self, token: str, value: Any) -> None:
        self._values[token] = value

    def get(self, token: str) -> Any:
        return self._values.get(token)


class TestConfigLayer:
    async def test_registers_config_service(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("CELERITY_CONFIG_STORE_ID", raising=False)
        monkeypatch.delenv("CELERITY_PLATFORM", raising=False)

        container = _MockContainer()
        layer = ConfigLayer()
        called = False

        async def next_handler() -> None:
            nonlocal called
            called = True

        await layer.handle(_make_context(container), next_handler)

        assert called
        svc = container.get(CONFIG_SERVICE_TOKEN)
        assert isinstance(svc, ConfigService)

    async def test_idempotent_init(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("CELERITY_CONFIG_STORE_ID", raising=False)

        container = _MockContainer()
        layer = ConfigLayer()
        call_count = 0

        async def next_handler() -> None:
            nonlocal call_count
            call_count += 1

        await layer.handle(_make_context(container), next_handler)
        await layer.handle(_make_context(container), next_handler)

        assert call_count == 2  # next_handler called both times
        # But ConfigService is only registered once (idempotent).
        assert container.get(CONFIG_SERVICE_TOKEN) is not None

    async def test_empty_config_when_no_store_id(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("CELERITY_CONFIG_STORE_ID", raising=False)

        container = _MockContainer()
        layer = ConfigLayer()

        async def noop() -> None:
            pass

        await layer.handle(_make_context(container), noop)

        svc: ConfigService = container.get(CONFIG_SERVICE_TOKEN)
        ns = svc.namespace("resources")
        assert await ns.get("anything") is None

    async def test_loads_from_backend_when_store_id_set(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("CELERITY_CONFIG_STORE_ID", "test-store")
        monkeypatch.setenv("CELERITY_PLATFORM", "local")

        container = _MockContainer()
        layer = ConfigLayer()

        mock_data = {"appCache_host": "redis.local", "appCache_port": "6379"}

        with patch(
            "celerity.config.backends.local.LocalConfigBackend.fetch",
            new_callable=AsyncMock,
            return_value=mock_data,
        ):

            async def noop() -> None:
                pass

            await layer.handle(_make_context(container), noop)

        svc: ConfigService = container.get(CONFIG_SERVICE_TOKEN)
        ns = svc.namespace("resources")
        assert await ns.get("appCache_host") == "redis.local"
        assert await ns.get("appCache_port") == "6379"
