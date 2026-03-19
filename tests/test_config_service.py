"""Tests for ConfigService and ConfigNamespace."""

from __future__ import annotations

import pytest

from celerity.config.service import ConfigService, ConfigServiceImpl


class TestConfigServiceImpl:
    async def test_get_returns_value(self) -> None:
        ns = ConfigServiceImpl({"host": "localhost", "port": "6379"})
        assert await ns.get("host") == "localhost"

    async def test_get_returns_none_for_missing(self) -> None:
        ns = ConfigServiceImpl({"host": "localhost"})
        assert await ns.get("missing") is None

    async def test_get_or_throw_returns_value(self) -> None:
        ns = ConfigServiceImpl({"host": "localhost"})
        assert await ns.get_or_throw("host") == "localhost"

    async def test_get_or_throw_raises_for_missing(self) -> None:
        ns = ConfigServiceImpl({})
        with pytest.raises(KeyError, match="Config key not found: missing"):
            await ns.get_or_throw("missing")

    async def test_get_all(self) -> None:
        data = {"a": "1", "b": "2"}
        ns = ConfigServiceImpl(data)
        result = await ns.get_all()
        assert result == data
        assert result is not data  # returns a copy

    async def test_set_data_replaces(self) -> None:
        ns = ConfigServiceImpl({"old": "val"})
        ns.set_data({"new": "val"})
        assert await ns.get("old") is None
        assert await ns.get("new") == "val"


class TestConfigService:
    def test_register_and_resolve_namespace(self) -> None:
        svc = ConfigService()
        ns = ConfigServiceImpl({"key": "val"})
        svc.register_namespace("resources", ns)
        assert svc.namespace("resources") is ns

    def test_namespace_raises_for_unknown(self) -> None:
        svc = ConfigService()
        with pytest.raises(KeyError, match="Config namespace not found"):
            svc.namespace("unknown")

    def test_default_namespace_with_single(self) -> None:
        svc = ConfigService()
        ns = ConfigServiceImpl({})
        svc.register_namespace("resources", ns)
        assert svc.default_namespace() is ns

    def test_default_namespace_none_with_multiple(self) -> None:
        svc = ConfigService()
        svc.register_namespace("resources", ConfigServiceImpl({}))
        svc.register_namespace("secrets", ConfigServiceImpl({}))
        assert svc.default_namespace() is None

    def test_namespace_names(self) -> None:
        svc = ConfigService()
        svc.register_namespace("a", ConfigServiceImpl({}))
        svc.register_namespace("b", ConfigServiceImpl({}))
        assert sorted(svc.namespace_names) == ["a", "b"]
