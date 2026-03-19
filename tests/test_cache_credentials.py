"""Tests for cache credential resolution."""

from __future__ import annotations

import pytest

from celerity.resources.cache.credentials import (
    CacheConnectionInfo,
    CacheIamAuth,
    CachePasswordAuth,
    resolve_cache_credentials,
)
from celerity.resources.cache.errors import CacheError


class FakeConfigNamespace:
    """In-memory config namespace for testing."""

    def __init__(self, data: dict[str, str]) -> None:
        self._data = data

    async def get(self, key: str) -> str | None:
        return self._data.get(key)

    async def get_or_throw(self, key: str) -> str:
        val = self._data.get(key)
        if val is None:
            raise KeyError(key)
        return val

    async def get_all(self) -> dict[str, str]:
        return dict(self._data)


class TestResolvePasswordAuth:
    @pytest.mark.asyncio
    async def test_full_password_config(self) -> None:
        ns = FakeConfigNamespace(
            {
                "myCache_host": "redis.example.com",
                "myCache_port": "6380",
                "myCache_tls": "true",
                "myCache_clusterMode": "true",
                "myCache_authToken": "secret123",
                "myCache_keyPrefix": "app:",
            }
        )

        info, auth = await resolve_cache_credentials(ns, "myCache")  # type: ignore[arg-type]

        assert isinstance(info, CacheConnectionInfo)
        assert info.host == "redis.example.com"
        assert info.port == 6380
        assert info.tls is True
        assert info.cluster_mode is True
        assert info.auth_mode == "password"
        assert info.key_prefix == "app:"

        assert isinstance(auth, CachePasswordAuth)
        assert auth.password == "secret123"

    @pytest.mark.asyncio
    async def test_defaults(self) -> None:
        ns = FakeConfigNamespace({"c_host": "localhost"})

        info, auth = await resolve_cache_credentials(ns, "c")  # type: ignore[arg-type]

        assert info.port == 6379
        assert info.tls is True  # default
        assert info.cluster_mode is False
        assert info.auth_mode == "password"
        assert info.key_prefix == ""

        assert isinstance(auth, CachePasswordAuth)
        assert auth.password is None

    @pytest.mark.asyncio
    async def test_tls_false(self) -> None:
        ns = FakeConfigNamespace(
            {
                "c_host": "localhost",
                "c_tls": "false",
            }
        )

        info, _ = await resolve_cache_credentials(ns, "c")  # type: ignore[arg-type]
        assert info.tls is False


class TestResolveIamAuth:
    @pytest.mark.asyncio
    async def test_iam_forces_tls(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_PLATFORM", "aws")
        ns = FakeConfigNamespace(
            {
                "c_host": "my-cluster.abc123.cache.amazonaws.com",
                "c_authMode": "iam",
                "c_user": "cache-user",
                "c_region": "us-east-1",
                "c_tls": "false",  # should be overridden
            }
        )

        info, auth = await resolve_cache_credentials(ns, "c")  # type: ignore[arg-type]

        assert info.tls is True
        assert info.auth_mode == "iam"
        assert isinstance(auth, CacheIamAuth)
        assert auth.token_provider is not None

    @pytest.mark.asyncio
    async def test_iam_requires_supported_platform(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_PLATFORM", "local")
        ns = FakeConfigNamespace(
            {
                "c_host": "host",
                "c_authMode": "iam",
                "c_user": "user",
                "c_region": "us-east-1",
            }
        )

        with pytest.raises(CacheError, match="not supported on platform"):
            await resolve_cache_credentials(ns, "c")  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_iam_requires_user(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_PLATFORM", "aws")
        ns = FakeConfigNamespace(
            {
                "c_host": "host",
                "c_authMode": "iam",
                "c_region": "us-east-1",
            }
        )

        with pytest.raises(CacheError, match="c_user"):
            await resolve_cache_credentials(ns, "c")  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_iam_requires_region(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_PLATFORM", "aws")
        ns = FakeConfigNamespace(
            {
                "c_host": "host",
                "c_authMode": "iam",
                "c_user": "user",
            }
        )

        with pytest.raises(CacheError, match="c_region"):
            await resolve_cache_credentials(ns, "c")  # type: ignore[arg-type]


class TestMissingHost:
    @pytest.mark.asyncio
    async def test_raises_on_missing_host(self) -> None:
        ns = FakeConfigNamespace({})

        with pytest.raises(CacheError, match="host"):
            await resolve_cache_credentials(ns, "c")  # type: ignore[arg-type]
