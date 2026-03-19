"""Tests for cache parameter types and helpers."""

from __future__ import annotations

from typing import Annotated, get_args

import pytest

from celerity.resources._tokens import resolve_marker_token
from celerity.resources.cache import (
    DEFAULT_CACHE_TOKEN,
    Cache,
    CacheParam,
    CacheResource,
    cache_token,
)


class TestCacheParam:
    def test_default_marker(self) -> None:
        param = CacheParam()
        assert param.resource_type == "cache"
        assert param.resource_name is None

    def test_named_marker(self) -> None:
        param = CacheParam("session")
        assert param.resource_type == "cache"
        assert param.resource_name == "session"

    def test_resolve_default_token(self) -> None:
        token = resolve_marker_token(CacheParam())
        assert token == "celerity:cache:default"

    def test_resolve_named_token(self) -> None:
        token = resolve_marker_token(CacheParam("session"))
        assert token == "celerity:cache:session"


class TestCacheTokens:
    def test_cache_token(self) -> None:
        assert cache_token("app-cache") == "celerity:cache:app-cache"

    def test_default_cache_token(self) -> None:
        assert DEFAULT_CACHE_TOKEN == "celerity:cache:default"


class TestCacheResource:
    def test_is_annotated_with_cache(self) -> None:
        args = get_args(CacheResource)
        assert args[0] is Cache
        assert isinstance(args[1], CacheParam)
        assert args[1].resource_name is None

    def test_named_alias(self) -> None:
        session_cache = Annotated[Cache, CacheParam("session")]
        args = get_args(session_cache)
        assert args[0] is Cache
        assert isinstance(args[1], CacheParam)
        assert args[1].resource_name == "session"


class TestGetCache:
    @pytest.mark.asyncio
    async def test_resolves_default(self) -> None:
        sentinel = object()

        class FakeContainer:
            async def resolve(self, token: str) -> object:
                assert token == "celerity:cache:default"
                return sentinel

        from celerity.resources.cache import get_cache

        result = await get_cache(FakeContainer(), None)  # type: ignore[arg-type]
        assert result is sentinel

    @pytest.mark.asyncio
    async def test_resolves_named(self) -> None:
        sentinel = object()

        class FakeContainer:
            async def resolve(self, token: str) -> object:
                assert token == "celerity:cache:my-cache"
                return sentinel

        from celerity.resources.cache import get_cache

        result = await get_cache(FakeContainer(), "my-cache")  # type: ignore[arg-type]
        assert result is sentinel
