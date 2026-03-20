"""Tests for queue get_queue() helper."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from celerity.resources.queue.params import get_queue


def _make_container(registered: dict[str, object] | None = None) -> AsyncMock:
    container = AsyncMock()
    store = registered or {}

    async def resolve(token: str) -> object:
        if token in store:
            return store[token]
        raise KeyError(token)

    container.resolve = AsyncMock(side_effect=resolve)
    return container


class TestGetQueue:
    @pytest.mark.asyncio
    async def test_resolves_default(self) -> None:
        mock_queue = MagicMock()
        container = _make_container({"celerity:queue:default": mock_queue})
        result = await get_queue(container)
        assert result is mock_queue

    @pytest.mark.asyncio
    async def test_resolves_named(self) -> None:
        mock_queue = MagicMock()
        container = _make_container({"celerity:queue:orders": mock_queue})
        result = await get_queue(container, "orders")
        assert result is mock_queue

    @pytest.mark.asyncio
    async def test_raises_when_not_registered(self) -> None:
        container = _make_container()
        with pytest.raises(KeyError):
            await get_queue(container)
