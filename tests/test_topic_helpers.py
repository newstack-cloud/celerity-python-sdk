"""Tests for topic get_topic() helper."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from celerity.resources.topic.params import get_topic


def _make_container(registered: dict[str, object] | None = None) -> AsyncMock:
    container = AsyncMock()
    store = registered or {}

    async def resolve(token: str) -> object:
        if token in store:
            return store[token]
        raise KeyError(token)

    container.resolve = AsyncMock(side_effect=resolve)
    return container


class TestGetTopic:
    @pytest.mark.asyncio
    async def test_resolves_default(self) -> None:
        mock_topic = MagicMock()
        container = _make_container({"celerity:topic:default": mock_topic})
        result = await get_topic(container)
        assert result is mock_topic

    @pytest.mark.asyncio
    async def test_resolves_named(self) -> None:
        mock_topic = MagicMock()
        container = _make_container({"celerity:topic:orders": mock_topic})
        result = await get_topic(container, "orders")
        assert result is mock_topic

    @pytest.mark.asyncio
    async def test_raises_when_not_registered(self) -> None:
        container = _make_container()
        with pytest.raises(KeyError):
            await get_topic(container)
