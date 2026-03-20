"""Tests for telemetry DI tokens and helpers."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from celerity.telemetry.helpers import (
    LOGGER_TOKEN,
    TRACER_TOKEN,
    get_logger,
    get_tracer,
)


class TestTokenValues:
    def test_logger_token(self) -> None:
        assert LOGGER_TOKEN == "CelerityLogger"

    def test_tracer_token(self) -> None:
        assert TRACER_TOKEN == "CelerityTracer"


class TestCommonReExports:
    def test_tokens_in_common(self) -> None:
        from celerity.common import LOGGER_TOKEN as L
        from celerity.common import TRACER_TOKEN as T

        assert L == "CelerityLogger"
        assert T == "CelerityTracer"


class TestGetLogger:
    @pytest.mark.asyncio
    async def test_resolves_from_container(self) -> None:
        mock_logger = MagicMock()
        container = AsyncMock()
        container.resolve.return_value = mock_logger

        result = await get_logger(container)
        container.resolve.assert_awaited_once_with(LOGGER_TOKEN)
        assert result is mock_logger


class TestGetTracer:
    @pytest.mark.asyncio
    async def test_resolves_from_container(self) -> None:
        mock_tracer = MagicMock()
        container = AsyncMock()
        container.resolve.return_value = mock_tracer

        result = await get_tracer(container)
        container.resolve.assert_awaited_once_with(TRACER_TOKEN)
        assert result is mock_tracer
