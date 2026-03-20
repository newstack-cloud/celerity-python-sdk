"""Tests for request-scoped logger via contextvars."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from celerity.telemetry.request_context import (
    ContextAwareLogger,
    clear_request_logger,
    get_request_logger,
    set_request_logger,
)
from celerity.types.telemetry import CelerityLogger


def _mock_logger(name: str = "mock") -> MagicMock:
    mock = MagicMock(spec=CelerityLogger)
    mock._name = name
    return mock


class TestRequestContext:
    def test_default_is_none(self) -> None:
        clear_request_logger()
        assert get_request_logger() is None

    def test_set_and_get(self) -> None:
        logger = _mock_logger()
        set_request_logger(logger)
        assert get_request_logger() is logger
        clear_request_logger()

    def test_clear(self) -> None:
        set_request_logger(_mock_logger())
        clear_request_logger()
        assert get_request_logger() is None

    @pytest.mark.asyncio
    async def test_concurrent_isolation(self) -> None:
        """Concurrent tasks get independent request loggers."""
        results: dict[str, CelerityLogger | None] = {}

        async def task(name: str) -> None:
            logger = _mock_logger(name)
            set_request_logger(logger)
            await asyncio.sleep(0.01)
            results[name] = get_request_logger()

        await asyncio.gather(task("a"), task("b"))
        assert results["a"] is not results["b"]
        assert results["a"]._name == "a"  # type: ignore[union-attr]
        assert results["b"]._name == "b"  # type: ignore[union-attr]
        clear_request_logger()


class TestContextAwareLogger:
    def test_delegates_to_request_logger(self) -> None:
        root = _mock_logger("root")
        request = _mock_logger("request")
        ctx_logger = ContextAwareLogger(root)

        set_request_logger(request)
        ctx_logger.info("test message", key="val")
        request.info.assert_called_once_with("test message", key="val")
        root.info.assert_not_called()
        clear_request_logger()

    def test_falls_back_to_root(self) -> None:
        root = _mock_logger("root")
        ctx_logger = ContextAwareLogger(root)

        clear_request_logger()
        ctx_logger.info("test message")
        root.info.assert_called_once_with("test message")

    def test_all_levels(self) -> None:
        root = _mock_logger("root")
        ctx_logger = ContextAwareLogger(root)
        clear_request_logger()

        ctx_logger.debug("d")
        ctx_logger.info("i")
        ctx_logger.warn("w")
        ctx_logger.error("e")

        root.debug.assert_called_once_with("d")
        root.info.assert_called_once_with("i")
        root.warn.assert_called_once_with("w")
        root.error.assert_called_once_with("e")

    def test_child_delegates(self) -> None:
        root = _mock_logger("root")
        request = _mock_logger("request")
        ctx_logger = ContextAwareLogger(root)

        set_request_logger(request)
        ctx_logger.child("orders", service="billing")
        request.child.assert_called_once_with("orders", service="billing")
        clear_request_logger()

    def test_with_context_delegates(self) -> None:
        root = _mock_logger("root")
        request = _mock_logger("request")
        ctx_logger = ContextAwareLogger(root)

        set_request_logger(request)
        ctx_logger.with_context(request_id="abc")
        request.with_context.assert_called_once_with(request_id="abc")
        clear_request_logger()
