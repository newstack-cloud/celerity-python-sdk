"""Tests for QueueLayer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

import pytest

from celerity.resources.queue.layer import QueueLayer


def _make_context(container: AsyncMock) -> MagicMock:
    ctx = MagicMock()
    ctx.container = container
    return ctx


def _make_container() -> AsyncMock:
    container = AsyncMock()
    registered: dict[str, object] = {}

    async def resolve(token: str) -> object:
        if token in registered:
            return registered[token]
        raise KeyError(token)

    def register_value(token: str, value: object) -> None:
        registered[token] = value

    container.resolve = AsyncMock(side_effect=resolve)
    container.register_value = MagicMock(side_effect=register_value)
    container._registered = registered
    return container


def _make_config_service(url_map: dict[str, str]) -> MagicMock:
    config_service = MagicMock()
    namespace = MagicMock()

    async def get(key: str) -> str | None:
        return url_map.get(key)

    namespace.get = AsyncMock(side_effect=get)
    config_service.namespace = MagicMock(return_value=namespace)
    return config_service


class TestQueueLayer:
    @pytest.mark.asyncio
    async def test_no_op_without_links(
        self, resource_links_file: Callable[[dict[str, Any]], Path]
    ) -> None:
        resource_links_file({})
        layer = QueueLayer()
        container = _make_container()
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        result = await layer.handle(ctx, next_handler)
        assert result == "ok"
        next_handler.assert_awaited_once()
        container.register_value.assert_not_called()

    @pytest.mark.asyncio
    @patch("celerity.resources.queue.layer.create_queue_client")
    async def test_registers_single_resource_with_default(
        self,
        mock_factory: MagicMock,
        resource_links_file: Callable[[dict[str, Any]], Path],
    ) -> None:
        mock_client = MagicMock()
        mock_queue_handle = MagicMock()
        mock_client.queue.return_value = mock_queue_handle
        mock_client.close = AsyncMock()
        mock_factory.return_value = mock_client

        config_service = _make_config_service({"myQueue": "https://sqs/my-queue"})

        resource_links_file({"my-queue": {"type": "queue", "configKey": "myQueue"}})

        layer = QueueLayer()
        container = _make_container()
        container._registered["ConfigService"] = config_service
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)

        tokens = [call.args[0] for call in container.register_value.call_args_list]
        assert "celerity:queue:my-queue" in tokens
        assert "celerity:queue:default" in tokens

    @pytest.mark.asyncio
    @patch("celerity.resources.queue.layer.create_queue_client")
    async def test_no_default_for_multiple_resources(
        self,
        mock_factory: MagicMock,
        resource_links_file: Callable[[dict[str, Any]], Path],
    ) -> None:
        mock_client = MagicMock()
        mock_client.queue.return_value = MagicMock()
        mock_client.close = AsyncMock()
        mock_factory.return_value = mock_client

        config_service = _make_config_service(
            {"ordersQ": "https://sqs/orders", "notifQ": "https://sqs/notif"}
        )

        resource_links_file(
            {
                "orders": {"type": "queue", "configKey": "ordersQ"},
                "notifications": {"type": "queue", "configKey": "notifQ"},
            }
        )

        layer = QueueLayer()
        container = _make_container()
        container._registered["ConfigService"] = config_service
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)

        tokens = [call.args[0] for call in container.register_value.call_args_list]
        assert "celerity:queue:orders" in tokens
        assert "celerity:queue:notifications" in tokens
        assert "celerity:queue:default" not in tokens

    @pytest.mark.asyncio
    @patch("celerity.resources.queue.layer.create_queue_client")
    async def test_idempotent(
        self,
        mock_factory: MagicMock,
        resource_links_file: Callable[[dict[str, Any]], Path],
    ) -> None:
        mock_client = MagicMock()
        mock_client.queue.return_value = MagicMock()
        mock_client.close = AsyncMock()
        mock_factory.return_value = mock_client

        config_service = _make_config_service({"q": "https://sqs/q"})

        resource_links_file({"queue": {"type": "queue", "configKey": "q"}})

        layer = QueueLayer()
        container = _make_container()
        container._registered["ConfigService"] = config_service
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)
        call_count = container.register_value.call_count

        await layer.handle(ctx, next_handler)
        assert container.register_value.call_count == call_count

    @pytest.mark.asyncio
    async def test_passes_through_to_next(
        self, resource_links_file: Callable[[dict[str, Any]], Path]
    ) -> None:
        resource_links_file({})
        layer = QueueLayer()
        container = _make_container()
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="response")

        result = await layer.handle(ctx, next_handler)
        assert result == "response"

    @pytest.mark.asyncio
    @patch("celerity.resources.queue.layer.create_queue_client")
    async def test_dispose_closes_client(
        self,
        mock_factory: MagicMock,
        resource_links_file: Callable[[dict[str, Any]], Path],
    ) -> None:
        mock_client = MagicMock()
        mock_client.queue.return_value = MagicMock()
        mock_client.close = AsyncMock()
        mock_factory.return_value = mock_client

        config_service = _make_config_service({"q": "https://sqs/q"})

        resource_links_file({"queue": {"type": "queue", "configKey": "q"}})

        layer = QueueLayer()
        container = _make_container()
        container._registered["ConfigService"] = config_service
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)
        await layer.dispose()
        mock_client.close.assert_awaited_once()
