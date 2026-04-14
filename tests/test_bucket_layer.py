"""Tests for ObjectStorageLayer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

import pytest

from celerity.resources.bucket.layer import ObjectStorageLayer


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


def _make_config_service(id_map: dict[str, str]) -> MagicMock:
    config_service = MagicMock()
    namespace = MagicMock()

    async def get(key: str) -> str | None:
        return id_map.get(key)

    namespace.get = AsyncMock(side_effect=get)
    config_service.namespace = MagicMock(return_value=namespace)
    return config_service


class TestObjectStorageLayer:
    @pytest.mark.asyncio
    async def test_no_op_without_links(
        self, resource_links_file: Callable[[dict[str, Any]], Path]
    ) -> None:
        resource_links_file({})
        layer = ObjectStorageLayer()
        container = _make_container()
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        result = await layer.handle(ctx, next_handler)
        assert result == "ok"
        next_handler.assert_awaited_once()
        container.register_value.assert_not_called()

    @pytest.mark.asyncio
    @patch("celerity.resources.bucket.layer.create_object_storage")
    async def test_registers_single_resource_with_default(
        self,
        mock_factory: MagicMock,
        resource_links_file: Callable[[dict[str, Any]], Path],
    ) -> None:
        mock_storage = MagicMock()
        mock_bucket_handle = MagicMock()
        mock_storage.bucket.return_value = mock_bucket_handle
        mock_storage.close = AsyncMock()
        mock_factory.return_value = mock_storage

        config_service = _make_config_service({"myBucket": "actual-bucket-name"})

        resource_links_file({"my-bucket": {"type": "bucket", "configKey": "myBucket"}})

        layer = ObjectStorageLayer()
        container = _make_container()
        container._registered["ConfigService"] = config_service
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)

        tokens = [call.args[0] for call in container.register_value.call_args_list]
        assert "celerity:bucket:my-bucket" in tokens
        assert "celerity:bucket:default" in tokens

    @pytest.mark.asyncio
    @patch("celerity.resources.bucket.layer.create_object_storage")
    async def test_no_default_for_multiple_resources(
        self,
        mock_factory: MagicMock,
        resource_links_file: Callable[[dict[str, Any]], Path],
    ) -> None:
        mock_storage = MagicMock()
        mock_storage.bucket.return_value = MagicMock()
        mock_storage.close = AsyncMock()
        mock_factory.return_value = mock_storage

        config_service = _make_config_service({"images": "images-bucket", "docs": "docs-bucket"})

        resource_links_file(
            {
                "images": {"type": "bucket", "configKey": "images"},
                "docs": {"type": "bucket", "configKey": "docs"},
            }
        )

        layer = ObjectStorageLayer()
        container = _make_container()
        container._registered["ConfigService"] = config_service
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)

        tokens = [call.args[0] for call in container.register_value.call_args_list]
        assert "celerity:bucket:images" in tokens
        assert "celerity:bucket:docs" in tokens
        assert "celerity:bucket:default" not in tokens

    @pytest.mark.asyncio
    @patch("celerity.resources.bucket.layer.create_object_storage")
    async def test_idempotent(
        self,
        mock_factory: MagicMock,
        resource_links_file: Callable[[dict[str, Any]], Path],
    ) -> None:
        mock_storage = MagicMock()
        mock_storage.bucket.return_value = MagicMock()
        mock_storage.close = AsyncMock()
        mock_factory.return_value = mock_storage

        config_service = _make_config_service({"b": "test-bucket"})

        resource_links_file({"bucket": {"type": "bucket", "configKey": "b"}})

        layer = ObjectStorageLayer()
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
        layer = ObjectStorageLayer()
        container = _make_container()
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="response")

        result = await layer.handle(ctx, next_handler)
        assert result == "response"

    @pytest.mark.asyncio
    @patch("celerity.resources.bucket.layer.create_object_storage")
    async def test_dispose_closes_storages(
        self,
        mock_factory: MagicMock,
        resource_links_file: Callable[[dict[str, Any]], Path],
    ) -> None:
        mock_storage = MagicMock()
        mock_storage.bucket.return_value = MagicMock()
        mock_storage.close = AsyncMock()
        mock_factory.return_value = mock_storage

        config_service = _make_config_service({"b": "test-bucket"})

        resource_links_file({"bucket": {"type": "bucket", "configKey": "b"}})

        layer = ObjectStorageLayer()
        container = _make_container()
        container._registered["ConfigService"] = config_service
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)
        assert len(layer._storages) == 1

        await layer.dispose()
        mock_storage.close.assert_awaited_once()
