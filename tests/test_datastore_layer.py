"""Tests for DatastoreLayer."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from celerity.resources.datastore.layer import DatastoreLayer


def _make_context(container: AsyncMock) -> MagicMock:
    ctx = MagicMock()
    ctx.container = container
    return ctx


def _make_container() -> AsyncMock:
    container = AsyncMock()
    registered: dict[str, object] = {}

    async def resolve(token: str) -> object:
        if token == "ConfigService":
            config_service = MagicMock()
            ns = AsyncMock()
            ns.get.return_value = "test-table"
            config_service.namespace.return_value = ns
            return config_service
        return registered.get(token)

    def register_value(token: str, value: object) -> None:
        registered[token] = value

    container.resolve = AsyncMock(side_effect=resolve)
    container.register_value = MagicMock(side_effect=register_value)
    container._registered = registered
    return container


def _mock_create_client() -> AsyncMock:
    """Create a mock for create_datastore_client that returns a mock DatastoreClient."""
    mock_client = AsyncMock()
    mock_ds = MagicMock()
    mock_client.datastore.return_value = mock_ds
    mock_client.close = AsyncMock()

    async def factory(*args, **kwargs):
        return mock_client

    return AsyncMock(side_effect=factory)


class TestDatastoreLayer:
    @pytest.mark.asyncio
    async def test_no_op_without_links(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_RESOURCE_LINKS", raising=False)
        layer = DatastoreLayer()
        container = _make_container()
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        result = await layer.handle(ctx, next_handler)
        assert result == "ok"
        next_handler.assert_awaited_once()
        container.register_value.assert_not_called()

    @pytest.mark.asyncio
    @patch("celerity.resources.datastore.layer.create_datastore_client")
    async def test_registers_single_resource_with_default(
        self,
        mock_factory: AsyncMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_client = AsyncMock()
        mock_client.datastore.return_value = MagicMock()
        mock_factory.return_value = mock_client

        monkeypatch.setenv(
            "CELERITY_RESOURCE_LINKS",
            json.dumps({"orders-db": {"type": "datastore", "configKey": "ordersDb"}}),
        )

        layer = DatastoreLayer()
        container = _make_container()
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)

        tokens = [call.args[0] for call in container.register_value.call_args_list]
        assert "celerity:datastore:orders-db" in tokens
        assert "celerity:datastore:default" in tokens

    @pytest.mark.asyncio
    @patch("celerity.resources.datastore.layer.create_datastore_client")
    async def test_no_default_for_multiple_resources(
        self,
        mock_factory: AsyncMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_client = AsyncMock()
        mock_client.datastore.return_value = MagicMock()
        mock_factory.return_value = mock_client

        monkeypatch.setenv(
            "CELERITY_RESOURCE_LINKS",
            json.dumps(
                {
                    "db-1": {"type": "datastore", "configKey": "db1"},
                    "db-2": {"type": "datastore", "configKey": "db2"},
                }
            ),
        )

        layer = DatastoreLayer()
        container = _make_container()
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)

        tokens = [call.args[0] for call in container.register_value.call_args_list]
        assert "celerity:datastore:db-1" in tokens
        assert "celerity:datastore:db-2" in tokens
        assert "celerity:datastore:default" not in tokens

    @pytest.mark.asyncio
    @patch("celerity.resources.datastore.layer.create_datastore_client")
    async def test_idempotent(
        self,
        mock_factory: AsyncMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_client = AsyncMock()
        mock_client.datastore.return_value = MagicMock()
        mock_factory.return_value = mock_client

        monkeypatch.setenv(
            "CELERITY_RESOURCE_LINKS",
            json.dumps({"db": {"type": "datastore", "configKey": "db"}}),
        )

        layer = DatastoreLayer()
        container = _make_container()
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)
        call_count = container.register_value.call_count

        await layer.handle(ctx, next_handler)
        assert container.register_value.call_count == call_count

    @pytest.mark.asyncio
    async def test_passes_through_to_next(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_RESOURCE_LINKS", raising=False)
        layer = DatastoreLayer()
        container = _make_container()
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="response")

        result = await layer.handle(ctx, next_handler)
        assert result == "response"

    @pytest.mark.asyncio
    @patch("celerity.resources.datastore.layer.create_datastore_client")
    async def test_dispose_closes_clients(
        self,
        mock_factory: AsyncMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_client = AsyncMock()
        mock_client.datastore.return_value = MagicMock()
        mock_client.close = AsyncMock()
        mock_factory.return_value = mock_client

        monkeypatch.setenv(
            "CELERITY_RESOURCE_LINKS",
            json.dumps({"db": {"type": "datastore", "configKey": "db"}}),
        )

        layer = DatastoreLayer()
        container = _make_container()
        ctx = _make_context(container)
        next_handler = AsyncMock(return_value="ok")

        await layer.handle(ctx, next_handler)
        assert layer._client is not None

        await layer.dispose()
        mock_client.close.assert_awaited_once()
