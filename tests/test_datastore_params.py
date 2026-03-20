"""Tests for datastore DI parameter types and helpers."""

from __future__ import annotations

from typing import Annotated, get_args
from unittest.mock import AsyncMock

import pytest

from celerity.resources.datastore.params import (
    DEFAULT_DATASTORE_TOKEN,
    DatastoreParam,
    DatastoreResource,
    datastore_token,
    get_datastore,
)
from celerity.resources.datastore.types import Datastore


class TestDatastoreToken:
    def test_default_token_value(self) -> None:
        assert DEFAULT_DATASTORE_TOKEN == "celerity:datastore:default"

    def test_named_token(self) -> None:
        assert datastore_token("orders") == "celerity:datastore:orders"


class TestDatastoreParam:
    def test_default_marker(self) -> None:
        param = DatastoreParam()
        assert param.resource_type == "datastore"
        assert param.resource_name is None

    def test_named_marker(self) -> None:
        param = DatastoreParam("audit-log")
        assert param.resource_name == "audit-log"


class TestDatastoreResource:
    def test_alias_type(self) -> None:
        args = get_args(DatastoreResource)
        assert args[0] is Datastore
        assert isinstance(args[1], DatastoreParam)
        assert args[1].resource_name is None

    def test_named_annotated(self) -> None:
        named_ds = Annotated[Datastore, DatastoreParam("orders")]
        args = get_args(named_ds)
        assert args[0] is Datastore
        assert isinstance(args[1], DatastoreParam)
        assert args[1].resource_name == "orders"


class TestGetDatastore:
    @pytest.mark.asyncio
    async def test_default(self) -> None:
        mock_ds = AsyncMock(spec=Datastore)
        container = AsyncMock()
        container.resolve.return_value = mock_ds

        result = await get_datastore(container)
        container.resolve.assert_awaited_once_with(DEFAULT_DATASTORE_TOKEN)
        assert result is mock_ds

    @pytest.mark.asyncio
    async def test_named(self) -> None:
        mock_ds = AsyncMock(spec=Datastore)
        container = AsyncMock()
        container.resolve.return_value = mock_ds

        result = await get_datastore(container, "orders")
        container.resolve.assert_awaited_once_with("celerity:datastore:orders")
        assert result is mock_ds
