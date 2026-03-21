"""Tests for SQL database programmatic helpers."""

from __future__ import annotations

import pytest

from celerity.resources.sql_database import (
    get_sql_credentials,
    get_sql_database,
    get_sql_reader,
    get_sql_writer,
)


class FakeContainer:
    async def resolve(self, token: str) -> object:
        return token  # Return the token itself for assertion


class TestGetSqlDatabase:
    @pytest.mark.asyncio
    async def test_resolves_default(self) -> None:
        sentinel = object()

        class Container:
            async def resolve(self, token: str) -> object:
                assert token == "celerity:sql:writer:default"
                return sentinel

        result = await get_sql_database(Container(), None)  # type: ignore[arg-type]
        assert result is sentinel

    @pytest.mark.asyncio
    async def test_resolves_named(self) -> None:
        sentinel = object()

        class Container:
            async def resolve(self, token: str) -> object:
                assert token == "celerity:sql:writer:mydb"
                return sentinel

        result = await get_sql_database(Container(), "mydb")  # type: ignore[arg-type]
        assert result is sentinel


class TestGetSqlWriter:
    @pytest.mark.asyncio
    async def test_resolves_default(self) -> None:
        sentinel = object()

        class Container:
            async def resolve(self, token: str) -> object:
                assert token == "celerity:sql:writer:default"
                return sentinel

        result = await get_sql_writer(Container(), None)  # type: ignore[arg-type]
        assert result is sentinel

    @pytest.mark.asyncio
    async def test_resolves_named(self) -> None:
        sentinel = object()

        class Container:
            async def resolve(self, token: str) -> object:
                assert token == "celerity:sql:writer:mydb"
                return sentinel

        result = await get_sql_writer(Container(), "mydb")  # type: ignore[arg-type]
        assert result is sentinel


class TestGetSqlReader:
    @pytest.mark.asyncio
    async def test_resolves_default(self) -> None:
        sentinel = object()

        class Container:
            async def resolve(self, token: str) -> object:
                assert token == "celerity:sql:reader:default"
                return sentinel

        result = await get_sql_reader(Container(), None)  # type: ignore[arg-type]
        assert result is sentinel

    @pytest.mark.asyncio
    async def test_resolves_named(self) -> None:
        sentinel = object()

        class Container:
            async def resolve(self, token: str) -> object:
                assert token == "celerity:sql:reader:mydb"
                return sentinel

        result = await get_sql_reader(Container(), "mydb")  # type: ignore[arg-type]
        assert result is sentinel


class TestGetSqlCredentials:
    @pytest.mark.asyncio
    async def test_resolves_default(self) -> None:
        sentinel = object()

        class Container:
            async def resolve(self, token: str) -> object:
                assert token == "celerity:sql:credentials:default"
                return sentinel

        result = await get_sql_credentials(Container(), None)  # type: ignore[arg-type]
        assert result is sentinel

    @pytest.mark.asyncio
    async def test_resolves_named(self) -> None:
        sentinel = object()

        class Container:
            async def resolve(self, token: str) -> object:
                assert token == "celerity:sql:credentials:mydb"
                return sentinel

        result = await get_sql_credentials(Container(), "mydb")  # type: ignore[arg-type]
        assert result is sentinel
