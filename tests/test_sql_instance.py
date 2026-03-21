"""Tests for SqlDatabaseInstance."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from celerity.resources.sql_database.factory import _SqlDatabaseInstanceImpl


class TestSqlDatabaseInstance:
    def test_writer_returns_writer_engine(self) -> None:
        writer = MagicMock()
        instance = _SqlDatabaseInstanceImpl(writer)
        assert instance.writer() is writer

    def test_reader_returns_reader_engine_when_configured(self) -> None:
        writer = MagicMock()
        reader = MagicMock()
        instance = _SqlDatabaseInstanceImpl(writer, reader)
        assert instance.reader() is reader

    def test_reader_falls_back_to_writer(self) -> None:
        writer = MagicMock()
        instance = _SqlDatabaseInstanceImpl(writer)
        assert instance.reader() is writer

    @pytest.mark.asyncio
    async def test_close_disposes_writer(self) -> None:
        writer = AsyncMock()
        instance = _SqlDatabaseInstanceImpl(writer)
        await instance.close()
        writer.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_disposes_both_engines(self) -> None:
        writer = AsyncMock()
        reader = AsyncMock()
        instance = _SqlDatabaseInstanceImpl(writer, reader)
        await instance.close()
        writer.dispose.assert_awaited_once()
        reader.dispose.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_closes_iam_token_provider(self) -> None:
        from celerity.resources.sql_database.types import SqlIamAuth

        writer = AsyncMock()
        mock_provider = AsyncMock()
        auth = SqlIamAuth(token_provider=mock_provider, url="u")
        instance = _SqlDatabaseInstanceImpl(writer, auth=auth)
        await instance.close()
        writer.dispose.assert_awaited_once()
        mock_provider.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_skips_provider_without_close(self) -> None:
        from celerity.resources.sql_database.types import SqlIamAuth

        writer = AsyncMock()
        mock_provider = MagicMock(spec=["get_token"])  # no close method
        auth = SqlIamAuth(token_provider=mock_provider, url="u")
        instance = _SqlDatabaseInstanceImpl(writer, auth=auth)
        await instance.close()  # should not raise
        writer.dispose.assert_awaited_once()
