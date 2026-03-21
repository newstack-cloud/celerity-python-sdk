"""Tests for SQL database factory."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from celerity.resources.sql_database.config import PoolConfig
from celerity.resources.sql_database.errors import SqlDatabaseError
from celerity.resources.sql_database.factory import (
    create_credentials,
    create_sql_database,
)
from celerity.resources.sql_database.types import (
    SqlConnectionInfo,
    SqlIamAuth,
    SqlPasswordAuth,
)


def _make_connection_info(
    engine: str = "postgres",
    ssl: bool = False,
    read_host: str | None = None,
) -> SqlConnectionInfo:
    return SqlConnectionInfo(
        host="localhost",
        port=5432,
        database="testdb",
        user="user",
        engine=engine,
        ssl=ssl,
        auth_mode="password",
        read_host=read_host,
    )


def _make_pool_config() -> PoolConfig:
    return PoolConfig(
        pool_min_size=0,
        pool_max_size=5,
        idle_timeout_seconds=10,
        acquire_timeout_seconds=5,
    )


class TestCreateSqlDatabase:
    @patch("celerity.resources.sql_database.factory.create_async_engine")
    def test_postgres_engine(self, mock_create: MagicMock) -> None:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine

        info = _make_connection_info(engine="postgres")
        auth = SqlPasswordAuth(
            password="pass",
            url="postgresql+asyncpg://user:pass@localhost:5432/testdb",
        )

        instance = create_sql_database(info, auth, _make_pool_config())

        assert instance.writer() is mock_engine
        call_args = mock_create.call_args
        assert "postgresql+asyncpg://" in call_args[0][0]

    @patch("celerity.resources.sql_database.factory.create_async_engine")
    def test_mysql_engine(self, mock_create: MagicMock) -> None:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine

        info = _make_connection_info(engine="mysql")
        auth = SqlPasswordAuth(
            password="pass",
            url="mysql+aiomysql://user:pass@localhost:5432/testdb",
        )

        instance = create_sql_database(info, auth, _make_pool_config())
        assert instance.writer() is mock_engine

    @patch("celerity.resources.sql_database.factory.create_async_engine")
    def test_pool_config_applied(self, mock_create: MagicMock) -> None:
        mock_create.return_value = MagicMock()
        pool = _make_pool_config()
        info = _make_connection_info()
        auth = SqlPasswordAuth(password="p", url="postgresql+asyncpg://u:p@h:5432/d")

        create_sql_database(info, auth, pool)

        kwargs = mock_create.call_args[1]
        assert kwargs["pool_size"] == pool.pool_max_size
        assert kwargs["pool_pre_ping"] is True
        assert kwargs["pool_timeout"] == pool.acquire_timeout_seconds
        assert kwargs["pool_recycle"] == int(pool.idle_timeout_seconds)

    @patch("celerity.resources.sql_database.factory.create_async_engine")
    def test_ssl_postgres(self, mock_create: MagicMock) -> None:
        import ssl

        mock_create.return_value = MagicMock()
        info = _make_connection_info(engine="postgres", ssl=True)
        auth = SqlPasswordAuth(
            password="p",
            url="postgresql+asyncpg://u:p@h:5432/d?ssl=require",
        )

        create_sql_database(info, auth, _make_pool_config())

        kwargs = mock_create.call_args[1]
        assert "connect_args" in kwargs
        ssl_ctx = kwargs["connect_args"]["ssl"]
        assert isinstance(ssl_ctx, ssl.SSLContext)
        assert ssl_ctx.check_hostname is True
        assert ssl_ctx.verify_mode == ssl.CERT_REQUIRED

    @patch("celerity.resources.sql_database.factory.create_async_engine")
    def test_ssl_mysql(self, mock_create: MagicMock) -> None:
        import ssl

        mock_create.return_value = MagicMock()
        info = _make_connection_info(engine="mysql", ssl=True)
        auth = SqlPasswordAuth(
            password="p",
            url="mysql+aiomysql://u:p@h:5432/d?ssl=true",
        )

        create_sql_database(info, auth, _make_pool_config())

        kwargs = mock_create.call_args[1]
        assert "connect_args" in kwargs
        ssl_ctx = kwargs["connect_args"]["ssl"]
        assert isinstance(ssl_ctx, ssl.SSLContext)
        assert ssl_ctx.check_hostname is True

    @patch("celerity.resources.sql_database.factory.create_async_engine")
    def test_no_ssl(self, mock_create: MagicMock) -> None:
        mock_create.return_value = MagicMock()
        info = _make_connection_info(ssl=False)
        auth = SqlPasswordAuth(password="p", url="postgresql+asyncpg://u:p@h:5432/d")

        create_sql_database(info, auth, _make_pool_config())

        kwargs = mock_create.call_args[1]
        assert "connect_args" not in kwargs

    @patch("celerity.resources.sql_database.factory.create_async_engine")
    def test_single_engine_no_read_host(self, mock_create: MagicMock) -> None:
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine

        info = _make_connection_info()
        auth = SqlPasswordAuth(password="p", url="postgresql+asyncpg://u:p@h:5432/d")

        instance = create_sql_database(info, auth, _make_pool_config())

        assert instance.writer() is mock_engine
        assert instance.reader() is mock_engine
        mock_create.assert_called_once()

    @patch("celerity.resources.sql_database.factory.create_async_engine")
    def test_two_engines_with_read_host(self, mock_create: MagicMock) -> None:
        writer_engine = MagicMock()
        reader_engine = MagicMock()
        mock_create.side_effect = [writer_engine, reader_engine]

        info = _make_connection_info(read_host="replica")
        auth = SqlPasswordAuth(
            password="p",
            url="postgresql+asyncpg://u:p@h:5432/d",
            read_url="postgresql+asyncpg://u:p@replica:5432/d",
        )

        instance = create_sql_database(info, auth, _make_pool_config())

        assert instance.writer() is writer_engine
        assert instance.reader() is reader_engine
        assert mock_create.call_count == 2


class TestCredentialsImpl:
    def test_get_connection_info(self) -> None:
        info = _make_connection_info()
        auth = SqlPasswordAuth(password="p", url="u")
        creds = create_credentials(info, auth)
        assert creds.get_connection_info() is info

    def test_get_password_auth(self) -> None:
        info = _make_connection_info()
        auth = SqlPasswordAuth(password="p", url="u")
        creds = create_credentials(info, auth)
        assert creds.get_password_auth() is auth

    def test_get_password_auth_wrong_mode(self) -> None:
        info = _make_connection_info()
        provider = MagicMock()
        auth = SqlIamAuth(token_provider=provider, url="u")
        creds = create_credentials(info, auth)

        with pytest.raises(SqlDatabaseError, match="not 'password'"):
            creds.get_password_auth()

    def test_get_iam_auth(self) -> None:
        info = _make_connection_info()
        provider = MagicMock()
        auth = SqlIamAuth(token_provider=provider, url="u")
        creds = create_credentials(info, auth)
        assert creds.get_iam_auth() is auth

    def test_get_iam_auth_wrong_mode(self) -> None:
        info = _make_connection_info()
        auth = SqlPasswordAuth(password="p", url="u")
        creds = create_credentials(info, auth)

        with pytest.raises(SqlDatabaseError, match="not 'iam'"):
            creds.get_iam_auth()
