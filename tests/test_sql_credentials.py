"""Tests for SQL database credential resolution."""

from __future__ import annotations

import pytest

from celerity.resources.sql_database.credentials import (
    resolve_database_credentials,
)
from celerity.resources.sql_database.errors import SqlDatabaseError
from celerity.resources.sql_database.types import (
    SqlConnectionInfo,
    SqlIamAuth,
    SqlPasswordAuth,
)


class FakeConfigNamespace:
    """In-memory config namespace for testing."""

    def __init__(self, data: dict[str, str]) -> None:
        self._data = data

    async def get(self, key: str) -> str | None:
        return self._data.get(key)

    async def get_or_throw(self, key: str) -> str:
        val = self._data.get(key)
        if val is None:
            raise KeyError(key)
        return val

    async def get_all(self) -> dict[str, str]:
        return dict(self._data)


class TestResolvePasswordAuth:
    @pytest.mark.asyncio
    async def test_full_password_config(self) -> None:
        ns = FakeConfigNamespace(
            {
                "db_host": "pg.example.com",
                "db_port": "5433",
                "db_database": "mydb",
                "db_user": "admin",
                "db_engine": "postgres",
                "db_ssl": "true",
                "db_authMode": "password",
                "db_readHost": "replica.example.com",
                "db_password": "secret123",
            }
        )

        info, auth = await resolve_database_credentials(ns, "db")  # type: ignore[arg-type]

        assert isinstance(info, SqlConnectionInfo)
        assert info.host == "pg.example.com"
        assert info.port == 5433
        assert info.database == "mydb"
        assert info.user == "admin"
        assert info.engine == "postgres"
        assert info.ssl is True
        assert info.auth_mode == "password"
        assert info.read_host == "replica.example.com"

        assert isinstance(auth, SqlPasswordAuth)
        assert auth.password == "secret123"
        assert "pg.example.com" in auth.url
        assert auth.read_url is not None
        assert "replica.example.com" in auth.read_url

    @pytest.mark.asyncio
    async def test_defaults(self) -> None:
        ns = FakeConfigNamespace(
            {
                "db_host": "localhost",
                "db_database": "testdb",
                "db_user": "user",
                "db_password": "pass",
            }
        )

        info, auth = await resolve_database_credentials(ns, "db")  # type: ignore[arg-type]

        assert info.port == 5432
        assert info.engine == "postgres"
        assert info.ssl is True
        assert info.auth_mode == "password"
        assert info.read_host is None

        assert isinstance(auth, SqlPasswordAuth)
        assert auth.read_url is None

    @pytest.mark.asyncio
    async def test_ssl_false(self) -> None:
        ns = FakeConfigNamespace(
            {
                "db_host": "localhost",
                "db_database": "testdb",
                "db_user": "user",
                "db_password": "pass",
                "db_ssl": "false",
            }
        )

        info, _ = await resolve_database_credentials(ns, "db")  # type: ignore[arg-type]
        assert info.ssl is False

    @pytest.mark.asyncio
    async def test_mysql_engine(self) -> None:
        ns = FakeConfigNamespace(
            {
                "db_host": "mysql.example.com",
                "db_database": "testdb",
                "db_user": "user",
                "db_engine": "mysql",
                "db_password": "pass",
            }
        )

        info, auth = await resolve_database_credentials(ns, "db")  # type: ignore[arg-type]

        assert info.engine == "mysql"
        assert isinstance(auth, SqlPasswordAuth)
        assert "mysql+aiomysql://" in auth.url


class TestResolveIamAuth:
    @pytest.mark.asyncio
    async def test_iam_forces_ssl(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_PLATFORM", "aws")
        ns = FakeConfigNamespace(
            {
                "db_host": "rds.amazonaws.com",
                "db_database": "mydb",
                "db_user": "iam_user",
                "db_authMode": "iam",
                "db_region": "us-east-1",
                "db_ssl": "false",  # should be overridden
            }
        )

        # Mock the RdsIamTokenProvider to avoid boto3 dependency
        import unittest.mock

        with unittest.mock.patch(
            "celerity.resources.sql_database.credentials._resolve_iam_token_provider"
        ) as mock_resolve:
            mock_provider = unittest.mock.AsyncMock()
            mock_provider.get_token.return_value = "iam-token-123"
            mock_resolve.return_value = mock_provider

            info, auth = await resolve_database_credentials(ns, "db")  # type: ignore[arg-type]

        assert info.ssl is True
        assert info.auth_mode == "iam"
        assert isinstance(auth, SqlIamAuth)
        assert auth.token_provider is mock_provider

    @pytest.mark.asyncio
    async def test_iam_unsupported_platform(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_PLATFORM", "local")
        ns = FakeConfigNamespace(
            {
                "db_host": "host",
                "db_database": "mydb",
                "db_user": "user",
                "db_authMode": "iam",
                "db_region": "us-east-1",
            }
        )

        with pytest.raises(SqlDatabaseError, match="not supported on platform"):
            await resolve_database_credentials(ns, "db")  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_iam_aws_creates_rds_provider(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_PLATFORM", "aws")

        import unittest.mock

        from celerity.resources.sql_database.credentials import _resolve_iam_token_provider

        with unittest.mock.patch(
            "celerity.resources.sql_database.providers.rds.iam.RdsIamTokenProvider"
        ) as mock_cls:
            mock_instance = unittest.mock.AsyncMock()
            mock_cls.return_value = mock_instance

            provider = _resolve_iam_token_provider(
                platform="aws",
                host="h",
                port=5432,
                user="u",
                region="us-east-1",
            )

            assert provider is mock_instance
            mock_cls.assert_called_once_with(
                host="h",
                port=5432,
                user="u",
                region="us-east-1",
            )


class TestValidation:
    @pytest.mark.asyncio
    async def test_missing_host(self) -> None:
        ns = FakeConfigNamespace({"db_database": "d", "db_user": "u", "db_password": "p"})
        with pytest.raises(SqlDatabaseError, match="host"):
            await resolve_database_credentials(ns, "db")  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_missing_database(self) -> None:
        ns = FakeConfigNamespace({"db_host": "h", "db_user": "u", "db_password": "p"})
        with pytest.raises(SqlDatabaseError, match="database"):
            await resolve_database_credentials(ns, "db")  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_missing_user(self) -> None:
        ns = FakeConfigNamespace({"db_host": "h", "db_database": "d", "db_password": "p"})
        with pytest.raises(SqlDatabaseError, match="user"):
            await resolve_database_credentials(ns, "db")  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_missing_password(self) -> None:
        ns = FakeConfigNamespace({"db_host": "h", "db_database": "d", "db_user": "u"})
        with pytest.raises(SqlDatabaseError, match="password"):
            await resolve_database_credentials(ns, "db")  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_invalid_engine(self) -> None:
        ns = FakeConfigNamespace(
            {"db_host": "h", "db_database": "d", "db_user": "u", "db_engine": "oracle"}
        )
        with pytest.raises(SqlDatabaseError, match="Invalid engine"):
            await resolve_database_credentials(ns, "db")  # type: ignore[arg-type]


class TestRdsIamTokenProvider:
    @pytest.mark.asyncio
    async def test_generates_token(self) -> None:
        from unittest.mock import AsyncMock, patch

        from celerity.resources.sql_database.providers.rds.iam import (
            RdsIamTokenProvider,
        )

        provider = RdsIamTokenProvider(
            host="rds.amazonaws.com",
            port=5432,
            user="iam_user",
            region="us-east-1",
        )

        with patch.object(
            provider, "_generate_token", new_callable=AsyncMock, return_value="rds-token-abc"
        ):
            token = await provider.get_token()

        assert token == "rds-token-abc"

    @pytest.mark.asyncio
    async def test_caches_token(self) -> None:
        from unittest.mock import AsyncMock, patch

        from celerity.resources.sql_database.providers.rds.iam import (
            RdsIamTokenProvider,
        )

        provider = RdsIamTokenProvider(
            host="h",
            port=5432,
            user="u",
            region="us-east-1",
        )

        with patch.object(
            provider, "_generate_token", new_callable=AsyncMock, return_value="cached-token"
        ) as mock_gen:
            token1 = await provider.get_token()
            token2 = await provider.get_token()

        assert token1 == token2 == "cached-token"
        mock_gen.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_refreshes_after_expiry(self) -> None:
        import time
        from unittest.mock import AsyncMock, patch

        from celerity.resources.sql_database.providers.rds.iam import (
            RdsIamTokenProvider,
        )

        provider = RdsIamTokenProvider(
            host="h",
            port=5432,
            user="u",
            region="us-east-1",
        )

        with patch.object(
            provider,
            "_generate_token",
            new_callable=AsyncMock,
            side_effect=["token-1", "token-2"],
        ) as mock_gen:
            token1 = await provider.get_token()
            assert token1 == "token-1"

            # Simulate token expiry
            provider._token_expiry = time.monotonic() - 1

            token2 = await provider.get_token()
            assert token2 == "token-2"
            assert mock_gen.await_count == 2
