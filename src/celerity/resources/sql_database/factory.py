"""SQL database instance factory."""

from __future__ import annotations

import ssl as ssl_module
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from celerity.resources.sql_database.config import PoolConfig  # noqa: TC001
from celerity.resources.sql_database.types import (
    SqlConnectionInfo,
    SqlDatabaseInstance,
    SqlIamAuth,
    SqlPasswordAuth,
)


class _SqlDatabaseInstanceImpl(SqlDatabaseInstance):
    """Concrete implementation of SqlDatabaseInstance."""

    def __init__(
        self,
        writer_engine: AsyncEngine,
        reader_engine: AsyncEngine | None = None,
        auth: SqlPasswordAuth | SqlIamAuth | None = None,
    ) -> None:
        self._writer = writer_engine
        self._reader = reader_engine
        self._auth = auth

    def writer(self) -> AsyncEngine:
        return self._writer

    def reader(self) -> AsyncEngine:
        return self._reader if self._reader is not None else self._writer

    async def close(self) -> None:
        await self._writer.dispose()
        if self._reader is not None:
            await self._reader.dispose()
        if isinstance(self._auth, SqlIamAuth):
            close = getattr(self._auth.token_provider, "close", None)
            if close is not None:
                await close()


def _build_engine_kwargs(
    pool_config: PoolConfig,
    connection_info: SqlConnectionInfo,
) -> dict[str, Any]:
    """Build common engine kwargs for pool and SSL configuration."""
    kwargs: dict[str, Any] = {
        "pool_size": pool_config.pool_max_size,
        "pool_pre_ping": True,
        "pool_timeout": pool_config.acquire_timeout_seconds,
        "pool_recycle": int(pool_config.idle_timeout_seconds),
    }

    if connection_info.ssl:
        if connection_info.engine == "postgres":
            ssl_ctx = ssl_module.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl_module.CERT_NONE
            kwargs["connect_args"] = {"ssl": ssl_ctx}
        elif connection_info.engine == "mysql":
            kwargs["connect_args"] = {"ssl": {"ssl": True}}

    return kwargs


def create_sql_database(
    connection_info: SqlConnectionInfo,
    auth: SqlPasswordAuth | SqlIamAuth,
    pool_config: PoolConfig,
) -> SqlDatabaseInstance:
    """Create a SqlDatabaseInstance with configured engines.

    Password mode creates engines with static connection URLs.
    IAM mode creates engines that will refresh tokens via the
    token provider's ``get_token()`` method at connect time.
    """
    engine_kwargs = _build_engine_kwargs(pool_config, connection_info)

    writer_engine = create_async_engine(auth.url, **engine_kwargs)

    reader_engine: AsyncEngine | None = None
    if auth.read_url:
        reader_engine = create_async_engine(auth.read_url, **engine_kwargs)

    return _SqlDatabaseInstanceImpl(writer_engine, reader_engine, auth)


class _CredentialsImpl:
    """Concrete credentials accessor returned by the layer."""

    def __init__(
        self,
        connection_info: SqlConnectionInfo,
        auth: SqlPasswordAuth | SqlIamAuth,
    ) -> None:
        self._connection_info = connection_info
        self._auth = auth

    def get_connection_info(self) -> SqlConnectionInfo:
        return self._connection_info

    def get_password_auth(self) -> SqlPasswordAuth:
        from celerity.resources.sql_database.errors import SqlDatabaseError

        if not isinstance(self._auth, SqlPasswordAuth):
            raise SqlDatabaseError("Auth mode is not 'password'")
        return self._auth

    def get_iam_auth(self) -> SqlIamAuth:
        from celerity.resources.sql_database.errors import SqlDatabaseError

        if not isinstance(self._auth, SqlIamAuth):
            raise SqlDatabaseError("Auth mode is not 'iam'")
        return self._auth


def create_credentials(
    connection_info: SqlConnectionInfo,
    auth: SqlPasswordAuth | SqlIamAuth,
) -> _CredentialsImpl:
    """Create a SqlDatabaseCredentials implementation."""
    return _CredentialsImpl(connection_info, auth)
