"""SQL database credential resolution from config store."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from celerity.resources._common import Platform, detect_platform
from celerity.resources.sql_database.config import build_connection_url
from celerity.resources.sql_database.errors import SqlDatabaseError
from celerity.resources.sql_database.types import (
    SqlConnectionInfo,
    SqlIamAuth,
    SqlPasswordAuth,
)

if TYPE_CHECKING:
    from celerity.config.service import ConfigNamespace


@runtime_checkable
class SqlIamTokenProvider(Protocol):
    """Protocol for platform-specific IAM token providers.

    Implementations generate short-lived authentication tokens
    for managed database services (e.g. RDS IAM auth, Cloud SQL
    IAM auth, Azure AD auth).
    """

    async def get_token(self) -> str: ...


async def resolve_database_credentials(
    config_namespace: ConfigNamespace,
    config_key: str,
) -> tuple[SqlConnectionInfo, SqlPasswordAuth | SqlIamAuth]:
    """Resolve connection info and auth from config keys.

    Config keys read::

        {configKey}_host        (required)
        {configKey}_port        (default 5432)
        {configKey}_database    (required)
        {configKey}_user        (required)
        {configKey}_engine      ("postgres" | "mysql", default "postgres")
        {configKey}_ssl         (default "true", forced for IAM)
        {configKey}_authMode    ("password" | "iam", default "password")
        {configKey}_readHost    (optional, enables read replica)
        {configKey}_password    (required for password mode)
    """
    host = await config_namespace.get(f"{config_key}_host")
    if not host:
        raise SqlDatabaseError(f"Missing required config key: {config_key}_host")

    database = await config_namespace.get(f"{config_key}_database")
    if not database:
        raise SqlDatabaseError(f"Missing required config key: {config_key}_database")

    user = await config_namespace.get(f"{config_key}_user")
    if not user:
        raise SqlDatabaseError(f"Missing required config key: {config_key}_user")

    port_str = await config_namespace.get(f"{config_key}_port")
    port = int(port_str) if port_str else 5432

    db_engine = await config_namespace.get(f"{config_key}_engine") or "postgres"
    if db_engine not in ("postgres", "mysql"):
        raise SqlDatabaseError(
            f"Invalid engine value: {db_engine!r}. Must be 'postgres' or 'mysql'"
        )

    ssl_str = await config_namespace.get(f"{config_key}_ssl")
    ssl = (ssl_str or "true").lower() != "false"
    auth_mode = await config_namespace.get(f"{config_key}_authMode") or "password"
    read_host = await config_namespace.get(f"{config_key}_readHost")
    region = await config_namespace.get(f"{config_key}_region")

    if auth_mode == "iam":
        ssl = True

    info = SqlConnectionInfo(
        host=host,
        port=port,
        database=database,
        user=user,
        engine=db_engine,
        ssl=ssl,
        auth_mode=auth_mode,
        read_host=read_host,
    )

    if auth_mode == "iam":
        platform = detect_platform()
        token_provider = _resolve_iam_token_provider(
            platform=platform,
            host=host,
            port=port,
            user=user,
            region=region,
        )
        # Build initial URLs with a placeholder; factory uses token_provider at connect time
        initial_token = await token_provider.get_token()
        url = build_connection_url(db_engine, user, initial_token, host, port, database, ssl)
        read_url = (
            build_connection_url(db_engine, user, initial_token, read_host, port, database, ssl)
            if read_host
            else None
        )
        return info, SqlIamAuth(token_provider=token_provider, url=url, read_url=read_url)

    password = await config_namespace.get(f"{config_key}_password")
    if not password:
        raise SqlDatabaseError(f"Missing required config key: {config_key}_password")

    url = build_connection_url(db_engine, user, password, host, port, database, ssl)
    read_url = (
        build_connection_url(db_engine, user, password, read_host, port, database, ssl)
        if read_host
        else None
    )
    return info, SqlPasswordAuth(password=password, url=url, read_url=read_url)


def _resolve_iam_token_provider(
    *,
    platform: Platform,
    host: str,
    port: int,
    user: str,
    region: str | None,
) -> SqlIamTokenProvider:
    """Create a platform-specific IAM token provider.

    Raises:
        SqlDatabaseError: If the current platform does not support IAM auth.
    """
    if platform == "aws":
        from celerity.resources.sql_database.providers.rds.iam import (
            RdsIamTokenProvider,
        )

        return RdsIamTokenProvider(
            host=host,
            port=port,
            user=user,
            region=region,
        )

    raise SqlDatabaseError(
        f"IAM auth is not supported on platform {platform!r}. Supported platforms: aws"
    )
