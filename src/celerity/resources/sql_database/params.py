"""SQL database parameter types and helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from sqlalchemy.ext.asyncio import AsyncEngine  # type: ignore[import-untyped]

from celerity.resources.sql_database.types import SqlDatabaseCredentials

if TYPE_CHECKING:
    from celerity.types.container import ServiceContainer


class SqlWriterParam:
    """DI marker for SQL writer engine injection.

    Used inside ``Annotated[...]`` to tell the DI resolver which
    SQL writer engine to inject.
    """

    resource_type: str = "sql:writer"

    def __init__(self, name: str | None = None) -> None:
        self.resource_name = name


class SqlReaderParam:
    """DI marker for SQL reader engine injection.

    Used inside ``Annotated[...]`` to tell the DI resolver which
    SQL reader engine to inject.
    """

    resource_type: str = "sql:reader"

    def __init__(self, name: str | None = None) -> None:
        self.resource_name = name


class SqlDatabaseParam:
    """DI marker for SQL database engine injection (convenience alias for writer).

    Used inside ``Annotated[...]``. Resolves to the writer engine for developers
    deploying applications that don't need separate reader/writer engines.
    """

    resource_type: str = "sql:writer"

    def __init__(self, name: str | None = None) -> None:
        self.resource_name = name


class SqlCredentialsMarker:
    """DI marker for SQL credentials injection.

    Used inside ``Annotated[...]`` to tell the DI resolver which
    SQL credentials to inject.
    """

    resource_type: str = "sql:credentials"

    def __init__(self, name: str | None = None) -> None:
        self.resource_name = name


# --- Token factories ---

DEFAULT_SQL_WRITER_TOKEN = "celerity:sql:writer:default"
DEFAULT_SQL_READER_TOKEN = "celerity:sql:reader:default"
DEFAULT_SQL_CREDENTIALS_TOKEN = "celerity:sql:credentials:default"


def sql_writer_token(resource_name: str) -> str:
    """Create a DI token for a named SQL writer resource."""
    return f"celerity:sql:writer:{resource_name}"


def sql_reader_token(resource_name: str) -> str:
    """Create a DI token for a named SQL reader resource."""
    return f"celerity:sql:reader:{resource_name}"


def sql_credentials_token(resource_name: str) -> str:
    """Create a DI token for named SQL credentials."""
    return f"celerity:sql:credentials:{resource_name}"


def sql_instance_token(resource_name: str) -> str:
    """Create a DI token for a named SQL database instance."""
    return f"celerity:sql:instance:{resource_name}"


# --- Type-safe aliases (what developers import and use) ---

SqlWriter = Annotated[AsyncEngine, SqlWriterParam()]
"""Default SQL writer -- type checker sees AsyncEngine,
DI resolves via ``celerity:sql:writer:default``."""

SqlReader = Annotated[AsyncEngine, SqlReaderParam()]
"""Default SQL reader -- type checker sees AsyncEngine,
DI resolves via ``celerity:sql:reader:default``."""

SqlDatabase = Annotated[AsyncEngine, SqlDatabaseParam()]
"""Default SQL database engine -- convenience alias for writer.

For applications that don't need reader/writer separation (no read replicas),
this provides the most natural API. Resolves to the same engine as SqlWriter.
"""

SqlCredentials = Annotated[SqlDatabaseCredentials, SqlCredentialsMarker()]
"""Default SQL credentials -- type checker sees SqlDatabaseCredentials,
DI resolves via ``celerity:sql:credentials:default``."""


# --- Programmatic helpers (non-DI) ---


async def get_sql_writer(
    container: ServiceContainer,
    resource_name: str | None = None,
) -> AsyncEngine:
    """Resolve a SQL writer engine from the container without DI."""
    token = sql_writer_token(resource_name) if resource_name else DEFAULT_SQL_WRITER_TOKEN
    result: AsyncEngine = await container.resolve(token)
    return result


async def get_sql_reader(
    container: ServiceContainer,
    resource_name: str | None = None,
) -> AsyncEngine:
    """Resolve a SQL reader engine from the container without DI."""
    token = sql_reader_token(resource_name) if resource_name else DEFAULT_SQL_READER_TOKEN
    result: AsyncEngine = await container.resolve(token)
    return result


async def get_sql_database(
    container: ServiceContainer,
    resource_name: str | None = None,
) -> AsyncEngine:
    """Resolve a SQL database engine from the container without DI.

    Convenience alias for get_sql_writer() for applications that don't
    need reader/writer separation.
    """
    token = sql_writer_token(resource_name) if resource_name else DEFAULT_SQL_WRITER_TOKEN
    result: AsyncEngine = await container.resolve(token)
    return result


async def get_sql_credentials(
    container: ServiceContainer,
    resource_name: str | None = None,
) -> SqlDatabaseCredentials:
    """Resolve SQL credentials from the container without DI."""
    token = sql_credentials_token(resource_name) if resource_name else DEFAULT_SQL_CREDENTIALS_TOKEN
    result: SqlDatabaseCredentials = await container.resolve(token)
    return result
