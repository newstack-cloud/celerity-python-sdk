"""SQL database ABCs and supporting types."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from celerity.resources.sql_database.credentials import SqlIamTokenProvider


@dataclass(frozen=True)
class SqlConnectionInfo:
    """Parsed connection parameters for a SQL database resource."""

    host: str
    port: int
    database: str
    user: str
    engine: str
    ssl: bool
    auth_mode: str
    read_host: str | None = None


@dataclass(frozen=True)
class SqlPasswordAuth:
    """Password-based authentication credentials."""

    password: str
    url: str
    read_url: str | None = None


@dataclass(frozen=True)
class SqlIamAuth:
    """IAM-based authentication via a platform-specific token provider."""

    token_provider: SqlIamTokenProvider
    url: str
    read_url: str | None = None


class SqlDatabaseInstance(ABC):
    """Top-level SQL database instance managing writer and reader engines."""

    @abstractmethod
    def writer(self) -> AsyncEngine:
        """Get the writer engine (primary database)."""

    @abstractmethod
    def reader(self) -> AsyncEngine:
        """Get the reader engine (read replica).

        Falls back to the writer engine when no read replica is configured.
        """

    @abstractmethod
    async def close(self) -> None:
        """Dispose of all engine connection pools."""


class SqlDatabaseCredentials(ABC):
    """Credential accessor for SQL database connections."""

    @abstractmethod
    def get_connection_info(self) -> SqlConnectionInfo:
        """Get the resolved connection info (host, port, database, etc.)."""

    @abstractmethod
    def get_password_auth(self) -> SqlPasswordAuth:
        """Get password auth details including connection URLs.

        Raises SqlDatabaseError if auth mode is not 'password'.
        """

    @abstractmethod
    def get_iam_auth(self) -> SqlIamAuth:
        """Get IAM auth details including token provider and connection URLs.

        Raises SqlDatabaseError if auth mode is not 'iam'.
        """
