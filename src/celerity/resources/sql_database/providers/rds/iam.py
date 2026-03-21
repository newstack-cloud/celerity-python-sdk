"""RDS IAM authentication token provider."""

from __future__ import annotations

import time
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types_aiobotocore_rds.client import RDSClient


class RdsIamTokenProvider:
    """Generates IAM auth tokens for RDS database connections.

    Uses aioboto3's async RDS client ``generate_db_auth_token()`` to
    create short-lived tokens. Tokens are cached for ~14 minutes
    (valid for 15) to avoid unnecessary regeneration.

    Uses lazy import of aioboto3 to avoid import cost when not needed.
    """

    _TOKEN_TTL_SECONDS = 15 * 60
    _REFRESH_BUFFER_SECONDS = 60

    def __init__(self, host: str, port: int, user: str, region: str | None = None) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._region = region
        self._cached_token: str | None = None
        self._token_expiry: float = 0.0
        self._exit_stack = AsyncExitStack()
        self._client: RDSClient | None = None

    async def _ensure_client(self) -> RDSClient:
        """Lazily create the RDS client on first use."""
        if self._client is None:
            import aioboto3

            session = aioboto3.Session()
            self._client = await self._exit_stack.enter_async_context(
                session.client("rds", region_name=self._region)
            )
        return self._client

    async def get_token(self) -> str:
        """Return a fresh or cached IAM auth token."""
        now = time.monotonic()
        if self._cached_token and now < self._token_expiry:
            return self._cached_token

        token = await self._generate_token()
        self._cached_token = token
        self._token_expiry = now + self._TOKEN_TTL_SECONDS - self._REFRESH_BUFFER_SECONDS
        return token

    async def _generate_token(self) -> str:
        """Generate a new RDS IAM auth token using aioboto3."""
        client = await self._ensure_client()
        token: str = await client.generate_db_auth_token(
            DBHostname=self._host,
            Port=self._port,
            DBUsername=self._user,
        )
        return token

    async def close(self) -> None:
        """Close the underlying RDS client."""
        await self._exit_stack.aclose()
        self._client = None
