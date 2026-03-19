"""Local config backend (reads from Valkey/Redis)."""

from __future__ import annotations

import json
import logging
import os

from celerity.config.backends.types import ConfigBackend

logger = logging.getLogger("celerity.config")


class LocalConfigBackend(ConfigBackend):
    """Backend that reads config from a local Valkey/Redis instance.

    Used in local development mode. The Celerity CLI manages the
    Redis instance and stores config as JSON values keyed by store ID.
    """

    def __init__(self, redis_url: str | None = None) -> None:
        self._url = redis_url or os.environ.get(
            "CELERITY_REDIS_ENDPOINT",
            "redis://localhost:6379",
        )

    async def fetch(self, store_id: str) -> dict[str, str]:
        try:
            import redis.asyncio as aioredis
        except ImportError:
            logger.warning("redis package not installed, cannot use local config backend")
            return {}

        client = aioredis.from_url(self._url)
        try:
            raw = await client.get(store_id)
            if raw is None:
                return {}
            try:
                data = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                logger.warning("local-config: invalid JSON for key %s", store_id)
                return {}
            if isinstance(data, dict):
                return {k: str(v) for k, v in data.items()}
            return {}
        finally:
            await client.close()
