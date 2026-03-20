"""Cache client factory.

Cache uses the Redis protocol for all platforms — Valkey (local),
ElastiCache (AWS), Memorystore (GCP), and Azure Cache are all
Redis-compatible, so no platform-based provider selection is needed.

However, IAM credential providers are platform-specific: the current
ElastiCache IAM token provider (``credentials.py``) is the AWS
implementation. Future platforms (GCP Memorystore IAM, Azure Cache AAD)
would add their own credential providers, selected by platform.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from celerity.resources.cache.config import resolve_connection_config
from celerity.resources.cache.credentials import resolve_cache_credentials
from celerity.resources.cache.providers.redis.client import (
    create_redis_cache_client,
)

if TYPE_CHECKING:
    from celerity.config.service import ConfigNamespace
    from celerity.resources._common import RuntimeMode
    from celerity.resources.cache.types import CacheClient


async def create_cache_client(
    config_namespace: ConfigNamespace,
    config_key: str,
    runtime_mode: RuntimeMode,
) -> CacheClient:
    """Create a cache client from config store values.

    Resolves credentials and connection settings, then creates
    the appropriate Redis client (standalone or cluster).
    """
    connection_info, auth = await resolve_cache_credentials(config_namespace, config_key)
    connection_config = resolve_connection_config(runtime_mode)
    return await create_redis_cache_client(connection_info, auth, connection_config)
