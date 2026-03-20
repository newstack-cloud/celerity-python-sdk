"""Redis cache client: standalone and cluster creation."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from redis.asyncio import Redis, RedisCluster
from redis.asyncio.retry import Retry
from redis.backoff import EqualJitterBackoff

from celerity.resources.cache.credentials import (
    CacheConnectionInfo,
    CacheIamAuth,
    CachePasswordAuth,
)
from celerity.resources.cache.providers.redis.cache import RedisCache
from celerity.resources.cache.types import Cache, CacheClient

if TYPE_CHECKING:
    from celerity.resources.cache.config import ConnectionConfig

logger = logging.getLogger("celerity.cache.redis")


class RedisCacheClient(CacheClient):
    """CacheClient backed by redis-py (standalone or cluster)."""

    def __init__(
        self,
        client: Redis[str] | RedisCluster[str],
        cluster_mode: bool,
        tracer: Any | None = None,
    ) -> None:
        self._client: Any = client
        self._cluster_mode = cluster_mode
        self._tracer = tracer

    def cache(self, name: str, key_prefix: str | None = None) -> Cache:
        return RedisCache(
            client=self._client,
            cluster_mode=self._cluster_mode,
            key_prefix=key_prefix or "",
            resource_name=name,
            tracer=self._tracer,
        )

    async def close(self) -> None:
        await self._client.aclose()


async def create_redis_cache_client(
    connection_info: CacheConnectionInfo,
    auth: CachePasswordAuth | CacheIamAuth,
    connection_config: ConnectionConfig,
    tracer: Any | None = None,
) -> RedisCacheClient:
    """Create a Redis client (standalone or cluster).

    Standalone: ``redis.asyncio.Redis(host, port, **opts)``
    Cluster: ``redis.asyncio.RedisCluster(host, port, **opts)``
    """
    retry = Retry(
        backoff=EqualJitterBackoff(base=connection_config.retry_delay_ms / 1000),
        retries=connection_config.max_retries,
    )

    common_kwargs: dict[str, Any] = {
        "host": connection_info.host,
        "port": connection_info.port,
        "ssl": connection_info.tls,
        "socket_connect_timeout": connection_config.connect_timeout_ms / 1000,
        "retry": retry,
        "retry_on_error": [ConnectionError, TimeoutError],
        "decode_responses": True,
    }

    if connection_config.command_timeout_ms > 0:
        common_kwargs["socket_timeout"] = connection_config.command_timeout_ms / 1000

    if isinstance(auth, CachePasswordAuth):
        if auth.password:
            common_kwargs["password"] = auth.password
    else:
        common_kwargs["credential_provider"] = _make_credential_provider(auth)

    if connection_info.user:
        common_kwargs["username"] = connection_info.user

    if connection_info.cluster_mode:
        client: Redis[str] | RedisCluster[str] = RedisCluster(
            **common_kwargs,
            read_from_replicas=True,
        )
        logger.debug(
            "cache: created cluster client %s:%d (tls=%s)",
            connection_info.host,
            connection_info.port,
            connection_info.tls,
        )
    else:
        client = Redis(**common_kwargs)
        logger.debug(
            "cache: created standalone client %s:%d (tls=%s)",
            connection_info.host,
            connection_info.port,
            connection_info.tls,
        )

    if not connection_config.lazy_connect:
        await client.ping()  # type: ignore[union-attr]

    return RedisCacheClient(client=client, cluster_mode=connection_info.cluster_mode, tracer=tracer)


def _make_credential_provider(auth: CacheIamAuth) -> Any:
    """Wrap the IAM token provider as a redis-py credential provider."""
    from redis.credentials import CredentialProvider

    from celerity.resources.cache.credentials import CacheTokenProvider

    class _IamCredentialProvider(CredentialProvider):
        def __init__(self, token_provider: CacheTokenProvider) -> None:
            self._token_provider = token_provider

        async def get_credentials(self) -> tuple[str, ...]:  # type: ignore[override]
            token = await self._token_provider.get_token()
            return (token,)

    return _IamCredentialProvider(auth.token_provider)
