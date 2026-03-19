"""Cache credential resolution from config store."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from celerity.resources._common import Platform, detect_platform
from celerity.resources.cache.errors import CacheError

if TYPE_CHECKING:
    from celerity.config.service import ConfigNamespace


@runtime_checkable
class CacheTokenProvider(Protocol):
    """Protocol for platform-specific IAM token providers.

    Implementations generate short-lived authentication tokens
    for managed Redis/Valkey services (e.g. ElastiCache SigV4,
    Memorystore OAuth2, Azure Entra ID).
    """

    async def get_token(self) -> str: ...


@dataclass
class CacheConnectionInfo:
    """Parsed connection parameters for a cache resource."""

    host: str
    port: int
    tls: bool
    cluster_mode: bool
    auth_mode: str
    user: str | None
    key_prefix: str
    region: str | None


@dataclass
class CachePasswordAuth:
    """Password-based authentication credentials."""

    password: str | None


@dataclass
class CacheIamAuth:
    """IAM-based authentication via a token provider."""

    token_provider: CacheTokenProvider


async def resolve_cache_credentials(
    config_namespace: ConfigNamespace,
    config_key: str,
) -> tuple[CacheConnectionInfo, CachePasswordAuth | CacheIamAuth]:
    """Resolve connection info and auth from config keys.

    Config keys read::

        {configKey}_host        (required)
        {configKey}_port        (default 6379)
        {configKey}_authMode    ("password" | "iam", default "password")
        {configKey}_tls         (default "true")
        {configKey}_clusterMode (default "false")
        {configKey}_user        (required for IAM)
        {configKey}_keyPrefix   (default "")
        {configKey}_authToken   (password mode)
        {configKey}_region      (required for IAM)
    """
    host = await config_namespace.get(f"{config_key}_host")
    if not host:
        raise CacheError(f"Missing required config key: {config_key}_host")

    port_str = await config_namespace.get(f"{config_key}_port")
    port = int(port_str) if port_str else 6379

    auth_mode = await config_namespace.get(f"{config_key}_authMode") or "password"
    tls_str = await config_namespace.get(f"{config_key}_tls")
    tls = (tls_str or "true").lower() != "false"
    cluster_mode_str = await config_namespace.get(f"{config_key}_clusterMode")
    cluster_mode = (cluster_mode_str or "false").lower() == "true"
    user = await config_namespace.get(f"{config_key}_user")
    key_prefix = await config_namespace.get(f"{config_key}_keyPrefix") or ""
    region = await config_namespace.get(f"{config_key}_region")

    if auth_mode == "iam":
        tls = True  # IAM always requires TLS
        if not user:
            raise CacheError(f"IAM auth requires {config_key}_user")
        if not region:
            raise CacheError(f"IAM auth requires {config_key}_region")

    info = CacheConnectionInfo(
        host=host,
        port=port,
        tls=tls,
        cluster_mode=cluster_mode,
        auth_mode=auth_mode,
        user=user,
        key_prefix=key_prefix,
        region=region,
    )

    if auth_mode == "iam":
        platform = detect_platform()
        token_provider = _resolve_iam_token_provider(
            platform=platform,
            host=host,
            user=user,  # type: ignore[arg-type]
            region=region,  # type: ignore[arg-type]
        )
        return info, CacheIamAuth(token_provider=token_provider)

    password = await config_namespace.get(f"{config_key}_authToken")
    return info, CachePasswordAuth(password=password)


def _resolve_iam_token_provider(
    *,
    platform: Platform,
    host: str,
    user: str,
    region: str,
) -> CacheTokenProvider:
    """Create a platform-specific IAM token provider.

    Each cloud platform uses a different mechanism to authenticate
    with managed Redis/Valkey services:

    - **AWS**: SigV4 presigned URL for ElastiCache
    - **GCP**: (v1) OAuth2 access token for Memorystore
    - **Azure**: (v1) Entra ID token for Azure Cache for Redis

    Raises:
        CacheError: If the current platform does not support IAM auth.
    """
    if platform == "aws":
        from celerity.resources.cache.providers.redis.iam.elasticache_token import (
            ElastiCacheTokenProvider,
        )

        return ElastiCacheTokenProvider(
            cache_id=host,
            user_id=user,
            region=region,
        )

    raise CacheError(
        f"IAM auth is not supported on platform {platform!r}. Supported platforms: aws"
    )
