"""Redis provider configuration types."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RedisCacheConfig:
    """Configuration for the Redis cache provider."""

    host: str
    port: int
    tls: bool
    cluster_mode: bool
    password: str | None
    username: str | None
    credential_provider: object | None
    connect_timeout_s: float
    command_timeout_s: float
    max_retries: int
    retry_delay_s: float
    lazy_connect: bool
    key_prefix: str
