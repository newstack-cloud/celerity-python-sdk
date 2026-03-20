"""Redis queue provider configuration types."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RedisQueueConfig:
    """Configuration for the Redis Streams queue provider."""

    url: str = "redis://localhost:6379"
