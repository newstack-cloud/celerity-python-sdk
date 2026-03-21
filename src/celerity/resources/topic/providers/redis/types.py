"""Redis topic provider configuration types."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RedisTopicConfig:
    """Configuration for a Redis topic provider."""

    url: str = "redis://localhost:6379"
