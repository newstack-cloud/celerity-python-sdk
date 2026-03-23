"""Redis queue provider configuration types."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


class RedisMessageType(IntEnum):
    """Message encoding type for Redis stream messages.

    Matches the Rust runtime's ``RedisMessageType`` enum:
    ``0`` = Text (UTF-8 string), ``1`` = Binary (base64-encoded).
    """

    TEXT = 0
    BINARY = 1


@dataclass(frozen=True, slots=True)
class RedisQueueConfig:
    """Configuration for the Redis Streams queue provider."""

    url: str = "redis://localhost:6379"
