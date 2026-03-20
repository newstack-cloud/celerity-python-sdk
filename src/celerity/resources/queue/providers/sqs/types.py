"""SQS queue provider configuration types."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SQSQueueConfig:
    """SQS client configuration.

    This is shared across all queue resources — per-resource queue
    URLs are resolved separately by the layer.
    """

    region: str | None = None
    endpoint_url: str | None = None
