"""SNS topic provider configuration types."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SNSTopicConfig:
    """SNS client configuration.

    This is shared across all topic resources — per-resource topic
    ARNs are resolved separately by the layer.
    """

    region: str | None = None
    endpoint_url: str | None = None
