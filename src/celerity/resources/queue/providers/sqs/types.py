"""SQS queue provider configuration types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celerity.resources._common import AwsCredentials


@dataclass(frozen=True, slots=True)
class SQSQueueConfig:
    """SQS client configuration.

    This is shared across all queue resources — per-resource queue
    URLs are resolved separately by the layer.
    """

    region: str | None = None
    endpoint_url: str | None = None
    credentials: AwsCredentials | None = None
