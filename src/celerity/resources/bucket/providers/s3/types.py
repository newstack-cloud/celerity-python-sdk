"""S3 provider configuration types."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class S3ObjectStorageConfig:
    """S3 client configuration.

    This is shared across all bucket resources — per-resource bucket
    names are resolved separately by the layer.
    """

    region: str | None = None
    endpoint_url: str | None = None
    force_path_style: bool = False
