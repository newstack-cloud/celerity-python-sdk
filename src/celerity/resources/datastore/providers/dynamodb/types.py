"""DynamoDB provider configuration types."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DynamoDBDatastoreConfig:
    """DynamoDB client configuration (region, endpoint, credentials).

    This is shared across all datastore resources — per-resource table
    names are resolved separately by the layer.
    """

    region: str | None = None
    endpoint_url: str | None = None
