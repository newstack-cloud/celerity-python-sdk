"""Datastore client factory."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celerity.resources.datastore.providers.dynamodb.types import (
        DynamoDBDatastoreConfig,
    )
    from celerity.resources.datastore.types import DatastoreClient


def create_datastore_client(
    config: DynamoDBDatastoreConfig | None = None,
) -> DatastoreClient:
    """Create a datastore client.

    If no config is provided, captures DynamoDB configuration from
    environment variables.
    """
    import aioboto3

    from celerity.resources.datastore.providers.dynamodb.client import (
        DynamoDBDatastoreClient,
    )
    from celerity.resources.datastore.providers.dynamodb.config import (
        capture_dynamodb_config,
    )

    resolved_config = config or capture_dynamodb_config()
    session = aioboto3.Session()
    return DynamoDBDatastoreClient(session=session, config=resolved_config)
