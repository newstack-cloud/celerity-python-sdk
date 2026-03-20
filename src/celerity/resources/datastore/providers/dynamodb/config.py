"""DynamoDB configuration capture from environment variables."""

from __future__ import annotations

import os

from celerity.resources.datastore.providers.dynamodb.types import DynamoDBDatastoreConfig


def capture_dynamodb_config() -> DynamoDBDatastoreConfig:
    """Capture DynamoDB client configuration from environment variables.

    This is the only place that reads environment variables for DynamoDB config.

    Environment variables::

        AWS_REGION / AWS_DEFAULT_REGION   -- AWS region
        CELERITY_AWS_DYNAMODB_ENDPOINT / AWS_ENDPOINT_URL -- endpoint override
    """
    return DynamoDBDatastoreConfig(
        region=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
        endpoint_url=(
            os.environ.get("CELERITY_AWS_DYNAMODB_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
        ),
    )
