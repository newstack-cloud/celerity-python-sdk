"""Datastore client factory with platform-based provider selection."""

from __future__ import annotations

from typing import TYPE_CHECKING

from celerity.resources._common import detect_cloud_deploy_target, detect_platform
from celerity.resources.datastore.errors import DatastoreError

if TYPE_CHECKING:
    from celerity.resources._common import Platform
    from celerity.resources.datastore.providers.dynamodb.types import (
        DynamoDBDatastoreConfig,
    )
    from celerity.resources.datastore.types import DatastoreClient
    from celerity.types.telemetry import CelerityTracer


def create_datastore_client(
    config: DynamoDBDatastoreConfig | None = None,
    tracer: CelerityTracer | None = None,
    provider: Platform | None = None,
    deploy_target: str | None = None,
) -> DatastoreClient:
    """Create a datastore client for the detected platform.

    Provider selection is based on ``CELERITY_PLATFORM``:

    - ``"aws"`` → DynamoDB
    - ``"local"`` → Selects emulator based on ``CELERITY_DEPLOY_TARGET``:
      - ``"aws"`` / ``"aws-serverless"`` / unset → DynamoDB Local
      - ``"gcloud"`` / ``"gcloud-serverless"`` → Firestore emulator (not yet implemented)
      - ``"azure"`` / ``"azure-serverless"`` → CosmosDB emulator (not yet implemented)
    - ``"gcp"`` → Firestore (not yet implemented)
    - ``"azure"`` → CosmosDB (not yet implemented)

    Args:
        config: Optional provider-specific config. If ``None``, captured
            from environment variables.
        tracer: Optional tracer for instrumenting operations.
        provider: Override platform detection (mainly for testing).
        deploy_target: Override deploy target detection (mainly for testing).
    """
    resolved_provider = provider or detect_platform()

    if resolved_provider == "aws":
        return _create_dynamodb_client(config, tracer)

    if resolved_provider == "local":
        return _create_local_client(config, tracer, deploy_target)

    # Future: "gcp" -> Firestore, "azure" -> CosmosDB
    raise DatastoreError(f"Unsupported datastore provider: {resolved_provider!r}")


def _create_dynamodb_client(
    config: DynamoDBDatastoreConfig | None,
    tracer: CelerityTracer | None,
) -> DatastoreClient:
    import aioboto3

    from celerity.resources.datastore.providers.dynamodb.client import (
        DynamoDBDatastoreClient,
    )
    from celerity.resources.datastore.providers.dynamodb.config import (
        capture_dynamodb_config,
    )

    resolved_config = config or capture_dynamodb_config()
    session = aioboto3.Session()
    return DynamoDBDatastoreClient(session=session, config=resolved_config, tracer=tracer)


def _create_local_client(
    config: DynamoDBDatastoreConfig | None,
    tracer: CelerityTracer | None,
    deploy_target: str | None,
) -> DatastoreClient:
    """Select the local emulator based on deploy target."""
    target = (deploy_target or detect_cloud_deploy_target()).lower()

    if target in ("aws", "aws-serverless"):
        # DynamoDB Local (v0 default when no deploy target is specified)
        return _create_dynamodb_client(config, tracer)

    # Future:
    # "gcloud" / "gcloud-serverless" -> Firestore emulator
    # "azure" / "azure-serverless" -> CosmosDB emulator
    raise DatastoreError(
        f"Unsupported local datastore deploy target: {target!r}. Only AWS is supported in v0."
    )
