"""Tests for datastore factory platform-based provider selection."""

from __future__ import annotations

import pytest

from celerity.resources.datastore.errors import DatastoreError
from celerity.resources.datastore.factory import create_datastore_client
from celerity.resources.datastore.providers.dynamodb.client import (
    DynamoDBDatastoreClient,
)


class TestProviderDispatch:
    def test_aws(self) -> None:
        client = create_datastore_client(provider="aws")
        assert isinstance(client, DynamoDBDatastoreClient)

    def test_default_reads_platform_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_PLATFORM", "aws")
        client = create_datastore_client()
        assert isinstance(client, DynamoDBDatastoreClient)

    def test_gcp_raises(self) -> None:
        with pytest.raises(DatastoreError, match="gcp"):
            create_datastore_client(provider="gcp")

    def test_azure_raises(self) -> None:
        with pytest.raises(DatastoreError, match="azure"):
            create_datastore_client(provider="azure")


class TestLocalProviderDispatch:
    def test_local_default_deploy_target_aws(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_DEPLOY_TARGET", raising=False)
        client = create_datastore_client(provider="local")
        assert isinstance(client, DynamoDBDatastoreClient)

    def test_local_aws_deploy_target(self) -> None:
        client = create_datastore_client(provider="local", deploy_target="aws")
        assert isinstance(client, DynamoDBDatastoreClient)

    def test_local_aws_serverless_deploy_target(self) -> None:
        client = create_datastore_client(provider="local", deploy_target="aws-serverless")
        assert isinstance(client, DynamoDBDatastoreClient)

    def test_local_deploy_target_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_DEPLOY_TARGET", "aws")
        client = create_datastore_client(provider="local")
        assert isinstance(client, DynamoDBDatastoreClient)

    def test_local_gcloud_deploy_target_raises(self) -> None:
        with pytest.raises(DatastoreError, match="gcloud"):
            create_datastore_client(provider="local", deploy_target="gcloud")

    def test_local_gcloud_serverless_deploy_target_raises(self) -> None:
        with pytest.raises(DatastoreError, match="gcloud-serverless"):
            create_datastore_client(provider="local", deploy_target="gcloud-serverless")

    def test_local_azure_deploy_target_raises(self) -> None:
        with pytest.raises(DatastoreError, match="azure"):
            create_datastore_client(provider="local", deploy_target="azure")
