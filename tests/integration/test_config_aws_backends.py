"""Integration tests for AWS config backends (LocalStack)."""

from __future__ import annotations

import contextlib
import json
from typing import Any

import boto3

from celerity.config.backends.aws.parameter_store import AwsParameterStoreBackend
from celerity.config.backends.aws.secrets_manager import AwsSecretsManagerBackend

from .conftest import LOCALSTACK_ENDPOINT

SSM_PATH = "/celerity-test/config/"
SECRET_ID = "celerity-test/app-config"


def _ssm_client() -> Any:
    return boto3.client(
        "ssm",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _secrets_client() -> Any:
    return boto3.client(
        "secretsmanager",
        endpoint_url=LOCALSTACK_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


class TestParameterStoreBackend:
    async def test_fetches_parameters_by_path(self) -> None:
        """Store parameters under a path and fetch them."""
        client = _ssm_client()
        try:
            client.put_parameter(
                Name=f"{SSM_PATH}appCache_host",
                Value="redis.example.com",
                Type="String",
                Overwrite=True,
            )
            client.put_parameter(
                Name=f"{SSM_PATH}appCache_port",
                Value="6379",
                Type="String",
                Overwrite=True,
            )

            backend = AwsParameterStoreBackend()
            result = await backend.fetch(SSM_PATH)

            assert result["appCache_host"] == "redis.example.com"
            assert result["appCache_port"] == "6379"
        finally:
            for suffix in ["appCache_host", "appCache_port"]:
                with contextlib.suppress(Exception):
                    client.delete_parameter(Name=f"{SSM_PATH}{suffix}")

    async def test_returns_empty_for_missing_path(self) -> None:
        """Missing path returns empty dict."""
        backend = AwsParameterStoreBackend()
        result = await backend.fetch("/celerity-test/nonexistent/")
        assert result == {}


class TestSecretsManagerBackend:
    async def test_fetches_secret_as_json(self) -> None:
        """Store a JSON secret and fetch key-value pairs."""
        client = _secrets_client()
        secret_data = {
            "ordersDb_host": "dynamodb.example.com",
            "ordersDb_region": "us-east-1",
        }
        try:
            client.create_secret(
                Name=SECRET_ID,
                SecretString=json.dumps(secret_data),
            )

            backend = AwsSecretsManagerBackend()
            result = await backend.fetch(SECRET_ID)

            assert result["ordersDb_host"] == "dynamodb.example.com"
            assert result["ordersDb_region"] == "us-east-1"
        finally:
            with contextlib.suppress(Exception):
                client.delete_secret(
                    SecretId=SECRET_ID,
                    ForceDeleteWithoutRecovery=True,
                )

    async def test_returns_empty_for_non_json_secret(self) -> None:
        """Non-JSON secret returns empty dict."""
        client = _secrets_client()
        non_json_id = "celerity-test/non-json"
        try:
            client.create_secret(
                Name=non_json_id,
                SecretString="not-json",
            )

            backend = AwsSecretsManagerBackend()
            result = await backend.fetch(non_json_id)
            assert result == {}
        finally:
            with contextlib.suppress(Exception):
                client.delete_secret(
                    SecretId=non_json_id,
                    ForceDeleteWithoutRecovery=True,
                )
