"""Tests for DynamoDB datastore config capture."""

from __future__ import annotations

import pytest

from celerity.resources.datastore.providers.dynamodb.config import (
    capture_dynamodb_config,
)


class TestCaptureDynamoDBConfig:
    def test_region_from_aws_region(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.delenv("CELERITY_AWS_DYNAMODB_ENDPOINT", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        config = capture_dynamodb_config()
        assert config.region == "us-east-1"
        assert config.endpoint_url is None

    def test_region_fallback_to_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
        monkeypatch.delenv("CELERITY_AWS_DYNAMODB_ENDPOINT", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        config = capture_dynamodb_config()
        assert config.region == "eu-west-1"

    def test_no_region(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.delenv("CELERITY_AWS_DYNAMODB_ENDPOINT", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        config = capture_dynamodb_config()
        assert config.region is None

    def test_endpoint_from_celerity_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.setenv("CELERITY_AWS_DYNAMODB_ENDPOINT", "http://localhost:8000")
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        config = capture_dynamodb_config()
        assert config.endpoint_url == "http://localhost:8000"

    def test_endpoint_fallback_to_aws_endpoint_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.delenv("CELERITY_AWS_DYNAMODB_ENDPOINT", raising=False)
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
        config = capture_dynamodb_config()
        assert config.endpoint_url == "http://localhost:4566"

    def test_celerity_endpoint_takes_precedence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.setenv("CELERITY_AWS_DYNAMODB_ENDPOINT", "http://specific:8000")
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://generic:4566")
        config = capture_dynamodb_config()
        assert config.endpoint_url == "http://specific:8000"
