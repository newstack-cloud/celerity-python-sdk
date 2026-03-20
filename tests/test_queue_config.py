"""Tests for queue configuration capture."""

from __future__ import annotations

import pytest

from celerity.resources.queue.providers.sqs.config import capture_sqs_config


class TestCaptureSqsConfig:
    def test_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.delenv("CELERITY_AWS_SQS_ENDPOINT", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        config = capture_sqs_config()
        assert config.region is None
        assert config.endpoint_url is None

    def test_aws_region(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        monkeypatch.delenv("CELERITY_AWS_SQS_ENDPOINT", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        config = capture_sqs_config()
        assert config.region == "eu-west-1"

    def test_default_region_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-1")
        monkeypatch.delenv("CELERITY_AWS_SQS_ENDPOINT", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        config = capture_sqs_config()
        assert config.region == "ap-southeast-1"

    def test_sqs_endpoint(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.setenv("CELERITY_AWS_SQS_ENDPOINT", "http://localhost:4566")
        config = capture_sqs_config()
        assert config.endpoint_url == "http://localhost:4566"

    def test_generic_endpoint_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
        monkeypatch.delenv("CELERITY_AWS_SQS_ENDPOINT", raising=False)
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
        config = capture_sqs_config()
        assert config.endpoint_url == "http://localhost:4566"
