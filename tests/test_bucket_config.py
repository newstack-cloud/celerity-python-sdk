"""Tests for S3 configuration capture."""

from __future__ import annotations

import pytest

from celerity.resources.bucket.providers.s3.config import capture_s3_config

# Env vars that capture_s3_config reads — cleared before each relevant test.
_S3_ENV_KEYS = (
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "CELERITY_AWS_S3_ENDPOINT",
    "AWS_ENDPOINT_URL",
    "CELERITY_AWS_S3_PATH_STYLE",
    "CELERITY_RUNTIME",
)


def _clear_s3_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _S3_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


class TestCaptureS3Config:
    def test_defaults_local(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Local environment (no CELERITY_RUNTIME): force_path_style is True."""
        _clear_s3_env(monkeypatch)
        config = capture_s3_config()
        assert config.region == "us-east-1"
        assert config.endpoint_url is None
        assert config.force_path_style is True

    def test_runtime_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Runtime environment: force_path_style defaults to False."""
        _clear_s3_env(monkeypatch)
        monkeypatch.setenv("CELERITY_RUNTIME", "true")
        config = capture_s3_config()
        assert config.region == "us-east-1"
        assert config.endpoint_url is None
        assert config.force_path_style is False

    def test_aws_region(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_s3_env(monkeypatch)
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        monkeypatch.setenv("CELERITY_RUNTIME", "true")
        config = capture_s3_config()
        assert config.region == "eu-west-1"

    def test_aws_default_region_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_s3_env(monkeypatch)
        monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
        monkeypatch.setenv("CELERITY_RUNTIME", "true")
        config = capture_s3_config()
        assert config.region == "ap-south-1"

    def test_endpoint_celerity(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_s3_env(monkeypatch)
        monkeypatch.setenv("CELERITY_AWS_S3_ENDPOINT", "http://localhost:4566")
        monkeypatch.setenv("CELERITY_RUNTIME", "true")
        config = capture_s3_config()
        assert config.endpoint_url == "http://localhost:4566"

    def test_endpoint_aws_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_s3_env(monkeypatch)
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
        monkeypatch.setenv("CELERITY_RUNTIME", "true")
        config = capture_s3_config()
        assert config.endpoint_url == "http://localhost:9000"

    def test_path_style_explicit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _clear_s3_env(monkeypatch)
        monkeypatch.setenv("CELERITY_AWS_S3_PATH_STYLE", "true")
        monkeypatch.setenv("CELERITY_RUNTIME", "true")
        config = capture_s3_config()
        assert config.force_path_style is True

    def test_local_always_path_style(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Even if CELERITY_AWS_S3_PATH_STYLE=false, local forces True."""
        _clear_s3_env(monkeypatch)
        monkeypatch.setenv("CELERITY_AWS_S3_PATH_STYLE", "false")
        config = capture_s3_config()
        assert config.force_path_style is True
