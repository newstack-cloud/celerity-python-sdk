"""Shared fixtures for integration tests.

These tests require Docker services running via:
    docker compose up -d --wait

Or via the test script:
    ./scripts/run-tests.sh integration
"""

from __future__ import annotations

import os

import pytest

# Service connection details matching docker-compose.yml
VALKEY_URL = os.environ.get("CELERITY_TEST_REDIS_URL", "redis://localhost:6399")
LOCALSTACK_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
POSTGRES_HOST = os.environ.get("CELERITY_TEST_PG_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("CELERITY_TEST_PG_PORT", "5499"))
POSTGRES_USER = os.environ.get("CELERITY_TEST_PG_USER", "celerity_test")
POSTGRES_PASSWORD = os.environ.get("CELERITY_TEST_PG_PASSWORD", "celerity_test")
POSTGRES_DB = os.environ.get("CELERITY_TEST_PG_DB", "celerity_test")


@pytest.fixture
def valkey_url() -> str:
    """Valkey/Redis connection URL."""
    return VALKEY_URL


@pytest.fixture
def localstack_endpoint() -> str:
    """LocalStack endpoint URL."""
    return LOCALSTACK_ENDPOINT


@pytest.fixture
def aws_env(monkeypatch: pytest.MonkeyPatch, localstack_endpoint: str) -> None:
    """Set AWS environment variables for LocalStack."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", localstack_endpoint)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
