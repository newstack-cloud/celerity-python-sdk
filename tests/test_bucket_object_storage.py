"""Tests for S3ObjectStorage (session-level client)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from celerity.resources.bucket.providers.s3.client import S3ObjectStorage
from celerity.resources.bucket.providers.s3.types import S3ObjectStorageConfig


@pytest.fixture
def config() -> S3ObjectStorageConfig:
    return S3ObjectStorageConfig(
        region="us-east-1",
        endpoint_url="http://localhost:4566",
        force_path_style=True,
    )


@pytest.fixture
def session() -> MagicMock:
    mock_session = MagicMock()
    mock_client = AsyncMock()

    class FakeContextManager:
        async def __aenter__(self) -> AsyncMock:
            return mock_client

        async def __aexit__(self, *args: Any) -> None:
            pass

    mock_session.client.return_value = FakeContextManager()
    return mock_session


class TestS3ObjectStorage:
    def test_bucket_returns_s3_bucket(
        self, session: MagicMock, config: S3ObjectStorageConfig
    ) -> None:
        from celerity.resources.bucket.providers.s3.client import S3Bucket

        storage = S3ObjectStorage(session=session, config=config)
        handle = storage.bucket("my-bucket")
        assert isinstance(handle, S3Bucket)

    @pytest.mark.asyncio
    async def test_lazy_client_creation(
        self, session: MagicMock, config: S3ObjectStorageConfig
    ) -> None:
        storage = S3ObjectStorage(session=session, config=config)
        assert storage._client is None
        await storage._ensure_client()
        assert storage._client is not None
        session.client.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_client_idempotent(
        self, session: MagicMock, config: S3ObjectStorageConfig
    ) -> None:
        storage = S3ObjectStorage(session=session, config=config)
        client1 = await storage._ensure_client()
        client2 = await storage._ensure_client()
        assert client1 is client2
        session.client.assert_called_once()

    @pytest.mark.asyncio
    async def test_close(self, session: MagicMock, config: S3ObjectStorageConfig) -> None:
        storage = S3ObjectStorage(session=session, config=config)
        await storage._ensure_client()
        await storage.close()
        assert storage._client is None

    @pytest.mark.asyncio
    async def test_path_style_config(
        self,
        session: MagicMock,
    ) -> None:
        config = S3ObjectStorageConfig(
            region="us-east-1",
            endpoint_url="http://localhost:4566",
            force_path_style=True,
        )
        storage = S3ObjectStorage(session=session, config=config)
        await storage._ensure_client()
        call_kwargs = session.client.call_args
        boto_config = call_kwargs.kwargs["config"]
        assert boto_config.s3["addressing_style"] == "path"

    @pytest.mark.asyncio
    async def test_no_path_style(self, session: MagicMock) -> None:
        config = S3ObjectStorageConfig(
            region="us-east-1",
            endpoint_url=None,
            force_path_style=False,
        )
        storage = S3ObjectStorage(session=session, config=config)
        await storage._ensure_client()
        call_kwargs = session.client.call_args
        boto_config = call_kwargs.kwargs["config"]
        assert boto_config.s3 is None
