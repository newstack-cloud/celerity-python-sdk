"""Tests for bucket factory platform-based provider selection."""

from __future__ import annotations

import pytest

from celerity.resources.bucket.errors import BucketError
from celerity.resources.bucket.factory import create_object_storage
from celerity.resources.bucket.providers.s3.client import S3ObjectStorage


class TestProviderDispatch:
    def test_aws(self) -> None:
        storage = create_object_storage(provider="aws")
        assert isinstance(storage, S3ObjectStorage)

    def test_local_uses_s3(self) -> None:
        """Local environments use MinIO (S3-compatible)."""
        storage = create_object_storage(provider="local")
        assert isinstance(storage, S3ObjectStorage)

    def test_default_reads_platform_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_PLATFORM", "aws")
        storage = create_object_storage()
        assert isinstance(storage, S3ObjectStorage)

    def test_gcp_raises(self) -> None:
        with pytest.raises(BucketError, match="gcp"):
            create_object_storage(provider="gcp")

    def test_azure_raises(self) -> None:
        with pytest.raises(BucketError, match="azure"):
            create_object_storage(provider="azure")
