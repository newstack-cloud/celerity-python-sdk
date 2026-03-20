"""Object storage client factory with platform-based provider selection."""

from __future__ import annotations

from typing import TYPE_CHECKING

from celerity.resources._common import detect_platform
from celerity.resources.bucket.errors import BucketError

if TYPE_CHECKING:
    from celerity.resources._common import Platform
    from celerity.resources.bucket.providers.s3.types import S3ObjectStorageConfig
    from celerity.resources.bucket.types import ObjectStorage
    from celerity.types.telemetry import CelerityTracer


def create_object_storage(
    config: S3ObjectStorageConfig | None = None,
    tracer: CelerityTracer | None = None,
    provider: Platform | None = None,
) -> ObjectStorage:
    """Create an ObjectStorage instance for the detected platform.

    Provider selection is based on ``CELERITY_PLATFORM``:

    - ``"aws"`` → S3
    - ``"local"`` → S3 (MinIO is always S3-compatible)
    - ``"gcp"`` → Google Cloud Storage (not yet implemented)
    - ``"azure"`` → Azure Blob Storage (not yet implemented)

    Args:
        config: Optional provider-specific config. If ``None``, captured
            from environment variables.
        tracer: Optional tracer for instrumenting operations.
        provider: Override platform detection (mainly for testing).
    """
    resolved_provider = provider or detect_platform()

    if resolved_provider == "aws":
        return _create_s3_storage(config, tracer)

    if resolved_provider == "local":
        # Local environments use MinIO (S3-compatible).
        # capture_s3_config() reads env vars including force_path_style
        # which is set to True for local environments.
        return _create_s3_storage(config, tracer)

    # Future: "gcp" -> GCS, "azure" -> Azure Blob Storage
    raise BucketError(f"Unsupported object storage provider: {resolved_provider!r}")


def _create_s3_storage(
    config: S3ObjectStorageConfig | None,
    tracer: CelerityTracer | None,
) -> ObjectStorage:
    import aioboto3

    from celerity.resources.bucket.providers.s3.client import S3ObjectStorage
    from celerity.resources.bucket.providers.s3.config import capture_s3_config

    resolved_config = config or capture_s3_config()
    session = aioboto3.Session()
    return S3ObjectStorage(session=session, config=resolved_config, tracer=tracer)
