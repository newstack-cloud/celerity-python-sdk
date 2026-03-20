"""Object storage client factory."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celerity.resources.bucket.providers.s3.types import S3ObjectStorageConfig
    from celerity.resources.bucket.types import ObjectStorage
    from celerity.types.telemetry import CelerityTracer


def create_object_storage(
    config: S3ObjectStorageConfig | None = None,
    tracer: CelerityTracer | None = None,
) -> ObjectStorage:
    """Create an ObjectStorage instance.

    If no config is provided, captures S3 configuration from
    environment variables. If a tracer is provided, bucket operations
    will be wrapped in tracing spans.
    """
    import aioboto3

    from celerity.resources.bucket.providers.s3.client import S3ObjectStorage
    from celerity.resources.bucket.providers.s3.config import capture_s3_config

    resolved_config = config or capture_s3_config()
    session = aioboto3.Session()
    return S3ObjectStorage(session=session, config=resolved_config, tracer=tracer)
