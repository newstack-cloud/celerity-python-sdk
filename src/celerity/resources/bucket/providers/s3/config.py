"""S3 configuration capture from environment variables."""

from __future__ import annotations

import os

from celerity.resources.bucket.providers.s3.types import S3ObjectStorageConfig


def capture_s3_config() -> S3ObjectStorageConfig:
    """Capture S3 client configuration from environment variables.

    This is the only place that reads environment variables for S3 config.

    Environment variables::

        AWS_REGION / AWS_DEFAULT_REGION          -- AWS region (default "us-east-1")
        CELERITY_AWS_S3_ENDPOINT / AWS_ENDPOINT_URL -- endpoint override
        CELERITY_AWS_S3_PATH_STYLE               -- force path style ("true"/"false")

    In local environments (``CELERITY_RUNTIME`` not set), ``force_path_style``
    is always ``True`` for MinIO compatibility.
    """
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    endpoint_url = os.environ.get("CELERITY_AWS_S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
    path_style_env = os.environ.get("CELERITY_AWS_S3_PATH_STYLE", "false")
    force_path_style = path_style_env.lower() == "true"

    # In local environments (functions deploy target), always force path style
    # for MinIO compatibility.
    is_local = not os.environ.get("CELERITY_RUNTIME")
    if is_local:
        force_path_style = True

    return S3ObjectStorageConfig(
        region=region,
        endpoint_url=endpoint_url,
        force_path_style=force_path_style,
    )
