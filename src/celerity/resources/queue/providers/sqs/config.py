"""SQS configuration capture from environment variables."""

from __future__ import annotations

import os

from celerity.resources._common import capture_aws_credentials
from celerity.resources.queue.providers.sqs.types import SQSQueueConfig


def capture_sqs_config() -> SQSQueueConfig:
    """Capture SQS client configuration from environment variables.

    This is the only place that reads environment variables for SQS config.

    Environment variables::

        AWS_REGION / AWS_DEFAULT_REGION              -- AWS region
        CELERITY_AWS_SQS_ENDPOINT / AWS_ENDPOINT_URL -- endpoint override
        AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY    -- credentials
    """
    return SQSQueueConfig(
        region=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
        endpoint_url=(
            os.environ.get("CELERITY_AWS_SQS_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
        ),
        credentials=capture_aws_credentials(),
    )
