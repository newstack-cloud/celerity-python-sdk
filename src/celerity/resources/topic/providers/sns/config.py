"""SNS configuration capture from environment variables."""

from __future__ import annotations

import os

from celerity.resources.topic.providers.sns.types import SNSTopicConfig


def capture_sns_config() -> SNSTopicConfig:
    """Capture SNS client configuration from environment variables.

    This is the only place that reads environment variables for SNS config.

    Environment variables::

        AWS_REGION / AWS_DEFAULT_REGION              -- AWS region
        CELERITY_AWS_SNS_ENDPOINT / AWS_ENDPOINT_URL -- endpoint override
    """
    return SNSTopicConfig(
        region=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
        endpoint_url=(
            os.environ.get("CELERITY_AWS_SNS_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
        ),
    )
