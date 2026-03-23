"""Shared resource infrastructure: resource links, platform, deploy target."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("celerity.resource")

type Platform = str
"""One of ``"aws"``, ``"gcp"``, ``"azure"``, ``"local"``, ``"other"``."""

type RuntimeMode = str
"""One of ``"functions"`` or ``"runtime"``."""


@dataclass(frozen=True)
class ResourceLink:
    """A single resource link from the blueprint."""

    type: str
    config_key: str


def capture_resource_links() -> dict[str, ResourceLink]:
    """Parse ``CELERITY_RESOURCE_LINKS`` env var.

    The env var contains JSON mapping resource names to their type and config key::

        {
            "orders-db": {"type": "datastore", "configKey": "ordersDb"},
            "app-cache": {"type": "cache", "configKey": "appCache"}
        }

    Returns:
        Dict mapping resource name to ``ResourceLink``.
        Empty dict if the env var is not set or empty.
    """
    raw = os.environ.get("CELERITY_RESOURCE_LINKS", "")
    if not raw:
        return {}

    try:
        parsed: dict[str, Any] = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Invalid CELERITY_RESOURCE_LINKS JSON: %s", raw[:100])
        return {}

    links: dict[str, ResourceLink] = {}
    for name, value in parsed.items():
        if isinstance(value, dict) and "type" in value and "configKey" in value:
            links[name] = ResourceLink(
                type=value["type"],
                config_key=value["configKey"],
            )
    return links


def get_links_of_type(
    links: dict[str, ResourceLink],
    resource_type: str,
) -> dict[str, str]:
    """Filter links to a specific type.

    Returns:
        Dict mapping resource name to config key for the given type.
    """
    return {name: link.config_key for name, link in links.items() if link.type == resource_type}


def get_resource_types(links: dict[str, ResourceLink]) -> set[str]:
    """Get the set of distinct resource types present in links."""
    return {link.type for link in links.values()}


def detect_platform() -> Platform:
    """Read ``CELERITY_PLATFORM`` env var. Defaults to ``"other"``."""
    return os.environ.get("CELERITY_PLATFORM", "other")


def detect_runtime_mode() -> RuntimeMode:
    """Detect the runtime execution mode from environment.

    ``CELERITY_RUNTIME`` present → ``"runtime"`` (long-lived server),
    absent → ``"functions"`` (serverless/FaaS).
    """
    return "runtime" if os.environ.get("CELERITY_RUNTIME") else "functions"


def detect_cloud_deploy_target() -> str:
    """Read ``CELERITY_DEPLOY_TARGET`` env var.

    The deploy target identifies the cloud deployment configuration,
    e.g. ``"aws"``, ``"aws-serverless"``, ``"gcloud"``,
    ``"gcloud-serverless"``, ``"azure"``, ``"azure-serverless"``.

    Only used by the datastore factory in local environments to select
    the correct emulator (e.g. DynamoDB Local for AWS targets).
    Defaults to ``"aws"`` when not set.
    """
    return os.environ.get("CELERITY_DEPLOY_TARGET", "aws")


@dataclass(frozen=True, slots=True)
class AwsCredentials:
    """Explicit AWS credentials that override the default credential chain."""

    access_key_id: str
    secret_access_key: str


def capture_aws_credentials(
    access_key_env: str = "AWS_ACCESS_KEY_ID",
    secret_key_env: str = "AWS_SECRET_ACCESS_KEY",
) -> AwsCredentials | None:
    """Capture AWS credentials from environment variables.

    Args:
        access_key_env: Primary env var for the access key.
        secret_key_env: Primary env var for the secret key.

    Returns:
        ``AwsCredentials`` if both values are present, otherwise ``None``
        (falling back to the default credential chain).
    """
    access_key = os.environ.get(access_key_env) or os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get(secret_key_env) or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access_key and secret_key:
        return AwsCredentials(access_key_id=access_key, secret_access_key=secret_key)
    return None
