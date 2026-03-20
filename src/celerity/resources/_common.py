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
