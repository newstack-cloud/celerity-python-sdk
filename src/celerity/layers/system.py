"""System layer creation."""

from __future__ import annotations

import importlib
import logging
from typing import Any

logger = logging.getLogger("celerity.layers")

RESOURCE_LAYER_MAP: dict[str, tuple[str, str]] = {
    "datastore": ("celerity.resources.datastore.layer", "DatastoreLayer"),
    "bucket": ("celerity.resources.bucket.layer", "ObjectStorageLayer"),
    "queue": ("celerity.resources.queue.layer", "QueueLayer"),
    "topic": ("celerity.resources.topic.layer", "TopicLayer"),
    "cache": ("celerity.resources.cache.layer", "CacheLayer"),
    "sqlDatabase": ("celerity.resources.sql_database.layer", "SqlDatabaseLayer"),
}


def create_default_system_layers() -> list[Any]:
    """Create the default system layers.

    Layer order:

    1. TelemetryLayer (optional -- if ``celerity-sdk[telemetry]`` installed)
    2. ConfigLayer (always -- resource layers depend on it)
    3. Resource layers (driven by ``CELERITY_RESOURCE_LINKS``)

    Resource layers are loaded dynamically: only packages for resource
    types present in the links are imported. If a resource package is
    not installed, the layer is silently skipped.

    Returns:
        An ordered list of system layer instances.
    """
    layers: list[Any] = []

    # 1. Telemetry (optional)
    try:
        from celerity.telemetry.telemetry_layer import TelemetryLayer

        layers.append(TelemetryLayer())
        logger.debug("Loaded TelemetryLayer")
    except ImportError:
        logger.debug("TelemetryLayer not available (install celerity-sdk[telemetry])")

    # 2. Config (always -- resource layers read config from it)
    from celerity.config.layer import ConfigLayer

    layers.append(ConfigLayer())
    logger.debug("Loaded ConfigLayer")

    # 3. Resource layers (driven by CELERITY_RESOURCE_LINKS)
    from celerity.resources._common import capture_resource_links, get_resource_types

    links = capture_resource_links()
    resource_types = get_resource_types(links)

    for resource_type in resource_types:
        entry = RESOURCE_LAYER_MAP.get(resource_type)
        if entry is None:
            continue

        module_path, class_name = entry
        try:
            mod = importlib.import_module(module_path)
            layer_class = getattr(mod, class_name)
            layers.append(layer_class())
            logger.debug("Loaded %s for resource type %s", class_name, resource_type)
        except (ImportError, AttributeError):
            logger.debug(
                "%s not available for resource type %s (install the resource package)",
                class_name,
                resource_type,
            )

    return layers
