"""System layer creation."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("celerity.layers")


async def create_default_system_layers() -> list[Any]:
    """Create the default system layers.

    System layers are loaded dynamically based on available packages:

    1. TelemetryLayer (if ``celerity-sdk[telemetry]`` is installed)
    2. ConfigLayer (always, when implemented)
    3. Resource layers (based on ``CELERITY_RESOURCE_LINKS`` env var)

    Returns:
        An ordered list of system layer instances.
    """
    layers: list[Any] = []

    try:
        from celerity.telemetry.telemetry_layer import TelemetryLayer

        layers.append(TelemetryLayer())
        logger.debug("Loaded TelemetryLayer")
    except ImportError:
        logger.debug("TelemetryLayer not available (install celerity-sdk[telemetry])")

    return layers
