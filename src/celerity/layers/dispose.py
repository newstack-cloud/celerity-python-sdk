"""Layer disposal utilities."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("celerity.layers")


async def dispose_layers(layers: list[Any]) -> None:
    """Call ``dispose()`` on all layers that support it.

    Exceptions are logged but do not prevent other layers from being
    disposed.

    Args:
        layers: Layer instances to dispose.
    """
    for layer in layers:
        dispose_fn = getattr(layer, "dispose", None)
        if dispose_fn is not None and callable(dispose_fn):
            try:
                await dispose_fn()
            except Exception:
                logger.exception("Error disposing layer %s", type(layer).__name__)
