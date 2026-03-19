"""Layer pipeline composition and execution."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from celerity.types.context import BaseHandlerContext
    from celerity.types.layer import CelerityLayer

logger = logging.getLogger("celerity.layers")


async def run_layer_pipeline(
    layers: list[Any],
    context: BaseHandlerContext,
    handler: Callable[..., Awaitable[Any]],
    handler_type: str | None = None,
) -> Any:
    """Compose layers into a nested async call chain and execute.

    Each layer wraps the next, forming a pipeline::

        layer_0 -> layer_1 -> ... -> layer_n -> core handler

    Layers whose ``supports(handler_type)`` returns ``False`` are skipped.

    Args:
        layers: Layer instances to compose. Class-based layers should be
            resolved from the container before calling this function.
        context: The handler context passed through the pipeline.
        handler: The core handler function to invoke at the innermost
            point of the pipeline.
        handler_type: If set, layers are filtered via ``supports()``.

    Returns:
        The result from the innermost handler, possibly modified by layers.
    """
    applicable = _filter_layers(layers, handler_type)
    logger.debug("run_layer_pipeline: %d layers (handler_type=%s)", len(applicable), handler_type)
    for layer in applicable:
        logger.debug("  layer: %s", type(layer).__name__)

    async def core() -> Any:
        return await handler()

    chain: Callable[..., Awaitable[Any]] = core
    for layer in reversed(applicable):
        chain = _wrap_layer(layer, context, chain)

    return await chain()


def _filter_layers(layers: list[Any], handler_type: str | None) -> list[CelerityLayer]:
    """Filter layers by handler type support."""
    if handler_type is None:
        return layers

    result: list[CelerityLayer] = []
    for layer in layers:
        if hasattr(layer, "supports") and not layer.supports(handler_type):
            continue
        result.append(layer)
    return result


def _wrap_layer(
    layer: CelerityLayer,
    context: BaseHandlerContext,
    next_fn: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    """Wrap a single layer around the next function in the chain."""

    async def wrapped() -> Any:
        return await layer.handle(context, next_fn)

    return wrapped
