"""WebSocket handler pipeline."""

from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any, cast

from celerity.handlers.param_extractor import resolve_handler_params
from celerity.handlers.resolve import resolve_handler_instance
from celerity.layers.pipeline import run_layer_pipeline
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.context import WebSocketHandlerContext

if TYPE_CHECKING:
    from celerity.types.container import ServiceContainer
    from celerity.types.handler import ResolvedHandlerBase
    from celerity.types.websocket import WebSocketMessage

logger = logging.getLogger("celerity.pipeline.ws")


async def execute_websocket_pipeline(
    handler: ResolvedHandlerBase,
    message: WebSocketMessage,
    options: dict[str, Any],
) -> None:
    """Execute the WebSocket handler pipeline.

    Args:
        handler: The resolved WebSocket handler.
        message: The incoming WebSocket message.
        options: Pipeline options including ``container``,
            ``system_layers``, ``module_layers``.
    """
    container = cast("ServiceContainer", options.get("container"))
    system_layers: list[Any] = options.get("system_layers", [])
    module_layers: list[Any] = options.get("module_layers", [])

    context = WebSocketHandlerContext(
        message=message,
        metadata=HandlerMetadataStore(handler.custom_metadata),
        container=container,
    )

    all_layers = [*system_layers, *module_layers, *handler.layers]
    logger.debug("event=%s — %d layers", message.event_type, len(all_layers))

    async def core_handler() -> None:
        if not handler.is_function_handler:
            await resolve_handler_instance(handler, container)
        params = resolve_handler_params(handler, context)
        result = handler.handler_fn(*params) if params else handler.handler_fn()
        if inspect.isawaitable(result):
            await result

    await run_layer_pipeline(all_layers, context, core_handler, handler_type="websocket")
