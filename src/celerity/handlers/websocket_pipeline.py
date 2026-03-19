"""WebSocket handler pipeline."""

from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any

from celerity.handlers.param_extractor import resolve_handler_params
from celerity.layers.pipeline import run_layer_pipeline
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.context import WebSocketHandlerContext

if TYPE_CHECKING:
    from celerity.types.handler import ResolvedWebSocketHandler
    from celerity.types.websocket import WebSocketMessage

logger = logging.getLogger("celerity.pipeline.ws")


async def execute_websocket_pipeline(
    handler: ResolvedWebSocketHandler,
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
    container = options.get("container")
    system_layers: list[Any] = options.get("system_layers", [])
    module_layers: list[Any] = options.get("module_layers", [])

    context = WebSocketHandlerContext(
        message=message,
        metadata=HandlerMetadataStore(handler.custom_metadata),
        container=container,  # type: ignore[arg-type]
    )

    all_layers = [*system_layers, *module_layers, *handler.layers]
    logger.debug(
        "route=%s event=%s — %d layers", message.event_type, handler.route, len(all_layers)
    )

    async def core_handler() -> None:
        params = resolve_handler_params(handler, context)
        result = handler.handler_fn(*params) if params else handler.handler_fn()
        if inspect.isawaitable(result):
            await result

    await run_layer_pipeline(all_layers, context, core_handler, handler_type="websocket")
