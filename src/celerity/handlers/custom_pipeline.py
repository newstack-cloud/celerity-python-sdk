"""Custom (invoke) handler pipeline."""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from celerity.handlers.param_extractor import resolve_handler_params
from celerity.handlers.resolve import resolve_handler_instance
from celerity.layers.pipeline import run_layer_pipeline
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.context import BaseHandlerContext

if TYPE_CHECKING:
    from celerity.types.container import ServiceContainer
    from celerity.types.handler import ResolvedHandlerBase

logger = logging.getLogger("celerity.pipeline.custom")


@dataclass
class CustomHandlerContext(BaseHandlerContext):
    """Context for custom/invoke handler pipelines."""

    payload: Any = None


async def execute_custom_pipeline(
    handler: ResolvedHandlerBase,
    payload: Any,
    options: dict[str, Any],
) -> Any:
    """Execute the custom handler pipeline.

    Args:
        handler: The resolved custom handler.
        payload: The invocation payload.
        options: Pipeline options including ``container``,
            ``system_layers``, ``module_layers``.

    Returns:
        The handler's return value.
    """
    container = cast("ServiceContainer", options.get("container"))
    system_layers: list[Any] = options.get("system_layers", [])
    module_layers: list[Any] = options.get("module_layers", [])

    context = CustomHandlerContext(
        payload=payload,
        metadata=HandlerMetadataStore(handler.custom_metadata),
        container=container,
    )

    all_layers = [*system_layers, *module_layers, *handler.layers]
    handler_name = options.get("handler_name", "unknown")
    logger.debug("name=%s — %d layers", handler_name, len(all_layers))

    async def core_handler() -> Any:
        if not handler.is_function_handler:
            await resolve_handler_instance(handler, container)
        params = resolve_handler_params(handler, context)
        result = handler.handler_fn(*params) if params else handler.handler_fn()
        if inspect.isawaitable(result):
            result = await result
        return result

    return await run_layer_pipeline(all_layers, context, core_handler, handler_type="custom")
