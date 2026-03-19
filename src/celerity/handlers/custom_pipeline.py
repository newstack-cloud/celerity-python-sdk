"""Custom (invoke) handler pipeline."""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from celerity.handlers.param_extractor import resolve_handler_params
from celerity.layers.pipeline import run_layer_pipeline
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.context import BaseHandlerContext

if TYPE_CHECKING:
    from celerity.types.handler import ResolvedCustomHandler

logger = logging.getLogger("celerity.pipeline.custom")


@dataclass
class CustomHandlerContext(BaseHandlerContext):
    """Context for custom/invoke handler pipelines."""

    payload: Any = None


async def execute_custom_pipeline(
    handler: ResolvedCustomHandler,
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
    container = options.get("container")
    system_layers: list[Any] = options.get("system_layers", [])
    module_layers: list[Any] = options.get("module_layers", [])

    context = CustomHandlerContext(
        payload=payload,
        metadata=HandlerMetadataStore(handler.custom_metadata),
        container=container,  # type: ignore[arg-type]
    )

    all_layers = [*system_layers, *module_layers, *handler.layers]
    logger.debug("name=%s — %d layers", handler.name, len(all_layers))

    async def core_handler() -> Any:
        params = resolve_handler_params(handler, context)
        result = handler.handler_fn(*params) if params else handler.handler_fn()
        if inspect.isawaitable(result):
            result = await result
        return result

    return await run_layer_pipeline(all_layers, context, core_handler, handler_type="custom")
