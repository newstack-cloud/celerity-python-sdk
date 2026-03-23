"""Consumer handler pipeline."""

from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any, cast

from celerity.handlers.param_extractor import resolve_handler_params
from celerity.handlers.resolve import resolve_handler_instance
from celerity.layers.pipeline import run_layer_pipeline
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.consumer import EventResult
from celerity.types.context import ConsumerHandlerContext

if TYPE_CHECKING:
    from celerity.types.consumer import ConsumerEventInput
    from celerity.types.container import ServiceContainer
    from celerity.types.handler import ResolvedHandlerBase

logger = logging.getLogger("celerity.pipeline.consumer")


async def execute_consumer_pipeline(
    handler: ResolvedHandlerBase,
    event: ConsumerEventInput,
    options: dict[str, Any],
) -> EventResult:
    """Execute the consumer handler pipeline.

    Args:
        handler: The resolved consumer handler.
        event: The consumer event input with messages.
        options: Pipeline options including ``container``,
            ``system_layers``, ``module_layers``.

    Returns:
        An ``EventResult`` indicating success or partial failure.
    """
    container = cast("ServiceContainer", options.get("container"))
    system_layers: list[Any] = options.get("system_layers", [])
    module_layers: list[Any] = options.get("module_layers", [])

    context = ConsumerHandlerContext(
        event=event,
        metadata=HandlerMetadataStore(handler.custom_metadata),
        container=container,
    )

    all_layers = [*system_layers, *module_layers, *handler.layers]
    logger.debug(
        "tag=%s — %d messages, %d layers", event.handler_tag, len(event.messages), len(all_layers)
    )

    async def core_handler() -> EventResult:
        if not handler.is_function_handler:
            await resolve_handler_instance(handler, container)
        params = resolve_handler_params(handler, context)
        result = handler.handler_fn(*params) if params else handler.handler_fn()
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, EventResult):
            return result
        return EventResult(success=True)

    try:
        result = await run_layer_pipeline(
            all_layers, context, core_handler, handler_type="consumer"
        )
        if isinstance(result, EventResult):
            return result
        return EventResult(success=True)
    except Exception:
        logger.exception("Error in consumer pipeline")
        return EventResult(success=False, error_message="Consumer handler failed")
