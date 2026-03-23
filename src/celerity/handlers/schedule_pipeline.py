"""Schedule handler pipeline."""

from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any

from celerity.handlers.param_extractor import resolve_handler_params
from celerity.handlers.resolve import resolve_handler_instance
from celerity.layers.pipeline import run_layer_pipeline
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.consumer import EventResult
from celerity.types.context import ScheduleHandlerContext

if TYPE_CHECKING:
    from celerity.types.handler import ResolvedHandlerBase
    from celerity.types.schedule import ScheduleEventInput

logger = logging.getLogger("celerity.pipeline.schedule")


async def execute_schedule_pipeline(
    handler: ResolvedHandlerBase,
    event: ScheduleEventInput,
    options: dict[str, Any],
) -> EventResult:
    """Execute the schedule handler pipeline.

    Args:
        handler: The resolved schedule handler.
        event: The schedule event input.
        options: Pipeline options including ``container``,
            ``system_layers``, ``module_layers``.

    Returns:
        An ``EventResult`` indicating success or failure.
    """
    container = options.get("container")
    system_layers: list[Any] = options.get("system_layers", [])
    module_layers: list[Any] = options.get("module_layers", [])

    context = ScheduleHandlerContext(
        event=event,
        metadata=HandlerMetadataStore(handler.custom_metadata),
        container=container,  # type: ignore[arg-type]
    )

    all_layers = [*system_layers, *module_layers, *handler.layers]
    logger.debug("tag=%s — %d layers", event.handler_tag, len(all_layers))

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
            all_layers, context, core_handler, handler_type="schedule"
        )
        if isinstance(result, EventResult):
            return result
        return EventResult(success=True)
    except Exception:
        logger.exception("Error in schedule pipeline")
        return EventResult(success=False, error_message="Schedule handler failed")
