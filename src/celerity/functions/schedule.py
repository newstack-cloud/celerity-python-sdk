"""Function-based schedule handler factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from celerity.types.module import FunctionHandlerDefinition

if TYPE_CHECKING:
    from collections.abc import Callable


def create_schedule_handler(
    *,
    source: str | None = None,
    schedule: str | None = None,
    layers: list[Any] | None = None,
    inject: list[Any] | None = None,
    metadata: dict[str, Any] | None = None,
    handler: Callable[..., Any],
) -> FunctionHandlerDefinition:
    """Create a function-based schedule handler definition.

    Args:
        source: Blueprint resource name hint for the schedule trigger.
        schedule: Schedule expression (e.g. ``"rate(1 day)"``).
        layers: Optional layers to apply.
        inject: Optional DI tokens for injected dependencies.
        metadata: Optional custom metadata.
        handler: The handler function that returns an ``EventResult``.

    Returns:
        A ``FunctionHandlerDefinition`` for registration in a module.

    Example::

        async def daily_cleanup(event, context):
            return EventResult(success=True)

        @module(function_handlers=[
            create_schedule_handler(
                schedule="rate(1 day)",
                handler=daily_cleanup,
            ),
        ])
        class AppModule:
            pass
    """
    handler_tag = source or schedule or ""
    meta: dict[str, Any] = {
        "handler_tag": handler_tag,
        **(metadata or {}),
    }
    if source:
        meta["source"] = source
    if schedule:
        meta["schedule"] = schedule
    if layers:
        meta["layers"] = layers
    if inject:
        meta["inject"] = inject
    return FunctionHandlerDefinition(type="schedule", handler=handler, metadata=meta)
