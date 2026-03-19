"""Function-based consumer handler factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from celerity.types.module import FunctionHandlerDefinition

if TYPE_CHECKING:
    from collections.abc import Callable


def create_consumer_handler(
    *,
    handler_tag: str = "",
    route: str | None = None,
    layers: list[Any] | None = None,
    inject: list[Any] | None = None,
    metadata: dict[str, Any] | None = None,
    handler: Callable[..., Any],
) -> FunctionHandlerDefinition:
    """Create a function-based consumer handler definition.

    Args:
        handler_tag: The handler tag for routing consumer events.
        route: Optional routing key for message filtering.
        layers: Optional layers to apply.
        inject: Optional DI tokens for injected dependencies.
        metadata: Optional custom metadata.
        handler: The handler function that receives consumer events
            and returns an ``EventResult``.

    Returns:
        A ``FunctionHandlerDefinition`` for registration in a module.

    Example::

        async def process_orders(event, context):
            return EventResult(success=True)

        @module(function_handlers=[
            create_consumer_handler(
                handler_tag="orders-queue",
                handler=process_orders,
            ),
        ])
        class AppModule:
            pass
    """
    meta: dict[str, Any] = {
        "handler_tag": handler_tag,
        **(metadata or {}),
    }
    if route:
        meta["route"] = route
    if layers:
        meta["layers"] = layers
    if inject:
        meta["inject"] = inject
    return FunctionHandlerDefinition(type="consumer", handler=handler, metadata=meta)
