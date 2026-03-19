"""Function-based WebSocket handler factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from celerity.types.module import FunctionHandlerDefinition

if TYPE_CHECKING:
    from collections.abc import Callable


def create_websocket_handler(
    *,
    route: str = "$default",
    protected_by: list[str] | None = None,
    layers: list[Any] | None = None,
    inject: list[Any] | None = None,
    metadata: dict[str, Any] | None = None,
    handler: Callable[..., Any],
) -> FunctionHandlerDefinition:
    """Create a function-based WebSocket handler definition.

    Args:
        route: The route key for message matching.
        protected_by: Guard names protecting this handler.
        layers: Optional layers to apply.
        inject: Optional DI tokens for injected dependencies.
        metadata: Optional custom metadata.
        handler: The handler function.

    Returns:
        A ``FunctionHandlerDefinition`` for registration in a module.

    Example::

        async def on_chat(message, context):
            ...

        @module(function_handlers=[
            create_websocket_handler(route="chat", handler=on_chat),
        ])
        class AppModule:
            pass
    """
    meta: dict[str, Any] = {
        "route": route,
        **(metadata or {}),
    }
    if protected_by:
        meta["protected_by"] = protected_by
    if layers:
        meta["layers"] = layers
    if inject:
        meta["inject"] = inject
    return FunctionHandlerDefinition(type="websocket", handler=handler, metadata=meta)
