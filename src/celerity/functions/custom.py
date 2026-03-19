"""Function-based custom (invoke) handler factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from celerity.types.module import FunctionHandlerDefinition

if TYPE_CHECKING:
    from collections.abc import Callable


def create_custom_handler(
    *,
    name: str,
    layers: list[Any] | None = None,
    inject: list[Any] | None = None,
    metadata: dict[str, Any] | None = None,
    handler: Callable[..., Any],
) -> FunctionHandlerDefinition:
    """Create a function-based custom (invocable) handler definition.

    Args:
        name: The handler name used for programmatic invocation.
        layers: Optional layers to apply.
        inject: Optional DI tokens for injected dependencies.
        metadata: Optional custom metadata.
        handler: The handler function that receives a payload and
            returns an arbitrary result.

    Returns:
        A ``FunctionHandlerDefinition`` for registration in a module.

    Example::

        async def process_report(payload, context):
            return {"processed": True}

        @module(function_handlers=[
            create_custom_handler(name="processReport", handler=process_report),
        ])
        class AppModule:
            pass
    """
    meta: dict[str, Any] = {
        "name": name,
        **(metadata or {}),
    }
    if layers:
        meta["layers"] = layers
    if inject:
        meta["inject"] = inject
    return FunctionHandlerDefinition(type="custom", handler=handler, metadata=meta)
