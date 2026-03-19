"""Function-based HTTP handler factories."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from celerity.types.module import FunctionHandlerDefinition

if TYPE_CHECKING:
    from collections.abc import Callable


def create_http_handler(
    *,
    path: str = "/",
    method: str = "GET",
    layers: list[Any] | None = None,
    inject: list[Any] | None = None,
    metadata: dict[str, Any] | None = None,
    handler: Callable[..., Any],
) -> FunctionHandlerDefinition:
    """Create a function-based HTTP handler definition.

    Args:
        path: The route path pattern.
        method: The HTTP method.
        layers: Optional layers to apply.
        inject: Optional DI tokens for injected dependencies.
        metadata: Optional custom metadata.
        handler: The handler function.

    Returns:
        A ``FunctionHandlerDefinition`` for registration in a module.

    Example::

        async def get_health(request, context):
            return HttpResponse(status=200, body='{"status": "ok"}')

        @module(function_handlers=[
            create_http_handler(path="/health", method="GET", handler=get_health),
        ])
        class AppModule:
            pass
    """
    meta = {
        "path": path,
        "method": method,
        **(metadata or {}),
    }
    if layers:
        meta["layers"] = layers
    if inject:
        meta["inject"] = inject
    return FunctionHandlerDefinition(type="http", handler=handler, metadata=meta)


def http_get(path: str, handler: Callable[..., Any]) -> FunctionHandlerDefinition:
    """Shorthand for ``create_http_handler(path=path, method="GET", handler=handler)``."""
    return create_http_handler(path=path, method="GET", handler=handler)


def http_post(path: str, handler: Callable[..., Any]) -> FunctionHandlerDefinition:
    """Shorthand for ``create_http_handler(path=path, method="POST", handler=handler)``."""
    return create_http_handler(path=path, method="POST", handler=handler)


def http_put(path: str, handler: Callable[..., Any]) -> FunctionHandlerDefinition:
    """Shorthand for ``create_http_handler(path=path, method="PUT", handler=handler)``."""
    return create_http_handler(path=path, method="PUT", handler=handler)


def http_patch(path: str, handler: Callable[..., Any]) -> FunctionHandlerDefinition:
    """Shorthand for ``create_http_handler(path=path, method="PATCH", handler=handler)``."""
    return create_http_handler(path=path, method="PATCH", handler=handler)


def http_delete(path: str, handler: Callable[..., Any]) -> FunctionHandlerDefinition:
    """Shorthand for ``create_http_handler(path=path, method="DELETE", handler=handler)``."""
    return create_http_handler(path=path, method="DELETE", handler=handler)
