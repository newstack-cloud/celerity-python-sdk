"""Lazy handler instance resolution.

Handlers are scanned at bootstrap time without resolving their class
instances. The instance is resolved from the DI container on first
invocation, then cached on the handler for subsequent calls.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celerity.types.container import ServiceContainer
    from celerity.types.handler import ResolvedHandlerBase


async def resolve_handler_instance(
    handler: ResolvedHandlerBase,
    container: ServiceContainer,
) -> object | None:
    """Lazily resolve a handler's class instance from the DI container.

    On first call, resolves the instance and caches it on the handler.
    Subsequent calls return the cached instance. For function handlers
    (no controller class), returns ``None``.

    After resolution, ``handler.handler_fn`` is rebound as a method on
    the resolved instance so that ``self`` is available in the handler.

    Args:
        handler: The resolved handler (instance may be ``None``).
        container: The DI container.

    Returns:
        The resolved instance, or ``None`` for function handlers.
    """
    if handler.handler_instance is not None:
        return handler.handler_instance

    if handler.controller_class is None:
        return None

    instance: object = await container.resolve(handler.controller_class)
    handler.handler_instance = instance

    # Rebind the unbound function as a method on the instance.
    fn_name = handler.handler_fn.__name__
    handler.handler_fn = getattr(instance, fn_name)

    return instance
