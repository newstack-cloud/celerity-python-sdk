"""Custom metadata decorators."""

from collections.abc import Callable
from typing import Any

from celerity.metadata.keys import _META_ATTR, CUSTOM_METADATA, get_metadata, set_metadata


def set_handler_metadata(key: str, value: Any) -> Callable[[Any], Any]:
    """Attach custom key-value metadata to a class or method.

    Custom metadata is available at runtime through the execution
    context and can be used by guards, layers, or application logic
    to make decisions based on handler annotations.

    Args:
        key: The metadata key.
        value: The metadata value.

    Returns:
        A decorator applicable to a class or method.

    Example::

        @controller("/orders")
        class OrderController:
            @post("/")
            @set_handler_metadata("audit", True)
            async def create_order(self, body: Body[CreateOrderInput]) -> HandlerResponse: ...
    """

    def decorator(target: Any) -> Any:
        if isinstance(target, type):
            existing: dict[str, Any] = get_metadata(target, CUSTOM_METADATA) or {}
            set_metadata(target, CUSTOM_METADATA, {**existing, key: value})
        else:
            if not hasattr(target, _META_ATTR):
                setattr(target, _META_ATTR, {})
            meta: dict[str, Any] = getattr(target, _META_ATTR)
            existing = meta.get(CUSTOM_METADATA, {})
            meta[CUSTOM_METADATA] = {**existing, key: value}
        return target

    return decorator


def action(value: Any) -> Callable[[Any], Any]:
    """Shorthand for ``@set_handler_metadata("action", value)``.

    Args:
        value: The action identifier.

    Returns:
        A decorator applicable to a class or method.

    Example::

        @post("/orders")
        @action("create_order")
        async def create_order(self, body: Body[CreateOrderInput]) -> HandlerResponse: ...
    """
    return set_handler_metadata("action", value)
