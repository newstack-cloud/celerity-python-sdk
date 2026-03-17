"""Resource declaration decorator."""

from collections.abc import Callable
from typing import Any

from celerity.metadata.keys import _META_ATTR, USE_RESOURCE, get_metadata, set_metadata


def use_resource(*resource_names: str) -> Callable[[Any], Any]:
    """Declare that a handler uses infrastructure resources.

    Resource declarations inform the deploy engine which blueprint
    resources a controller or handler method depends on. Multiple
    ``@use_resource`` decorators stack -- their resource lists are
    concatenated.

    Args:
        resource_names: Resource identifiers referenced in the blueprint.

    Returns:
        A decorator applicable to a class or method.

    Example::

        @controller("/orders")
        @use_resource("orders-table", "orders-queue")
        class OrderController:
            @get("/{order_id}")
            @use_resource("orders-cache")
            async def get_order(self, order_id: Param[str]) -> HandlerResponse: ...
    """

    def decorator(target: Any) -> Any:
        if isinstance(target, type):
            existing: list[str] = get_metadata(target, USE_RESOURCE) or []
            set_metadata(target, USE_RESOURCE, [*existing, *resource_names])
        else:
            if not hasattr(target, _META_ATTR):
                setattr(target, _META_ATTR, {})
            meta: dict[str, Any] = getattr(target, _META_ATTR)
            existing = meta.get(USE_RESOURCE, [])
            meta[USE_RESOURCE] = [*existing, *resource_names]
        return target

    return decorator
