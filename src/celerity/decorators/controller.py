"""Controller class decorator."""

from collections.abc import Callable

from celerity.metadata.keys import CONTROLLER, INJECTABLE, set_metadata


def controller(prefix: str | None = None) -> Callable[[type], type]:
    """Mark a class as a controller with an optional route prefix.

    A controller groups related HTTP handler methods under a common
    path prefix and registers the class for dependency injection.

    Args:
        prefix: Route prefix applied to all handler methods in the class.
            When ``None``, no prefix is added and handlers use their own
            paths directly.

    Returns:
        A class decorator that attaches controller metadata.

    Example::

        @controller("/orders")
        class OrderController:
            @get("/")
            async def list_orders(self) -> HandlerResponse: ...

            @get("/{order_id}")
            async def get_order(self, order_id: Param[str]) -> HandlerResponse: ...

        @controller()
        class HealthController:
            @get("/health")
            async def health(self) -> HandlerResponse: ...
    """

    def decorator(cls: type) -> type:
        meta: dict[str, str] = {}
        if prefix is not None:
            meta["prefix"] = prefix
        set_metadata(cls, CONTROLLER, meta)
        set_metadata(cls, INJECTABLE, True)
        return cls

    return decorator
