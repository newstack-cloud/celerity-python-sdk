"""Layer decorator."""

from __future__ import annotations

from typing import Any

from celerity.metadata.keys import _META_ATTR, LAYER, get_metadata, set_metadata


def use_layer(*layers: Any) -> Any:
    """Apply middleware layers to a controller class or handler method.

    Layers run in the order they are declared. When applied at the
    class level, every handler in the controller inherits the layers.
    Method-level layers run after class-level layers.

    Multiple ``@use_layer`` decorators stack -- their layer lists are
    concatenated.

    Args:
        layers: Layer classes or instances to apply.

    Returns:
        A decorator applicable to a class or method.

    Example::

        @controller("/orders")
        @use_layer(LoggingLayer, MetricsLayer)
        class OrderController:
            @get("/")
            @use_layer(CacheLayer)
            async def list_orders(self) -> HandlerResponse: ...
    """

    def decorator(target: Any) -> Any:
        if isinstance(target, type):
            existing: list[Any] = get_metadata(target, LAYER) or []
            set_metadata(target, LAYER, [*existing, *layers])
        else:
            if not hasattr(target, _META_ATTR):
                setattr(target, _META_ATTR, {})
            meta: dict[str, Any] = getattr(target, _META_ATTR)
            existing = meta.get(LAYER, [])
            meta[LAYER] = [*existing, *layers]
        return target

    return decorator


def use_layers(layers: list[Any]) -> Any:
    """Apply a list of middleware layers to a controller class or handler method.

    Convenience wrapper around ``@use_layer`` for when layers are already
    in a list.

    Args:
        layers: A list of layer classes or instances to apply.

    Returns:
        A decorator applicable to a class or method.

    Example::

        my_layers = [LoggingLayer, MetricsLayer, CacheLayer]

        @controller("/orders")
        @use_layers(my_layers)
        class OrderController:
            @get("/")
            async def list_orders(self) -> HandlerResponse: ...
    """
    return use_layer(*layers)
