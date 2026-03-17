"""Consumer class and method decorators."""

from collections.abc import Callable
from typing import TypeVar

from celerity.metadata.keys import (
    _META_ATTR,
    CONSUMER,
    CONSUMER_HANDLER,
    INJECTABLE,
    set_metadata,
)


def consumer(source: str | None = None) -> Callable[[type], type]:
    """Mark a class as a consumer handler for a message source.

    When ``source`` is provided, it is stored as a blueprint resource
    name hint for the deploy engine. When omitted, the consumer
    metadata is empty and the source binding is fully blueprint-driven.

    Args:
        source: The resource identifier of the message source.
            When ``None``, no source hint is stored.

    Returns:
        A class decorator that registers the consumer.

    Example::

        @consumer("orders-queue")
        class OrderConsumer:
            @message_handler()
            async def handle(self, messages: Messages[OrderEvent]) -> EventResult: ...

        @consumer()
        class GenericConsumer:
            @message_handler()
            async def handle(self, messages: Messages[dict]) -> EventResult: ...
    """

    def decorator(cls: type) -> type:
        meta: dict[str, str] = {}
        if source is not None:
            meta["source"] = source
        set_metadata(cls, CONSUMER, meta)
        set_metadata(cls, INJECTABLE, True)
        return cls

    return decorator


_FuncT = TypeVar("_FuncT", bound=Callable[..., object])


def message_handler(route: str | None = None) -> Callable[[_FuncT], _FuncT]:
    """Mark a method as the message processing handler.

    Each consumer class should have exactly one ``@message_handler``
    method. An optional route can be used for message filtering.

    Args:
        route: Optional routing key for message filtering.

    Returns:
        A method decorator.

    Example::

        @message_handler()
        async def handle(self, messages: Messages[OrderEvent]) -> EventResult: ...

        @message_handler("order.created")
        async def on_created(self, messages: Messages[OrderCreated]) -> EventResult: ...
    """

    def decorator(fn: _FuncT) -> _FuncT:
        if not hasattr(fn, _META_ATTR):
            setattr(fn, _META_ATTR, {})
        meta: dict[str, str] = {}
        if route is not None:
            meta["route"] = route
        getattr(fn, _META_ATTR)[CONSUMER_HANDLER] = meta
        return fn

    return decorator
