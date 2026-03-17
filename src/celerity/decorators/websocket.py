"""WebSocket class and method decorators."""

from collections.abc import Callable
from typing import TypeVar

from celerity.metadata.keys import (
    _META_ATTR,
    INJECTABLE,
    WEBSOCKET_CONTROLLER,
    WEBSOCKET_EVENT,
    set_metadata,
)

_FuncT = TypeVar("_FuncT", bound=Callable[..., object])


def ws_controller() -> Callable[[type], type]:
    """Mark a class as a WebSocket handler controller.

    A WebSocket controller groups ``@on_connect``, ``@on_message``,
    and ``@on_disconnect`` handlers in a single class.

    Returns:
        A class decorator that registers the WebSocket controller.

    Example::

        @ws_controller()
        class ChatController:
            @on_connect()
            async def connect(self, connection_id: ConnectionId) -> None: ...

            @on_message()
            async def handle(self, body: MessageBody[ChatMessage]) -> None: ...

            @on_disconnect()
            async def disconnect(self, connection_id: ConnectionId) -> None: ...
    """

    def decorator(cls: type) -> type:
        set_metadata(cls, WEBSOCKET_CONTROLLER, True)
        set_metadata(cls, INJECTABLE, True)
        return cls

    return decorator


def on_connect() -> Callable[[_FuncT], _FuncT]:
    """Mark a method as the WebSocket connect handler.

    Called when a client initiates a WebSocket connection. The route
    is automatically set to ``"$connect"``.

    Returns:
        A method decorator.

    Example::

        @on_connect()
        async def connect(self, connection_id: ConnectionId) -> None:
            await self.sessions.register(connection_id)
    """

    def decorator(fn: _FuncT) -> _FuncT:
        if not hasattr(fn, _META_ATTR):
            setattr(fn, _META_ATTR, {})
        getattr(fn, _META_ATTR)[WEBSOCKET_EVENT] = {
            "route": "$connect",
            "event_type": "connect",
        }
        return fn

    return decorator


def on_message(route: str = "$default") -> Callable[[_FuncT], _FuncT]:
    """Mark a method as a WebSocket message handler for a specific route.

    Args:
        route: The route key to match incoming messages against.
            Defaults to ``"$default"``, which catches all messages
            that do not match a more specific route.

    Returns:
        A method decorator.

    Example::

        @on_message()
        async def handle_default(self, body: MessageBody[dict]) -> None:
            ...

        @on_message("sendChat")
        async def handle_chat(self, body: MessageBody[ChatMessage]) -> None:
            ...
    """

    def decorator(fn: _FuncT) -> _FuncT:
        if not hasattr(fn, _META_ATTR):
            setattr(fn, _META_ATTR, {})
        getattr(fn, _META_ATTR)[WEBSOCKET_EVENT] = {
            "route": route,
            "event_type": "message",
        }
        return fn

    return decorator


def on_disconnect() -> Callable[[_FuncT], _FuncT]:
    """Mark a method as the WebSocket disconnect handler.

    Called when a client disconnects. The route is automatically
    set to ``"$disconnect"``.

    Returns:
        A method decorator.

    Example::

        @on_disconnect()
        async def disconnect(self, connection_id: ConnectionId) -> None:
            await self.sessions.remove(connection_id)
    """

    def decorator(fn: _FuncT) -> _FuncT:
        if not hasattr(fn, _META_ATTR):
            setattr(fn, _META_ATTR, {})
        getattr(fn, _META_ATTR)[WEBSOCKET_EVENT] = {
            "route": "$disconnect",
            "event_type": "disconnect",
        }
        return fn

    return decorator
