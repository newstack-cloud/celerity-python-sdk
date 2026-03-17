"""WebSocket parameter injection types.

Used as type annotations on WebSocket handler method parameters
to declare how values are extracted from WebSocket events.

Example::

    @on_message("sendChat")
    async def handle_chat(
        self,
        connection_id: ConnectionId,
        body: MessageBody[ChatMessage],
    ) -> None:
        ...
"""

from typing import TypeVar

from celerity.decorators.params import ParamMeta, _AnnotatedParam

T = TypeVar("T")


class ConnectionId:
    """Extract the WebSocket connection ID.

    Example::

        @on_connect()
        async def connect(self, connection_id: ConnectionId) -> None: ...
    """

    __celerity_param__ = ParamMeta(type="connectionId")


class MessageBody(_AnnotatedParam[T]):
    """Extract the parsed WebSocket message body.

    The inner type argument is used by the scanner for validation.

    Example::

        @on_message("sendChat")
        async def handle(self, body: MessageBody[ChatMessage]) -> None: ...
    """

    _meta = ParamMeta(type="messageBody")


class MessageId:
    """Extract the unique message ID.

    Example::

        @on_message()
        async def handle(self, message_id: MessageId) -> None: ...
    """

    __celerity_param__ = ParamMeta(type="messageId")


class RequestContext:
    """Extract the WebSocket request context from the upgrade request.

    Example::

        @on_connect()
        async def connect(self, ctx: RequestContext) -> None: ...
    """

    __celerity_param__ = ParamMeta(type="requestContext")


class EventType:
    """Extract the WebSocket event type (connect/message/disconnect).

    Example::

        @on_message()
        async def handle(self, event_type: EventType) -> None: ...
    """

    __celerity_param__ = ParamMeta(type="eventType")
