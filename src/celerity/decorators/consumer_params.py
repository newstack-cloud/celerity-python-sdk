"""Consumer parameter injection types.

Used as type annotations on consumer handler method parameters
to declare how values are extracted from consumer events.

Example::

    @message_handler()
    async def handle(
        self,
        messages: Messages[OrderEvent],
        event: ConsumerEvent,
    ) -> EventResult:
        ...
"""

from typing import TypeVar

from celerity.decorators.params import ParamMeta, _AnnotatedParam

T = TypeVar("T")


class Messages(_AnnotatedParam[T]):
    """Extract the message batch (raw or validated).

    The inner type argument is used by the scanner for validation.

    Example::

        @message_handler()
        async def handle(self, messages: Messages[OrderEvent]) -> EventResult: ...
    """

    _meta = ParamMeta(type="messages")


class ConsumerEvent:
    """Extract the full ConsumerEventInput envelope.

    Example::

        @message_handler()
        async def handle(self, event: ConsumerEvent) -> EventResult: ...
    """

    __celerity_param__ = ParamMeta(type="consumerEvent")


class ConsumerVendor:
    """Extract vendor-specific metadata.

    Example::

        @message_handler()
        async def handle(self, vendor: ConsumerVendor) -> EventResult: ...
    """

    __celerity_param__ = ParamMeta(type="consumerVendor")


class ConsumerTraceContext:
    """Extract the consumer trace context.

    Example::

        @message_handler()
        async def handle(self, trace: ConsumerTraceContext) -> EventResult: ...
    """

    __celerity_param__ = ParamMeta(type="consumerTraceContext")
