"""Invoke (custom handler) parameter injection types.

Used as type annotations on invocable handler method parameters
to declare how values are extracted from invocation events.

Example::

    @invoke("processReport")
    async def process_report(
        self,
        payload: Payload[ReportInput],
        ctx: InvokeContext,
    ) -> dict:
        ...
"""

from typing import TypeVar

from celerity.decorators.params import ParamMeta, _AnnotatedParam

T = TypeVar("T")


class Payload(_AnnotatedParam[T]):
    """Extract the invocation payload.

    The inner type argument is used by the scanner for validation.

    Example::

        @invoke("processReport")
        async def process(self, payload: Payload[ReportInput]) -> dict: ...
    """

    _meta = ParamMeta(type="payload")


class InvokeContext:
    """Extract the BaseHandlerContext for a custom handler.

    Example::

        @invoke("processReport")
        async def process(self, ctx: InvokeContext) -> dict: ...
    """

    __celerity_param__ = ParamMeta(type="invokeContext")
