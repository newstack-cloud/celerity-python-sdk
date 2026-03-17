"""Schedule parameter injection types.

Used as type annotations on schedule handler method parameters
to declare how values are extracted from schedule events.

Example::

    @schedule_handler("rate(1 day)")
    async def daily_sync(
        self,
        schedule_id: ScheduleId,
        input_data: ScheduleInput[SyncConfig],
    ) -> EventResult:
        ...
"""

from typing import TypeVar

from celerity.decorators.params import ParamMeta, _AnnotatedParam

T = TypeVar("T")


class ScheduleInput(_AnnotatedParam[T]):
    """Extract the schedule input payload.

    The inner type argument is used by the scanner for validation.

    Example::

        @schedule_handler("rate(1 hour)")
        async def process(self, input_data: ScheduleInput[SyncConfig]) -> EventResult: ...
    """

    _meta = ParamMeta(type="scheduleInput")


class ScheduleId:
    """Extract the schedule identifier.

    Example::

        @schedule_handler()
        async def process(self, schedule_id: ScheduleId) -> EventResult: ...
    """

    __celerity_param__ = ParamMeta(type="scheduleId")


class ScheduleExpression:
    """Extract the schedule expression string.

    Example::

        @schedule_handler()
        async def process(self, expression: ScheduleExpression) -> EventResult: ...
    """

    __celerity_param__ = ParamMeta(type="scheduleExpression")


class ScheduleEventInputParam:
    """Extract the full ScheduleEventInput envelope.

    Example::

        @schedule_handler()
        async def process(self, event: ScheduleEventInputParam) -> EventResult: ...
    """

    __celerity_param__ = ParamMeta(type="scheduleEventInput")
