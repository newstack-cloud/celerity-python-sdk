"""Schedule handler decorator."""

from collections.abc import Callable
from typing import TypeVar

from celerity.metadata.keys import _META_ATTR, SCHEDULE_HANDLER

_FuncT = TypeVar("_FuncT", bound=Callable[..., object])


def _is_schedule_expression(value: str) -> bool:
    return value.startswith("rate(") or value.startswith("cron(")


def schedule_handler(
    source_or_expression: str | None = None,
    *,
    source: str | None = None,
    schedule: str | None = None,
) -> Callable[[_FuncT], _FuncT]:
    """Mark a method as a schedule handler.

    Cross-cutting: works on any controller type. The method should return
    an ``EventResult`` to report success/failure.

    Can be called with no arguments (fully blueprint-driven), a single
    positional string, or explicit keyword arguments:

    - No args: fully blueprint-driven, no annotations.
    - String starting with ``rate(`` or ``cron(``: schedule expression.
    - Other string: ``source`` blueprint resource name hint.
    - Keyword args: explicit ``source`` and/or ``schedule``.

    Args:
        source_or_expression: A source name or schedule expression string.
        source: Blueprint resource name hint for the deploy engine.
        schedule: Schedule expression (e.g. ``"rate(1 day)"``).

    Examples::

        @schedule_handler()
        async def cleanup(self) -> EventResult: ...

        @schedule_handler("rate(1 day)")
        async def daily_sync(self) -> EventResult: ...

        @schedule_handler("daily-cleanup")
        async def cleanup(self) -> EventResult: ...

        @schedule_handler(source="weekly-report", schedule="cron(0 9 ? * MON *)")
        async def report(self) -> EventResult: ...
    """
    meta: dict[str, str] = {}
    if source_or_expression is not None:
        if _is_schedule_expression(source_or_expression):
            meta["schedule"] = source_or_expression
        else:
            meta["source"] = source_or_expression
    if source is not None:
        meta["source"] = source
    if schedule is not None:
        meta["schedule"] = schedule

    def decorator(fn: _FuncT) -> _FuncT:
        if not hasattr(fn, _META_ATTR):
            setattr(fn, _META_ATTR, {})
        getattr(fn, _META_ATTR)[SCHEDULE_HANDLER] = meta
        return fn

    return decorator
