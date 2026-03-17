"""Invoke (custom handler) decorator."""

from collections.abc import Callable
from typing import TypeVar

from celerity.metadata.keys import _META_ATTR, INVOKE

_FuncT = TypeVar("_FuncT", bound=Callable[..., object])


def invoke(name: str) -> Callable[[_FuncT], _FuncT]:
    """Mark a method as programmatically invocable.

    An invocable handler can be called directly by name from other
    services or from the runtime itself, independent of HTTP or
    event triggers.

    Args:
        name: The handler name used for invocation.

    Returns:
        A method decorator.

    Example::

        @invoke("processReport")
        async def process_report(self, payload: Payload[ReportInput]) -> dict: ...
    """

    def decorator(fn: _FuncT) -> _FuncT:
        if not hasattr(fn, _META_ATTR):
            setattr(fn, _META_ATTR, {})
        getattr(fn, _META_ATTR)[INVOKE] = {"name": name}
        return fn

    return decorator
