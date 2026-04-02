"""Async polling utility for eventual consistency."""

from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


async def wait_for(
    predicate: Callable[[], Any],
    *,
    timeout: float = 10.0,
    interval: float = 0.5,
) -> None:
    """Poll a predicate until it returns a truthy value or timeout expires.

    The predicate can be sync or async.

    Args:
        predicate: Function that returns truthy when the condition is met.
        timeout: Maximum wait time in seconds.
        interval: Poll interval in seconds.

    Raises:
        TimeoutError: If the predicate does not return truthy before timeout.
    """
    deadline = asyncio.get_event_loop().time() + timeout

    while asyncio.get_event_loop().time() < deadline:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        await asyncio.sleep(interval)

    raise TimeoutError(f"wait_for timed out after {timeout}s")
