"""Middleware layer ABC."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from celerity.types.context import BaseHandlerContext


class CelerityLayer(ABC):
    """Middleware layer that wraps handler execution.

    Layers form a composable pipeline: each layer receives a context and
    a next_handler callback, and must call ``await next_handler(context)``
    to continue the chain.
    """

    def supports(self, handler_type: str) -> bool:
        """Return whether this layer applies to the given handler type.

        Override to restrict a layer to specific handler types.
        Default returns True (applies to all).
        """
        return True

    @abstractmethod
    async def handle(
        self,
        context: BaseHandlerContext,
        next_handler: Callable[..., Awaitable[Any]],
    ) -> Any:
        """Execute layer logic, calling next_handler to continue the chain."""
        ...

    async def dispose(self) -> None:  # noqa: B027
        """Clean up resources when the layer is no longer needed."""
