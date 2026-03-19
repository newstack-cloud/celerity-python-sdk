"""Cache error types."""

from __future__ import annotations


class CacheError(Exception):
    """Error raised by cache operations.

    Wraps underlying Redis errors with a consistent interface.
    """

    def __init__(self, message: str, *, cause: Exception | None = None) -> None:
        super().__init__(message)
        self.__cause__ = cause
