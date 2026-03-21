"""Topic error types."""

from __future__ import annotations


class TopicError(Exception):
    """Error raised by topic operations.

    Wraps provider-specific errors (SNS ClientError, Redis errors)
    into a consistent error type.
    """

    def __init__(
        self,
        message: str,
        *,
        resource: str | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.resource = resource
        self.__cause__ = cause
