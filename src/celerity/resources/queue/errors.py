"""Queue error types."""

from __future__ import annotations


class QueueError(Exception):
    """Error raised by queue operations.

    Wraps provider-specific errors (SQS ClientError, Redis errors)
    with a consistent interface.
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
