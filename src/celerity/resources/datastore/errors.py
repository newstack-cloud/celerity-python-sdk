"""Datastore error types."""

from __future__ import annotations


class DatastoreError(Exception):
    """Error raised by datastore operations.

    Wraps underlying provider errors with a consistent interface.
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


class ConditionalCheckFailedError(DatastoreError):
    """Raised when a conditional put or delete fails.

    This occurs when the ``ConditionExpression`` evaluates to false,
    e.g., trying to put an item only if it doesn't already exist.
    """
