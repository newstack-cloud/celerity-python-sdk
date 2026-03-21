"""SQL database error types."""

from __future__ import annotations


class SqlDatabaseError(Exception):
    """Error raised by SQL database operations.

    Wraps underlying driver errors with a consistent interface.
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
