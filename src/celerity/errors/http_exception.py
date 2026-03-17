"""HTTP errors with status codes."""

from typing import Any


class HttpError(Exception):
    """Base HTTP error carrying a status code and optional details."""

    def __init__(
        self,
        message: str,
        status_code: int,
        details: Any = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.details = details


class BadRequestError(HttpError):
    def __init__(self, message: str = "Bad Request", details: Any = None) -> None:
        super().__init__(message, 400, details)


class UnauthorizedError(HttpError):
    def __init__(self, message: str = "Unauthorized", details: Any = None) -> None:
        super().__init__(message, 401, details)


class ForbiddenError(HttpError):
    def __init__(self, message: str = "Forbidden", details: Any = None) -> None:
        super().__init__(message, 403, details)


class NotFoundError(HttpError):
    def __init__(self, message: str = "Not Found", details: Any = None) -> None:
        super().__init__(message, 404, details)


class MethodNotAllowedError(HttpError):
    def __init__(self, message: str = "Method Not Allowed", details: Any = None) -> None:
        super().__init__(message, 405, details)


class ConflictError(HttpError):
    def __init__(self, message: str = "Conflict", details: Any = None) -> None:
        super().__init__(message, 409, details)


class UnprocessableEntityError(HttpError):
    def __init__(self, message: str = "Unprocessable Entity", details: Any = None) -> None:
        super().__init__(message, 422, details)


class TooManyRequestsError(HttpError):
    def __init__(self, message: str = "Too Many Requests", details: Any = None) -> None:
        super().__init__(message, 429, details)


class InternalServerError(HttpError):
    def __init__(self, message: str = "Internal Server Error", details: Any = None) -> None:
        super().__init__(message, 500, details)


class HttpNotImplementedError(HttpError):
    def __init__(self, message: str = "Not Implemented", details: Any = None) -> None:
        super().__init__(message, 501, details)


class BadGatewayError(HttpError):
    def __init__(self, message: str = "Bad Gateway", details: Any = None) -> None:
        super().__init__(message, 502, details)


class ServiceUnavailableError(HttpError):
    def __init__(self, message: str = "Service Unavailable", details: Any = None) -> None:
        super().__init__(message, 503, details)


class GatewayTimeoutError(HttpError):
    def __init__(self, message: str = "Gateway Timeout", details: Any = None) -> None:
        super().__init__(message, 504, details)
