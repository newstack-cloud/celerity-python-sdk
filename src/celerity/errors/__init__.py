"""HTTP errors."""

from celerity.errors.http_exception import (
    BadGatewayError,
    BadRequestError,
    ConflictError,
    ForbiddenError,
    GatewayTimeoutError,
    HttpError,
    HttpNotImplementedError,
    InternalServerError,
    MethodNotAllowedError,
    NotFoundError,
    ServiceUnavailableError,
    TooManyRequestsError,
    UnauthorizedError,
    UnprocessableEntityError,
)

__all__ = [
    "BadGatewayError",
    "BadRequestError",
    "ConflictError",
    "ForbiddenError",
    "GatewayTimeoutError",
    "HttpError",
    "HttpNotImplementedError",
    "InternalServerError",
    "MethodNotAllowedError",
    "NotFoundError",
    "ServiceUnavailableError",
    "TooManyRequestsError",
    "UnauthorizedError",
    "UnprocessableEntityError",
]
