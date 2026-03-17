"""Tests for celerity.errors."""

import pytest

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


class TestHttpError:
    def test_base_error(self) -> None:
        exc = HttpError("Teapot", 418)
        assert exc.message == "Teapot"
        assert exc.status_code == 418
        assert exc.details is None
        assert str(exc) == "Teapot"

    def test_base_error_with_details(self) -> None:
        exc = HttpError("Bad", 400, details={"field": "name"})
        assert exc.details == {"field": "name"}

    def test_is_exception(self) -> None:
        exc = HttpError("err", 500)
        assert isinstance(exc, Exception)

    def test_can_raise_and_catch(self) -> None:
        with pytest.raises(HttpError) as exc_info:
            raise HttpError("boom", 500)
        assert exc_info.value.status_code == 500


@pytest.mark.parametrize(
    ("exc_class", "expected_status", "expected_message"),
    [
        (BadRequestError, 400, "Bad Request"),
        (UnauthorizedError, 401, "Unauthorized"),
        (ForbiddenError, 403, "Forbidden"),
        (NotFoundError, 404, "Not Found"),
        (MethodNotAllowedError, 405, "Method Not Allowed"),
        (ConflictError, 409, "Conflict"),
        (UnprocessableEntityError, 422, "Unprocessable Entity"),
        (TooManyRequestsError, 429, "Too Many Requests"),
        (InternalServerError, 500, "Internal Server Error"),
        (HttpNotImplementedError, 501, "Not Implemented"),
        (BadGatewayError, 502, "Bad Gateway"),
        (ServiceUnavailableError, 503, "Service Unavailable"),
        (GatewayTimeoutError, 504, "Gateway Timeout"),
    ],
)
def test_error_defaults(
    exc_class: type[HttpError],
    expected_status: int,
    expected_message: str,
) -> None:
    exc = exc_class()  # type: ignore[call-arg]
    assert exc.status_code == expected_status
    assert exc.message == expected_message
    assert isinstance(exc, HttpError)


class TestErrorCustomMessage:
    def test_not_found_custom_message(self) -> None:
        exc = NotFoundError("Order not found")
        assert exc.message == "Order not found"
        assert exc.status_code == 404

    def test_bad_request_with_details(self) -> None:
        exc = BadRequestError("Invalid input", details={"field": "email"})
        assert exc.details == {"field": "email"}
