"""Asserting HTTP client for API tests."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Generator

import httpx


@dataclass
class TestResponse:
    """Response from a test HTTP request."""

    status: int
    headers: httpx.Headers
    body: Any
    text: str


_SENTINEL = object()


class TestRequest:
    """Chainable HTTP request builder with inline assertions."""

    def __init__(self, client: httpx.AsyncClient, method: str, path: str) -> None:
        self._client = client
        self._method = method
        self._path = path
        self._headers: dict[str, str] = {}
        self._body: Any = None
        self._expectations: list[dict[str, Any]] = []

    def auth(self, token: str) -> TestRequest:
        """Set bearer token authorization."""
        self._headers["Authorization"] = f"Bearer {token}"
        return self

    def set(self, key: str, value: str) -> TestRequest:
        """Set a request header."""
        self._headers[key] = value
        return self

    def send(self, body: Any) -> TestRequest:
        """Set the request JSON body."""
        self._body = body
        return self

    def expect(self, first: Any, second: Any = _SENTINEL) -> TestRequest:
        """Add an assertion.

        Overloaded:
        - ``expect(200)`` — assert status code
        - ``expect({"key": "val"})`` — assert body equals
        - ``expect(lambda body: ...)`` — assert with function
        - ``expect("content-type", "application/json")`` — assert header
        - ``expect("content-type", re.compile(r"json"))`` — assert header regex
        """
        if second is not _SENTINEL:
            self._expectations.append({"type": "header", "key": first, "value": second})
        elif isinstance(first, int):
            self._expectations.append({"type": "status", "value": first})
        elif callable(first):
            self._expectations.append({"type": "body_fn", "value": first})
        else:
            self._expectations.append({"type": "body", "value": first})
        return self

    async def end(self) -> TestResponse:
        """Execute the request and run all assertions."""
        kwargs: dict[str, Any] = {"headers": self._headers}
        if self._body is not None:
            kwargs["json"] = self._body

        response = await self._client.request(self._method, self._path, **kwargs)
        text = response.text
        try:
            body = response.json()
        except Exception:
            body = text

        result = TestResponse(
            status=response.status_code,
            headers=response.headers,
            body=body,
            text=text,
        )

        for exp in self._expectations:
            match exp["type"]:
                case "status":
                    assert result.status == exp["value"], (
                        f"Expected status {exp['value']} but got {result.status}.\nBody: {text}"
                    )
                case "body":
                    expected = json.dumps(exp["value"], sort_keys=True)
                    actual = json.dumps(result.body, sort_keys=True)
                    assert expected == actual, f"Expected body {expected} but got {actual}"
                case "body_fn":
                    exp["value"](result.body)
                case "header":
                    actual = result.headers.get(exp["key"])
                    value = exp["value"]
                    if isinstance(value, re.Pattern):
                        assert actual and value.search(actual), (
                            f"Expected header {exp['key']!r} to match {value} but got {actual!r}"
                        )
                    else:
                        assert actual == value, (
                            f"Expected header {exp['key']!r} to be {value!r} but got {actual!r}"
                        )

        return result

    def __await__(self) -> Generator[Any, None, TestResponse]:
        return self.end().__await__()


class TestHttpClient:
    """Asserting HTTP client for API tests."""

    def __init__(self, base_url: str) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=30.0)

    def get(self, path: str) -> TestRequest:
        return TestRequest(self._client, "GET", path)

    def post(self, path: str) -> TestRequest:
        return TestRequest(self._client, "POST", path)

    def put(self, path: str) -> TestRequest:
        return TestRequest(self._client, "PUT", path)

    def patch(self, path: str) -> TestRequest:
        return TestRequest(self._client, "PATCH", path)

    def delete(self, path: str) -> TestRequest:
        return TestRequest(self._client, "DELETE", path)

    async def close(self) -> None:
        await self._client.aclose()


def create_test_client(base_url: str | None = None) -> TestHttpClient:
    """Create a test HTTP client for API tests.

    Reads ``CELERITY_TEST_BASE_URL`` env var (default: ``http://localhost:8081``).
    """
    url = (
        base_url
        if base_url is not None
        else os.environ.get("CELERITY_TEST_BASE_URL", "http://localhost:8081")
    )
    return TestHttpClient(url)
