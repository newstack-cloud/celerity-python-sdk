"""HTTP request and response types."""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class HttpRequest:
    """Incoming HTTP request, normalised across runtime and serverless modes."""

    method: str
    path: str
    path_params: dict[str, str] = field(default_factory=dict)
    query: dict[str, str | list[str]] = field(default_factory=dict)
    headers: dict[str, str | list[str]] = field(default_factory=dict)
    cookies: dict[str, str] = field(default_factory=dict)
    text_body: str | None = None
    binary_body: bytes | None = None
    content_type: str | None = None
    request_id: str = ""
    request_time: str = ""
    auth: dict[str, Any] | None = None
    client_ip: str | None = None
    trace_context: dict[str, str] | None = None
    user_agent: str | None = None
    matched_route: str | None = None


@dataclass
class HttpResponse:
    """Outgoing HTTP response returned by handlers."""

    status: int
    headers: dict[str, str] | None = None
    body: str | None = None
    binary_body: bytes | None = None


HandlerResponse = HttpResponse
"""Convenience alias for HttpResponse."""
