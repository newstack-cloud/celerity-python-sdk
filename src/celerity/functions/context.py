"""Handler context construction helpers."""

from __future__ import annotations

from typing import Any

from celerity.metadata.store import HandlerMetadataStore
from celerity.types.context import HttpHandlerContext
from celerity.types.http import HttpRequest


def build_http_request(
    *,
    method: str = "GET",
    path: str = "/",
    path_params: dict[str, str] | None = None,
    query: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
    cookies: dict[str, str] | None = None,
    body: str | None = None,
    content_type: str | None = None,
    auth: dict[str, Any] | None = None,
    request_id: str = "",
    client_ip: str | None = None,
) -> HttpRequest:
    """Build an ``HttpRequest`` from keyword arguments.

    Convenience helper for constructing requests in function handlers
    and tests.

    Args:
        method: HTTP method.
        path: Request path.
        path_params: Extracted path parameters.
        query: Query string parameters.
        headers: Request headers.
        cookies: Request cookies.
        body: Raw text body.
        content_type: Content-Type header value.
        auth: Decoded auth payload.
        request_id: Unique request ID.
        client_ip: Client IP address.

    Returns:
        A populated ``HttpRequest``.
    """
    return HttpRequest(
        method=method,
        path=path,
        path_params=path_params or {},
        query=query or {},
        headers=headers or {},
        cookies=cookies or {},
        text_body=body,
        content_type=content_type,
        auth=auth,
        request_id=request_id,
        client_ip=client_ip,
    )


def build_http_context(
    request: HttpRequest,
    container: Any = None,
    metadata: dict[str, Any] | None = None,
) -> HttpHandlerContext:
    """Build an ``HttpHandlerContext`` from a request and options.

    Args:
        request: The HTTP request.
        container: The DI container.
        metadata: Optional initial metadata key-value pairs.

    Returns:
        A populated ``HttpHandlerContext``.
    """
    return HttpHandlerContext(
        request=request,
        metadata=HandlerMetadataStore(metadata),
        container=container,
    )
