"""Tests for the ValidationLayer."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from celerity.errors.http_exception import BadRequestError
from celerity.layers.validate import ValidationLayer, validate
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.http import HttpRequest


class OrderInput(BaseModel):
    name: str
    quantity: int


class SearchParams(BaseModel):
    page: int
    sort: str = "name"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_http_context(
    *,
    body: str | None = None,
    path_params: dict[str, str] | None = None,
    query: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
) -> Any:
    """Create a minimal HTTP context with a request and metadata store."""
    from types import SimpleNamespace

    request = HttpRequest(
        method="POST",
        path="/orders",
        text_body=body,
        path_params=path_params or {},
        query=query or {},
        headers=headers or {},
    )
    return SimpleNamespace(
        request=request,
        message=None,
        metadata=HandlerMetadataStore({}),
    )


def _make_ws_context(*, json_body: Any = None) -> Any:
    """Create a minimal WebSocket context."""
    from types import SimpleNamespace

    message = SimpleNamespace(json_body=json_body)
    return SimpleNamespace(
        request=None,
        message=message,
        metadata=HandlerMetadataStore({}),
    )


# ---------------------------------------------------------------------------
# HTTP body validation
# ---------------------------------------------------------------------------


class TestBodyValidation:
    async def test_valid_body_stored_in_metadata(self) -> None:
        layer = ValidationLayer({"body": OrderInput})
        ctx = _make_http_context(body='{"name": "Widget", "quantity": 5}')
        called = False

        async def next_handler() -> None:
            nonlocal called
            called = True

        await layer.handle(ctx, next_handler)
        assert called
        validated = ctx.metadata.get("validated_body")
        assert isinstance(validated, OrderInput)
        assert validated.name == "Widget"
        assert validated.quantity == 5

    async def test_invalid_body_raises_bad_request(self) -> None:
        layer = ValidationLayer({"body": OrderInput})
        ctx = _make_http_context(body='{"name": "Widget"}')  # missing quantity

        with pytest.raises(BadRequestError, match="Body validation failed"):
            await layer.handle(ctx, _noop)

    async def test_empty_body_skips_validation(self) -> None:
        layer = ValidationLayer({"body": OrderInput})
        ctx = _make_http_context(body=None)
        called = False

        async def next_handler() -> None:
            nonlocal called
            called = True

        await layer.handle(ctx, next_handler)
        assert called
        assert ctx.metadata.get("validated_body") is None


# ---------------------------------------------------------------------------
# HTTP params validation
# ---------------------------------------------------------------------------


class TestParamsValidation:
    async def test_valid_params(self) -> None:
        class OrderParams(BaseModel):
            order_id: str

        layer = ValidationLayer({"params": OrderParams})
        ctx = _make_http_context(path_params={"order_id": "abc-123"})
        await layer.handle(ctx, _noop)

        validated = ctx.metadata.get("validated_params")
        assert validated.order_id == "abc-123"

    async def test_invalid_params_raises(self) -> None:
        class StrictParams(BaseModel):
            order_id: int

        layer = ValidationLayer({"params": StrictParams})
        ctx = _make_http_context(path_params={"order_id": "not-a-number"})

        with pytest.raises(BadRequestError, match="Path params validation failed"):
            await layer.handle(ctx, _noop)


# ---------------------------------------------------------------------------
# HTTP query validation
# ---------------------------------------------------------------------------


class TestQueryValidation:
    async def test_valid_query(self) -> None:
        layer = ValidationLayer({"query": SearchParams})
        ctx = _make_http_context(query={"page": 1, "sort": "date"})
        await layer.handle(ctx, _noop)

        validated = ctx.metadata.get("validated_query")
        assert validated.page == 1
        assert validated.sort == "date"

    async def test_invalid_query_raises(self) -> None:
        layer = ValidationLayer({"query": SearchParams})
        ctx = _make_http_context(query={"page": "not-a-number"})

        with pytest.raises(BadRequestError, match="Query validation failed"):
            await layer.handle(ctx, _noop)


# ---------------------------------------------------------------------------
# HTTP headers validation
# ---------------------------------------------------------------------------


class TestHeadersValidation:
    async def test_valid_headers(self) -> None:
        class RequiredHeaders(BaseModel):
            x_api_key: str

        layer = ValidationLayer({"headers": RequiredHeaders})
        ctx = _make_http_context(headers={"x_api_key": "secret-123"})
        await layer.handle(ctx, _noop)

        validated = ctx.metadata.get("validated_headers")
        assert validated.x_api_key == "secret-123"


# ---------------------------------------------------------------------------
# WebSocket validation
# ---------------------------------------------------------------------------


class TestWebSocketValidation:
    async def test_valid_ws_body(self) -> None:
        class ChatMessage(BaseModel):
            action: str
            text: str

        layer = ValidationLayer({"ws_message_body": ChatMessage})
        ctx = _make_ws_context(json_body={"action": "send", "text": "hello"})
        await layer.handle(ctx, _noop)

        validated = ctx.metadata.get("validated_message_body")
        assert validated.action == "send"
        assert validated.text == "hello"

    async def test_invalid_ws_body_raises(self) -> None:
        class ChatMessage(BaseModel):
            action: str
            text: str

        layer = ValidationLayer({"ws_message_body": ChatMessage})
        ctx = _make_ws_context(json_body={"action": "send"})  # missing text

        with pytest.raises(BadRequestError, match="WebSocket message validation"):
            await layer.handle(ctx, _noop)


# ---------------------------------------------------------------------------
# validate() factory
# ---------------------------------------------------------------------------


class TestValidateFactory:
    def test_returns_validation_layer(self) -> None:
        layer = validate({"body": OrderInput})
        assert isinstance(layer, ValidationLayer)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _noop() -> None:
    pass
