"""Tests for celerity.handlers.param_extractor."""

from typing import Any

from celerity.decorators.params import Auth, Body, Param
from celerity.handlers.param_extractor import extract_param_metadata, resolve_handler_params
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.context import HttpHandlerContext
from celerity.types.handler import ParamMetadata, ResolvedHttpHandler
from celerity.types.http import HttpRequest


class TestExtractParamMetadata:
    def test_extracts_body(self) -> None:
        def handler(self: Any, body: Body[dict[str, str]]) -> None:
            pass

        result = extract_param_metadata(handler)
        assert len(result) == 1
        assert result[0].type == "body"
        assert result[0].index == 1

    def test_extracts_multiple(self) -> None:
        def handler(
            self: Any,
            order_id: Param[str],
            auth: Auth,
        ) -> None:
            pass

        result = extract_param_metadata(handler)
        assert len(result) == 2
        assert result[0].type == "param"
        assert result[1].type == "auth"

    def test_skips_self(self) -> None:
        def handler(self: Any) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result == []

    def test_skips_unannotated(self) -> None:
        def handler(self: Any, something: str) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result == []


class TestExtractParamsFromRequest:
    def _make_context(self, **request_kwargs: object) -> HttpHandlerContext:
        return HttpHandlerContext(
            request=HttpRequest(method="GET", path="/", **request_kwargs),  # type: ignore[arg-type]
            metadata=HandlerMetadataStore(),
            container=None,  # type: ignore[arg-type]
        )

    def test_extract_body(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="body")],
        )
        ctx = self._make_context(text_body='{"name": "test"}')
        values = resolve_handler_params(handler, ctx)
        assert values == [{"name": "test"}]

    def test_extract_query(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="query")],
        )
        ctx = self._make_context(query={"page": "1"})
        values = resolve_handler_params(handler, ctx)
        assert values == [{"page": "1"}]

    def test_extract_path_param(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="param", key="id")],
        )
        ctx = self._make_context(path_params={"id": "123"})
        values = resolve_handler_params(handler, ctx)
        assert values == ["123"]

    def test_extract_auth(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="auth")],
        )
        ctx = self._make_context(auth={"sub": "user1"})
        values = resolve_handler_params(handler, ctx)
        assert values == [{"sub": "user1"}]

    def test_extract_request(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="request")],
        )
        ctx = self._make_context()
        values = resolve_handler_params(handler, ctx)
        assert isinstance(values[0], HttpRequest)

    def test_extract_request_id(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="requestId")],
        )
        ctx = self._make_context(request_id="req-123")
        values = resolve_handler_params(handler, ctx)
        assert values == ["req-123"]

    def test_extract_bearer_token(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="token")],
        )
        ctx = self._make_context(headers={"authorization": "Bearer sk-abc"})
        values = resolve_handler_params(handler, ctx)
        assert values == ["sk-abc"]
