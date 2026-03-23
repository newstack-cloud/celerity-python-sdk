"""Tests for celerity.handlers.param_extractor."""

from typing import Annotated, Any

from celerity.decorators.params import (
    Auth,
    Body,
    Cookie,
    Cookies,
    Header,
    Headers,
    Key,
    Param,
    Query,
    QueryParam,
)
from celerity.handlers.param_extractor import (
    extract_param_metadata,
    resolve_handler_params,
)
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
        assert result[0].index == 0

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

    def test_param_auto_derives_key(self) -> None:
        def handler(self: Any, order_id: Param[str]) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result[0].key == "order_id"

    def test_param_explicit_key_override(self) -> None:
        def handler(
            self: Any,
            order_id: Annotated[Param[str], Key("orderId")],
        ) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result[0].key == "orderId"

    def test_header_converts_underscores_to_hyphens(self) -> None:
        def handler(self: Any, x_request_id: Header[str]) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result[0].type == "header"
        assert result[0].key == "x-request-id"

    def test_header_explicit_key_override(self) -> None:
        def handler(
            self: Any,
            api_key: Annotated[Header[str], Key("X-Api-Key")],
        ) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result[0].key == "X-Api-Key"

    def test_cookie_auto_derives_key(self) -> None:
        def handler(self: Any, session_id: Cookie[str]) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result[0].type == "cookie"
        assert result[0].key == "session_id"

    def test_query_param_auto_derives_key(self) -> None:
        def handler(self: Any, page: QueryParam[int]) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result[0].type == "query_param"
        assert result[0].key == "page"

    def test_query_param_explicit_key_override(self) -> None:
        def handler(
            self: Any,
            sort_by: Annotated[QueryParam[str], Key("sortBy")],
        ) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result[0].key == "sortBy"

    def test_headers_plural_no_auto_key(self) -> None:
        def handler(self: Any, headers: Headers[dict[str, str]]) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result[0].type == "headers"
        assert result[0].key is None

    def test_query_plural_no_auto_key(self) -> None:
        def handler(self: Any, filters: Query[dict[str, str]]) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result[0].type == "query"
        assert result[0].key is None

    def test_cookies_plural_no_auto_key(self) -> None:
        def handler(self: Any, cookies: Cookies[dict[str, str]]) -> None:
            pass

        result = extract_param_metadata(handler)
        assert result[0].type == "cookies"
        assert result[0].key is None


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

    def test_extract_single_header(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="header", key="x-request-id")],
        )
        ctx = self._make_context(headers={"x-request-id": "req-456"})
        values = resolve_handler_params(handler, ctx)
        assert values == ["req-456"]

    def test_extract_single_cookie(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="cookie", key="session_id")],
        )
        ctx = self._make_context(cookies={"session_id": "abc123"})
        values = resolve_handler_params(handler, ctx)
        assert values == ["abc123"]

    def test_extract_single_query_param(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="query_param", key="page")],
        )
        ctx = self._make_context(query={"page": "3", "limit": "10"})
        values = resolve_handler_params(handler, ctx)
        assert values == ["3"]

    def test_extract_path_param_by_auto_key(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="param", key="order_id")],
        )
        ctx = self._make_context(path_params={"order_id": "ord-789"})
        values = resolve_handler_params(handler, ctx)
        assert values == ["ord-789"]

    def test_headers_plural_returns_full_dict(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="headers")],
        )
        headers = {"content-type": "application/json", "x-custom": "val"}
        ctx = self._make_context(headers=headers)
        values = resolve_handler_params(handler, ctx)
        assert values == [headers]

    def test_query_plural_returns_full_dict(self) -> None:
        handler = ResolvedHttpHandler(
            handler_fn=lambda: None,
            param_metadata=[ParamMetadata(index=1, type="query")],
        )
        query = {"page": "1", "limit": "10"}
        ctx = self._make_context(query=query)
        values = resolve_handler_params(handler, ctx)
        assert values == [query]
