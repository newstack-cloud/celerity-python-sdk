"""Tests for celerity.layers.pipeline."""

from typing import Any

from celerity.layers.pipeline import run_layer_pipeline
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.context import BaseHandlerContext
from celerity.types.layer import CelerityLayer


class _TrackingLayer(CelerityLayer):
    def __init__(self, name: str, log: list[str]) -> None:
        self.name = name
        self.log = log

    async def handle(self, context: Any, next_handler: Any) -> Any:
        self.log.append(f"{self.name}:before")
        result = await next_handler()
        self.log.append(f"{self.name}:after")
        return result


class _ShortCircuitLayer(CelerityLayer):
    async def handle(self, context: Any, next_handler: Any) -> str:
        return "short-circuited"


class _HttpOnlyLayer(CelerityLayer):
    def __init__(self, log: list[str]) -> None:
        self.log = log

    def supports(self, handler_type: str) -> bool:
        return handler_type == "http"

    async def handle(self, context: Any, next_handler: Any) -> Any:
        self.log.append("http-only")
        return await next_handler()


def _make_context() -> BaseHandlerContext:
    return BaseHandlerContext(
        metadata=HandlerMetadataStore(),
        container=None,  # type: ignore[arg-type]
    )


class TestLayerPipeline:
    async def test_layers_run_in_order(self) -> None:
        log: list[str] = []
        layers = [_TrackingLayer("A", log), _TrackingLayer("B", log)]

        async def handler() -> str:
            log.append("handler")
            return "ok"

        result = await run_layer_pipeline(layers, _make_context(), handler)
        assert result == "ok"
        assert log == ["A:before", "B:before", "handler", "B:after", "A:after"]

    async def test_short_circuit(self) -> None:
        log: list[str] = []
        layers = [_ShortCircuitLayer(), _TrackingLayer("B", log)]

        async def handler() -> str:
            log.append("handler")
            return "should not reach"

        result = await run_layer_pipeline(layers, _make_context(), handler)
        assert result == "short-circuited"
        assert log == []

    async def test_supports_filter(self) -> None:
        log: list[str] = []
        layers = [_HttpOnlyLayer(log), _TrackingLayer("B", log)]

        async def handler() -> str:
            log.append("handler")
            return "ok"

        await run_layer_pipeline(layers, _make_context(), handler, handler_type="consumer")
        assert "http-only" not in log
        assert "B:before" in log

    async def test_supports_allows_matching_type(self) -> None:
        log: list[str] = []
        layers = [_HttpOnlyLayer(log)]

        async def handler() -> str:
            log.append("handler")
            return "ok"

        await run_layer_pipeline(layers, _make_context(), handler, handler_type="http")
        assert "http-only" in log

    async def test_empty_layers(self) -> None:
        async def handler() -> str:
            return "direct"

        result = await run_layer_pipeline([], _make_context(), handler)
        assert result == "direct"

    async def test_layer_can_modify_context(self) -> None:
        class _EnrichLayer(CelerityLayer):
            async def handle(self, context: Any, next_handler: Any) -> Any:
                context.metadata.set("enriched", True)
                return await next_handler()

        ctx = _make_context()

        async def handler() -> Any:
            return ctx.metadata.get("enriched")

        result = await run_layer_pipeline([_EnrichLayer()], ctx, handler)
        assert result is True
