"""HTTP handler pipeline."""

from __future__ import annotations

import inspect
import json
import logging
from typing import TYPE_CHECKING, Any

from celerity.errors.http_exception import HttpError
from celerity.handlers.param_extractor import resolve_handler_params
from celerity.layers.pipeline import run_layer_pipeline
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.context import HttpHandlerContext
from celerity.types.http import HttpResponse

if TYPE_CHECKING:
    from celerity.types.handler import ResolvedHandlerBase
    from celerity.types.http import HttpRequest

logger = logging.getLogger("celerity.pipeline")


async def execute_http_pipeline(
    handler: ResolvedHandlerBase,
    request: HttpRequest,
    options: dict[str, Any],
) -> HttpResponse:
    """Execute the HTTP handler pipeline.

    Pipeline order:

    1. Build ``HttpHandlerContext``
    2. Compose layers: system -> module -> handler
    3. Run layer pipeline with ``supports()`` filtering
    4. Extract parameters from request
    5. Invoke handler function
    6. Normalise response

    Guard execution is managed by the core runtime, not this pipeline.

    Args:
        handler: The resolved handler to execute.
        request: The incoming HTTP request.
        options: Pipeline options including ``container``,
            ``system_layers``, ``app_layers``, ``handler_name``.

    Returns:
        The HTTP response.
    """
    container = options.get("container")
    system_layers: list[Any] = options.get("system_layers", [])
    app_layers: list[Any] = options.get("app_layers", [])
    handler_name: str | None = options.get("handler_name")

    context = HttpHandlerContext(
        request=request,
        metadata=HandlerMetadataStore({
            **(handler.custom_metadata or {}),
            **({"handler_name": handler_name} if handler_name else {}),
        }),
        container=container,  # type: ignore[arg-type]
    )

    all_layers = [*system_layers, *app_layers, *handler.layers]
    logger.debug("%s %s — %d layers", request.method, request.path, len(all_layers))

    async def core_handler() -> HttpResponse:
        params = resolve_handler_params(handler, context)
        if handler.is_function_handler:
            result = handler.handler_fn(request, context, *params)
        elif params:
            result = handler.handler_fn(*params)
        else:
            result = handler.handler_fn()

        if inspect.isawaitable(result):
            result = await result

        return _normalise_response(result)

    try:
        raw = await run_layer_pipeline(all_layers, context, core_handler, handler_type="http")
        response = _normalise_response(raw)
        logger.debug("response %d", response.status)
        return response
    except HttpError as exc:
        logger.debug("HttpError %d: %s", exc.status_code, exc.message)
        return HttpResponse(
            status=exc.status_code,
            body=json.dumps({"message": exc.message}),
            headers={"content-type": "application/json"},
        )
    except Exception:
        logger.exception("Unhandled error in handler pipeline")
        return HttpResponse(
            status=500,
            body=json.dumps({"message": "Internal Server Error"}),
            headers={"content-type": "application/json"},
        )


def _normalise_response(result: Any) -> HttpResponse:
    """Convert a handler return value to an HttpResponse."""
    if isinstance(result, HttpResponse):
        return result
    if isinstance(result, dict):
        return HttpResponse(
            status=200,
            body=json.dumps(result),
            headers={"content-type": "application/json"},
        )
    if isinstance(result, str):
        return HttpResponse(status=200, body=result)
    if result is None:
        return HttpResponse(status=204)
    return HttpResponse(
        status=200,
        body=json.dumps(result),
        headers={"content-type": "application/json"},
    )
