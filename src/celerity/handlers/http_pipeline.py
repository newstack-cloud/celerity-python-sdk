"""HTTP handler pipeline."""

from __future__ import annotations

import dataclasses
import inspect
import json
import logging
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from celerity.errors.http_exception import HttpError
from celerity.handlers.param_extractor import resolve_handler_params
from celerity.handlers.resolve import resolve_handler_instance
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
        metadata=HandlerMetadataStore(
            {
                **(handler.custom_metadata or {}),
                **({"handler_name": handler_name} if handler_name else {}),
            }
        ),
        container=container,  # type: ignore[arg-type]
    )

    all_layers = [*system_layers, *app_layers, *handler.layers]
    logger.debug("%s %s — %d layers", request.method, request.path, len(all_layers))

    http_method = getattr(handler, "method", None)

    async def core_handler() -> HttpResponse:
        if not handler.is_function_handler:
            await resolve_handler_instance(handler, container)
        params = resolve_handler_params(handler, context)
        if handler.is_function_handler:
            result = handler.handler_fn(request, context, *params)
        elif params:
            result = handler.handler_fn(*params)
        else:
            result = handler.handler_fn()

        if inspect.isawaitable(result):
            result = await result

        return _normalise_response(result, method=http_method)

    try:
        raw = await run_layer_pipeline(all_layers, context, core_handler, handler_type="http")
        response = _normalise_response(raw, method=http_method)
        logger.debug("response %d", response.status)
        return response
    except HttpError as exc:
        logger.debug("HttpError %d: %s", exc.status_code, exc.message)
        error_body: dict[str, Any] = {"message": exc.message}
        if exc.details is not None:
            error_body["details"] = exc.details
        return HttpResponse(
            status=exc.status_code,
            body=json.dumps(error_body),
            headers={"content-type": "application/json"},
        )
    except Exception:
        logger.exception("Unhandled error in handler pipeline")
        return HttpResponse(
            status=500,
            body=json.dumps({"message": "Internal Server Error"}),
            headers={"content-type": "application/json"},
        )


def _default_status(method: str | None) -> int:
    """Infer default success status from HTTP method."""
    if method and method.upper() == "POST":
        return 201
    return 200


def _normalise_response(result: Any, *, method: str | None = None) -> HttpResponse:
    """Convert a handler return value to an HttpResponse."""
    if isinstance(result, HttpResponse):
        return result
    if result is None:
        return HttpResponse(status=204)
    if isinstance(result, tuple) and len(result) == 2:
        status, body = result
        if isinstance(status, int):
            return _normalise_body(body, status)
    status = _default_status(method)
    if isinstance(result, dict):
        return HttpResponse(
            status=status,
            body=json.dumps(result),
            headers={"content-type": "application/json"},
        )
    if isinstance(result, str):
        return HttpResponse(status=status, body=result)
    if isinstance(result, BaseModel):
        return HttpResponse(
            status=status,
            body=result.model_dump_json(),
            headers={"content-type": "application/json"},
        )
    if dataclasses.is_dataclass(result) and not isinstance(result, type):
        return HttpResponse(
            status=status,
            body=json.dumps(dataclasses.asdict(result)),
            headers={"content-type": "application/json"},
        )
    return HttpResponse(
        status=status,
        body=json.dumps(result),
        headers={"content-type": "application/json"},
    )


def _normalise_body(body: Any, status: int) -> HttpResponse:
    """Normalise the body part of a ``(status, body)`` tuple response."""
    if body is None:
        return HttpResponse(status=status)
    if isinstance(body, str):
        return HttpResponse(status=status, body=body)
    if isinstance(body, dict):
        return HttpResponse(
            status=status,
            body=json.dumps(body),
            headers={"content-type": "application/json"},
        )
    if isinstance(body, BaseModel):
        return HttpResponse(
            status=status,
            body=body.model_dump_json(),
            headers={"content-type": "application/json"},
        )
    if dataclasses.is_dataclass(body) and not isinstance(body, type):
        return HttpResponse(
            status=status,
            body=json.dumps(dataclasses.asdict(body)),
            headers={"content-type": "application/json"},
        )
    return HttpResponse(
        status=status,
        body=json.dumps(body),
        headers={"content-type": "application/json"},
    )
