"""Validation layer factory."""

from __future__ import annotations

import json
from typing import Any

from celerity.errors.http_exception import BadRequestError
from celerity.types.layer import CelerityLayer

# Validation error messages matching the Node.js SDK convention.
_VALIDATION_MESSAGES: dict[str, str] = {
    "body": "Body validation failed",
    "params": "Path params validation failed",
    "query": "Query validation failed",
    "headers": "Headers validation failed",
    "ws_message_body": "WebSocket message validation failed",
}


def _format_validation_error(exc: Exception) -> list[dict[str, Any]]:
    """Convert a Pydantic ``ValidationError`` into a structured details list.

    Each entry mirrors the shape used by the Node.js SDK (Zod issues):

    .. code-block:: json

        {
            "code": "value_error",
            "path": ["email"],
            "message": "value is not a valid email address"
        }

    Non-Pydantic errors fall back to a single-entry list with the
    exception message.
    """
    try:
        from pydantic import ValidationError

        if isinstance(exc, ValidationError):
            details: list[dict[str, Any]] = []
            for error in exc.errors():
                entry: dict[str, Any] = {
                    "code": error.get("type", "validation_error"),
                    "path": [str(p) for p in error.get("loc", [])],
                    "message": error.get("msg", str(exc)),
                }
                ctx = error.get("ctx")
                if ctx:
                    for key, value in ctx.items():
                        if key not in ("error",):
                            entry[key] = value
                details.append(entry)
            return details
    except ImportError:
        pass

    return [{"code": "validation_error", "path": [], "message": str(exc)}]


class ValidationLayer(CelerityLayer):
    """Layer that validates request parts against schemas.

    Validates body, params, query, and/or headers using the provided
    schemas before passing control to the next layer or handler.
    Raises ``BadRequestError`` on validation failure with structured
    details matching the Node.js SDK format.

    Args:
        schemas: A dict mapping part names to schema objects. Each
            schema must implement ``model_validate(data)``.
    """

    def __init__(self, schemas: dict[str, Any]) -> None:
        self._schemas = schemas

    async def handle(self, context: Any, next_handler: Any) -> Any:
        """Validate request parts and continue the pipeline.

        Args:
            context: The handler context with request/event data.
            next_handler: The next function in the pipeline chain.

        Returns:
            The result from the next handler.

        Raises:
            BadRequestError: If any schema validation fails.
        """
        request = getattr(context, "request", None)
        if request is not None:
            self._validate_http(request, context)

        message = getattr(context, "message", None)
        if message is not None:
            self._validate_websocket(message, context)

        return await next_handler()

    def _validate_http(self, request: Any, context: Any) -> None:
        if "body" in self._schemas and request.text_body:
            _run_validation(
                self._schemas["body"],
                json.loads(request.text_body),
                "body",
                context,
                "validated_body",
            )

        if "params" in self._schemas and request.path_params:
            _run_validation(
                self._schemas["params"],
                request.path_params,
                "params",
                context,
                "validated_params",
            )

        if "query" in self._schemas and request.query:
            _run_validation(
                self._schemas["query"],
                request.query,
                "query",
                context,
                "validated_query",
            )

        if "headers" in self._schemas and request.headers:
            _run_validation(
                self._schemas["headers"],
                dict(request.headers),
                "headers",
                context,
                "validated_headers",
            )

    def _validate_websocket(self, message: Any, context: Any) -> None:
        if "ws_message_body" in self._schemas and message.json_body:
            _run_validation(
                self._schemas["ws_message_body"],
                message.json_body,
                "ws_message_body",
                context,
                "validated_message_body",
            )


def _run_validation(
    schema: Any,
    data: Any,
    part: str,
    context: Any,
    metadata_key: str,
) -> None:
    """Validate data against a schema and store the result in context metadata.

    Raises ``BadRequestError`` with structured details on failure.
    """
    try:
        validated = schema.model_validate(data)
        context.metadata.set(metadata_key, validated)
    except Exception as exc:
        raise BadRequestError(
            message=_VALIDATION_MESSAGES.get(part, "Validation failed"),
            details=_format_validation_error(exc),
        ) from exc


def validate(schemas: dict[str, Any]) -> ValidationLayer:
    """Create a validation layer for the given schemas.

    Args:
        schemas: A dict mapping part names to schema objects.
            Supported keys: ``"body"``, ``"params"``, ``"query"``,
            ``"headers"``, ``"ws_message_body"``.

    Returns:
        A ``ValidationLayer`` instance.

    Example::

        @controller("/orders")
        @use_layer(validate({"body": CreateOrderInput}))
        class OrderController:
            @post("/")
            async def create(self, body: Body[CreateOrderInput]) -> HandlerResponse: ...
    """
    return ValidationLayer(schemas)
