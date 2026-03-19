"""Validation layer factory."""

from __future__ import annotations

import json
from typing import Any

from celerity.errors.http_exception import BadRequestError
from celerity.types.layer import CelerityLayer


class ValidationLayer(CelerityLayer):
    """Layer that validates request parts against schemas.

    Validates body, params, query, and/or headers using the provided
    schemas before passing control to the next layer or handler.
    Raises ``BadRequestError`` on validation failure.

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
            try:
                data = json.loads(request.text_body)
                validated = self._schemas["body"].model_validate(data)
                context.metadata.set("validated_body", validated)
            except Exception as exc:
                raise BadRequestError(f"Body validation failed: {exc}") from exc

        if "params" in self._schemas and request.path_params:
            try:
                validated = self._schemas["params"].model_validate(request.path_params)
                context.metadata.set("validated_params", validated)
            except Exception as exc:
                raise BadRequestError(f"Params validation failed: {exc}") from exc

        if "query" in self._schemas and request.query:
            try:
                validated = self._schemas["query"].model_validate(request.query)
                context.metadata.set("validated_query", validated)
            except Exception as exc:
                raise BadRequestError(f"Query validation failed: {exc}") from exc

        if "headers" in self._schemas and request.headers:
            try:
                validated = self._schemas["headers"].model_validate(dict(request.headers))
                context.metadata.set("validated_headers", validated)
            except Exception as exc:
                raise BadRequestError(f"Headers validation failed: {exc}") from exc

    def _validate_websocket(self, message: Any, context: Any) -> None:
        if "ws_message_body" in self._schemas and message.json_body:
            try:
                validated = self._schemas["ws_message_body"].model_validate(message.json_body)
                context.metadata.set("validated_message_body", validated)
            except Exception as exc:
                raise BadRequestError(f"WebSocket message validation failed: {exc}") from exc


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
