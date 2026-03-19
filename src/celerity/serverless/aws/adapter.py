"""AWS Lambda adapter for the Celerity SDK.

Provides ``create_lambda_handler()`` which returns a sync Lambda handler
function. The handler bootstraps the application on cold start, caches
state for warm invocations, and dispatches to the appropriate pipeline
based on event type.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
from typing import Any

from celerity.serverless.aws.event_mapper import (
    detect_event_type,
    map_api_gateway_v2_event,
    map_api_gateway_websocket_event,
    map_consumer_result_to_sqs_batch_response,
    map_eventbridge_event,
    map_http_response_to_result,
    map_sqs_event,
)

logger = logging.getLogger("celerity.serverless.aws")

_cached_state: dict[str, Any] | None = None


async def _ensure_bootstrapped() -> dict[str, Any]:
    """Bootstrap the application on first invocation, cache for warm starts."""
    global _cached_state
    if _cached_state is not None:
        return _cached_state

    from celerity.application import CelerityFactory
    from celerity.bootstrap.discovery import discover_module

    root_module = discover_module()
    app = await CelerityFactory.create(root_module)

    _cached_state = {
        "app": app,
        "registry": app.registry,
        "container": app.container,
        "system_layers": app.system_layers,
    }

    logger.debug("cold start: bootstrapped %s", root_module.__name__)
    return _cached_state


def create_lambda_handler() -> Any:
    """Create a Lambda handler function.

    Usage in ``handler.py``::

        from celerity.serverless.aws import create_lambda_handler
        handler = create_lambda_handler()

    The handler dispatches based on event type detected from the event
    shape and the ``CELERITY_HANDLER_TYPE`` env var.

    Returns:
        A sync Lambda handler function ``(event, context) -> result``.
    """
    handler_id = os.environ.get("CELERITY_HANDLER_ID")
    handler_tag = os.environ.get("CELERITY_HANDLER_TAG")
    _register_sigterm_handler()

    def handler(event: dict[str, Any], context: Any) -> Any:
        loop = _get_or_create_event_loop()
        return loop.run_until_complete(
            _handle(event, context, handler_id, handler_tag),
        )

    return handler


async def _handle(
    event: dict[str, Any],
    context: Any,
    handler_id: str | None,
    handler_tag: str | None,
) -> Any:
    """Route the Lambda invocation to the appropriate handler pipeline."""
    state = await _ensure_bootstrapped()
    event_type = detect_event_type(event)

    logger.debug("dispatch: event_type=%s handler_id=%s", event_type, handler_id)

    if event_type == "http":
        return await _handle_http(event, state, handler_id)
    if event_type == "consumer":
        return await _handle_consumer(event, state, handler_tag)
    if event_type == "schedule":
        return await _handle_schedule(event, state, handler_tag)
    if event_type == "websocket":
        return await _handle_websocket(event, state)
    if event_type == "custom":
        return await _handle_custom(event, state, handler_id)

    logger.warning("unknown event type: %s", event_type)
    return {"statusCode": 500, "body": "Unknown event type"}


# ---------------------------------------------------------------------------
# Handler type dispatchers
# ---------------------------------------------------------------------------


async def _handle_http(
    event: dict[str, Any],
    state: dict[str, Any],
    handler_id: str | None,
) -> dict[str, Any]:
    from celerity.handlers.http_pipeline import execute_http_pipeline

    request = map_api_gateway_v2_event(event)
    registry = state["registry"]

    handler = None
    if handler_id:
        handler = registry.get_handler_by_id("http", handler_id)
    if handler is None:
        handler = registry.get_handler("http", f"{request.method} {request.path}")

    if handler is None:
        return {"statusCode": 404, "body": "Handler not found"}

    response = await execute_http_pipeline(
        handler,
        request,
        {
            "container": state["container"],
            "system_layers": state["system_layers"],
        },
    )
    return map_http_response_to_result(response)


async def _handle_consumer(
    event: dict[str, Any],
    state: dict[str, Any],
    handler_tag: str | None,
) -> dict[str, Any]:
    from celerity.handlers.consumer_pipeline import execute_consumer_pipeline

    tag = handler_tag or _derive_consumer_tag(event)
    sdk_event = map_sqs_event(event, tag)
    registry = state["registry"]

    handler = registry.get_handler("consumer", tag)
    if handler is None:
        logger.warning("consumer handler not found for tag: %s", tag)
        return {"batchItemFailures": []}

    result = await execute_consumer_pipeline(
        handler,
        sdk_event,
        {
            "container": state["container"],
            "system_layers": state["system_layers"],
        },
    )

    failures: list[dict[str, str]] | None = None
    if result.failures:
        failures = [
            {"message_id": f.message_id, "error_message": f.error_message or ""}
            for f in result.failures
        ]
    return map_consumer_result_to_sqs_batch_response(failures)


async def _handle_schedule(
    event: dict[str, Any],
    state: dict[str, Any],
    handler_tag: str | None,
) -> dict[str, Any]:
    from celerity.handlers.schedule_pipeline import execute_schedule_pipeline

    tag = handler_tag or event.get("id", "")
    sdk_event = map_eventbridge_event(event, tag)
    registry = state["registry"]

    handler = registry.get_handler("schedule", tag)
    if handler is None:
        logger.warning("schedule handler not found for tag: %s", tag)
        return {"success": False}

    result = await execute_schedule_pipeline(
        handler,
        sdk_event,
        {
            "container": state["container"],
            "system_layers": state["system_layers"],
        },
    )
    return {"success": result.success, "errorMessage": result.error_message}


async def _handle_websocket(
    event: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any]:
    from celerity.handlers.websocket_pipeline import execute_websocket_pipeline
    from celerity.serverless.aws.websocket_sender import ApiGatewayWebSocketSender

    message, endpoint_url = map_api_gateway_websocket_event(event)
    registry = state["registry"]
    container = state["container"]

    # Register WebSocket sender if not already present.
    if not container.has("WebSocketSender") and endpoint_url:
        sender = ApiGatewayWebSocketSender(endpoint_url)
        container.register("WebSocketSender", {"use_value": sender})

    route = event.get("requestContext", {}).get("routeKey", "$default")
    handler = registry.get_handler("websocket", route)
    if handler is None:
        handler = registry.get_handler("websocket", "$default")
    if handler is None:
        return {"statusCode": 200}

    await execute_websocket_pipeline(
        handler,
        message,
        {
            "container": container,
            "system_layers": state["system_layers"],
        },
    )
    return {"statusCode": 200}


async def _handle_custom(
    event: dict[str, Any],
    state: dict[str, Any],
    handler_id: str | None,
) -> Any:
    from celerity.handlers.custom_pipeline import execute_custom_pipeline

    registry = state["registry"]
    handler_name = handler_id or event.get("handlerName")

    handler = None
    if handler_name:
        handler = registry.get_handler("custom", handler_name)

    # Fallback: use the only registered custom handler.
    if handler is None:
        all_custom = registry.get_handlers_by_type("custom")
        if len(all_custom) == 1:
            handler = all_custom[0]

    if handler is None:
        return {"error": "Custom handler not found"}

    payload = event.get("payload", event)
    return await execute_custom_pipeline(
        handler,
        payload,
        {
            "container": state["container"],
            "system_layers": state["system_layers"],
        },
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _derive_consumer_tag(event: dict[str, Any]) -> str:
    """Derive a consumer handler tag from the SQS event."""
    records: list[dict[str, Any]] = event.get("Records", [])
    if records:
        arn: str = records[0].get("eventSourceARN", "")
        if arn:
            return arn.rsplit(":", 1)[-1]
    return ""


def _get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """Get the running event loop or create a new one."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def _register_sigterm_handler() -> None:
    """Register a SIGTERM handler for graceful Lambda shutdown."""

    def _sigterm_handler(_signum: int, _frame: Any) -> None:
        global _cached_state
        if _cached_state and _cached_state.get("app"):
            loop = _get_or_create_event_loop()
            loop.run_until_complete(_cached_state["app"].close())
            _cached_state = None
            logger.debug("SIGTERM: application shut down")

    signal.signal(signal.SIGTERM, _sigterm_handler)
