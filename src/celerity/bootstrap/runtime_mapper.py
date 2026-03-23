"""Bidirectional mapping between PyO3 runtime types and SDK types.

PyO3 types come from ``celerity_runtime_sdk`` (the Rust FFI module).
SDK types are the Python dataclasses used internally by the handler
pipeline. This module translates between the two at the FFI boundary.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from celerity_runtime_sdk import (
    ConsumerEventInput as RuntimeConsumerEvent,
    EventResult as RuntimeEventResult,
    GuardInput as RuntimeGuardInput,
    GuardResult as RuntimeGuardResult,
    MessageProcessingFailure as RuntimeFailure,
    Request as RuntimeRequest,
    RequestContext as RuntimeRequestContext,
    Response as RuntimeResponse,
    ResponseBuilder,
    ScheduleEventInput as RuntimeScheduleEvent,
    WebSocketMessageInfo as RuntimeWebSocketMsg,
)

from celerity.types.consumer import ConsumerEventInput, ConsumerMessage, EventResult
from celerity.types.guard import GuardInput
from celerity.types.http import HttpRequest, HttpResponse
from celerity.types.schedule import ScheduleEventInput
from celerity.types.websocket import (
    WebSocketEventType,
    WebSocketMessage,
    WebSocketMessageType,
    WebSocketRequestContext,
)

if TYPE_CHECKING:
    from celerity.handlers.guard_pipeline import GuardResult as PipelineGuardResult


# ---------------------------------------------------------------------------
# PyO3 -> SDK
# ---------------------------------------------------------------------------


def flatten_multi_value_record(
    record: dict[str, list[str]],
) -> dict[str, str | list[str]]:
    """Flatten multi-value records: single-element lists become plain strings."""
    result: dict[str, str | list[str]] = {}
    for key, values in record.items():
        result[key] = values[0] if len(values) == 1 else values
    return result


def map_runtime_request(
    py_request: RuntimeRequest,
    py_context: RuntimeRequestContext,
) -> HttpRequest:
    """Map PyO3 ``Request`` + ``RequestContext`` -> SDK ``HttpRequest``."""
    return HttpRequest(
        method=py_request.method.upper(),
        path=py_request.path,
        path_params=py_request.path_params or {},
        query=flatten_multi_value_record(py_request.query or {}),
        headers=flatten_multi_value_record(py_request.headers or {}),
        cookies=py_request.cookies or {},
        text_body=py_request.text_body,
        binary_body=py_request.binary_body,
        content_type=py_request.content_type or None,
        request_id=py_context.request_id,
        request_time=str(py_context.request_time) if py_context.request_time else "",
        auth=py_context.auth,
        client_ip=py_context.client_ip or None,
        trace_context=py_context.trace_context,
        user_agent=py_request.user_agent or None,
        matched_route=py_context.matched_route,
    )


def map_runtime_websocket_message(py_msg: RuntimeWebSocketMsg) -> WebSocketMessage:
    """Map PyO3 ``WebSocketMessageInfo`` -> SDK ``WebSocketMessage``."""
    ws_request_context: WebSocketRequestContext | None = None
    if py_msg.request_context is not None:
        rc = py_msg.request_context
        ws_request_context = WebSocketRequestContext(
            request_id=rc.request_id,
            request_time=float(rc.request_time.timestamp()) if rc.request_time else 0.0,
            path=rc.path,
            protocol_version=str(rc.protocol_version),
            headers=flatten_multi_value_record(rc.headers or {}),
            user_agent=rc.user_agent_header,
            client_ip=rc.client_ip,
            query=flatten_multi_value_record(rc.query or {}),
            cookies=rc.cookies or {},
            auth=getattr(rc, "auth", None),
            trace_context=rc.trace_context,
        )

    return WebSocketMessage(
        message_type=WebSocketMessageType(str(py_msg.type).lower()),
        event_type=WebSocketEventType(str(py_msg.event_type).lower()),
        connection_id=py_msg.connection_id,
        message_id=py_msg.message_id,
        json_body=py_msg.json_body,
        binary_body=py_msg.binary_body,
        request_context=ws_request_context,
        trace_context=py_msg.trace_context,
    )


def map_runtime_consumer_event(py_event: RuntimeConsumerEvent) -> ConsumerEventInput:
    """Map PyO3 ``ConsumerEventInput`` -> SDK ``ConsumerEventInput``."""
    messages = [
        ConsumerMessage(
            message_id=msg.message_id,
            body=msg.body,
            source=msg.source,
            source_type=getattr(msg, "source_type", None),
            source_name=getattr(msg, "source_name", None),
            event_type=getattr(msg, "event_type", None),
            message_attributes=msg.message_attributes,
            vendor=msg.vendor,
        )
        for msg in py_event.messages
    ]
    return ConsumerEventInput(
        handler_tag=py_event.handler_tag,
        messages=messages,
        vendor=py_event.vendor,
        trace_context=py_event.trace_context,
    )


def map_runtime_schedule_event(py_event: RuntimeScheduleEvent) -> ScheduleEventInput:
    """Map PyO3 ``ScheduleEventInput`` -> SDK ``ScheduleEventInput``."""
    return ScheduleEventInput(
        handler_tag=py_event.handler_tag,
        schedule_id=py_event.schedule_id,
        message_id=py_event.message_id,
        schedule=py_event.schedule,
        input=py_event.input,
        vendor=py_event.vendor,
        trace_context=py_event.trace_context,
    )


def map_runtime_guard_input(py_input: RuntimeGuardInput) -> GuardInput:
    """Map PyO3 ``GuardInput`` -> SDK ``GuardInput``."""
    req = py_input.request
    return GuardInput(
        token=py_input.token,
        method=req.method,
        path=req.path,
        headers=flatten_multi_value_record(req.headers or {}),
        query=flatten_multi_value_record(req.query or {}),
        cookies=req.cookies or {},
        body=req.body,
        request_id=req.request_id,
        client_ip=req.client_ip or "",
        auth=py_input.auth or {},
        handler_name=py_input.handler_name,
    )


# ---------------------------------------------------------------------------
# SDK -> PyO3
# ---------------------------------------------------------------------------


def map_to_runtime_response(sdk_response: HttpResponse) -> RuntimeResponse:
    """Map SDK ``HttpResponse`` -> PyO3 ``Response``."""
    builder = ResponseBuilder()
    builder.set_status(sdk_response.status)
    if sdk_response.headers:
        builder.set_headers(sdk_response.headers)
    if sdk_response.body:
        builder.set_text_body(sdk_response.body)
    if sdk_response.binary_body:
        builder.set_binary_body(sdk_response.binary_body)
    return builder.build()


def map_to_runtime_event_result(sdk_result: EventResult) -> RuntimeEventResult:
    """Map SDK ``EventResult`` -> PyO3 ``EventResult``."""
    failures: list[RuntimeFailure] | None = None
    if sdk_result.failures:
        failures = [
            RuntimeFailure(
                message_id=f.message_id,
                error_message=f.error_message,
            )
            for f in sdk_result.failures
        ]
    return RuntimeEventResult(
        success=sdk_result.success,
        failures=failures,
        error_message=sdk_result.error_message,
    )


def map_to_runtime_guard_result(
    pipeline_result: PipelineGuardResult,
) -> RuntimeGuardResult:
    """Map guard pipeline ``GuardResult`` -> PyO3 ``GuardResult``."""
    if pipeline_result.allowed:
        return RuntimeGuardResult(status="allowed", auth=pipeline_result.auth)

    status_code = pipeline_result.status_code or 403
    status = "unauthorised" if status_code == 401 else "forbidden"
    return RuntimeGuardResult(status=status, message=pipeline_result.message)
