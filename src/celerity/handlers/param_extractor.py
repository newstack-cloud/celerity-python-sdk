"""Parameter extraction from type hints and request context."""

from __future__ import annotations

import inspect
import json
from typing import TYPE_CHECKING, Any, get_args, get_origin, get_type_hints

from celerity.decorators.params import Key
from celerity.types.common import Schema
from celerity.types.handler import ParamMetadata

if TYPE_CHECKING:
    from celerity.types.context import BaseHandlerContext
    from celerity.types.handler import ResolvedHandlerBase


# Maps param types that support schema validation to their metadata keys.
_VALIDATED_METADATA_KEYS: dict[str, str] = {
    "body": "validated_body",
    "query": "validated_query",
    "param": "validated_params",
    "headers": "validated_headers",
    "messageBody": "validated_message_body",
}

# Maps param types to their validation schema dict keys.
_PARAM_TYPE_TO_SCHEMA_KEY: dict[str, str] = {
    "body": "body",
    "query": "query",
    "param": "params",
    "headers": "headers",
    "messageBody": "ws_message_body",
}

# Singular param types that auto-derive key from the Python parameter name.
_AUTO_KEY_TYPES: set[str] = {"param", "header", "cookie", "query_param"}


def _transform_key(param_type: str, name: str) -> str:
    """Transform a Python parameter name into the appropriate request key.

    For ``"header"`` types, converts ``snake_case`` to ``kebab-case``
    (e.g. ``content_type`` -> ``content-type``). All other types use the
    parameter name as-is.
    """
    if param_type == "header":
        return name.replace("_", "-")
    return name


def _unwrap_annotated(hint: Any) -> tuple[Any, str | None]:
    """Unwrap an ``Annotated`` type and extract a ``Key`` marker if present.

    Returns a tuple of ``(inner_hint, explicit_key)``.  If the hint is
    not ``Annotated``, returns ``(hint, None)``.
    """
    import typing

    if get_origin(hint) is typing.Annotated:
        args = get_args(hint)
        inner = args[0]
        for extra in args[1:]:
            if isinstance(extra, Key):
                return inner, extra.name
        return inner, None
    return hint, None


def extract_param_metadata(method: Any) -> list[ParamMetadata]:
    """Extract parameter injection metadata from a method's type hints.

    Inspects each parameter's type hint for a ``__celerity_param__``
    attribute set by the parameter injection types (``Body``, ``Query``,
    ``Param``, ``Auth``, etc.).

    When the type is generic (e.g. ``Body[CreateOrderInput]``), the inner
    type argument is checked for ``Schema`` compatibility (Pydantic
    ``model_validate``). If compatible, it is attached as the schema for
    validation layer use.

    Args:
        method: The handler method to inspect.

    Returns:
        A list of ``ParamMetadata`` entries for parameters that have
        injection markers.
    """
    try:
        hints = get_type_hints(method, include_extras=True)
    except Exception:
        return []

    sig = inspect.signature(method)
    params = list(sig.parameters.values())
    result: list[ParamMetadata] = []

    # Use a separate counter that excludes ``self`` so that indices
    # match the bound method's signature at invocation time.
    param_index = 0
    for param in params:
        if param.name == "self":
            continue
        hint = hints.get(param.name)
        if hint is None:
            param_index += 1
            continue

        # Unwrap Annotated[Param[str], Key("name")] to find the param
        # type and any explicit Key marker.
        inner_hint, explicit_key = _unwrap_annotated(hint)

        if hasattr(inner_hint, "__celerity_param__"):
            meta = inner_hint.__celerity_param__
            schema = meta.schema or _resolve_schema_from_args(inner_hint)
            # Explicit Key marker takes precedence, then auto-derive
            # from the Python parameter name for singular types.
            key = explicit_key or meta.key
            if key is None and meta.type in _AUTO_KEY_TYPES:
                key = _transform_key(meta.type, param.name)
            result.append(
                ParamMetadata(
                    index=param_index,
                    type=meta.type,
                    key=key,
                    schema=schema,
                )
            )
        param_index += 1

    return result


def _resolve_schema_from_args(hint: Any) -> Schema[Any] | None:
    """Extract a schema from the inner type argument of a generic param type.

    For ``Body[CreateOrderInput]``, the dynamically created type has
    ``__args__ = (CreateOrderInput,)``. If the inner type satisfies the
    ``Schema`` protocol (has ``model_validate``), it is returned.
    """
    args = getattr(hint, "__args__", None)
    if not args:
        return None
    inner = args[0]
    if isinstance(inner, type) and isinstance(inner, Schema):
        return inner
    return None


def build_validation_schemas(
    param_metadata: list[ParamMetadata],
) -> dict[str, Any] | None:
    """Build a validation schemas dict from param metadata.

    Inspects each param's schema and maps it to the appropriate
    validation key. Used by scanners to prepend a ``ValidationLayer``
    to handler layers.

    Args:
        param_metadata: The handler's extracted param metadata.

    Returns:
        A dict of ``{schema_key: schema}`` if any schemas are present,
        or ``None`` if no validation is needed.
    """
    schemas: dict[str, Any] = {}
    for meta in param_metadata:
        if not meta.schema:
            continue
        schema_key = _PARAM_TYPE_TO_SCHEMA_KEY.get(meta.type)
        if schema_key:
            schemas[schema_key] = meta.schema

    return schemas or None


def resolve_handler_params(
    handler: ResolvedHandlerBase,
    context: BaseHandlerContext,
) -> list[Any]:
    """Extract actual parameter values from the request context.

    Uses the handler's ``param_metadata`` to map each declared parameter
    to a concrete value extracted from the request.

    Args:
        handler: The resolved handler with param metadata.
        context: The handler context containing the request/event.

    Returns:
        An ordered list of parameter values matching the handler's
        parameter positions.
    """

    values: list[Any] = []

    for meta in handler.param_metadata:
        value = _extract_single_param(meta, context)
        values.append(value)

    return values


def _extract_single_param(meta: ParamMetadata, context: BaseHandlerContext) -> Any:
    """Extract a single parameter value based on its metadata type."""
    from celerity.types.context import (
        ConsumerHandlerContext,
        HttpHandlerContext,
        ScheduleHandlerContext,
        WebSocketHandlerContext,
    )

    param_type = meta.type

    if isinstance(context, HttpHandlerContext) and context.request is not None:
        # Check for pre-validated values from ValidationLayer first.
        validated = _get_validated(param_type, meta.key, context)
        if validated is not _SENTINEL:
            return validated

        request = context.request
        if param_type == "body":
            body = request.text_body
            if body:
                try:
                    return json.loads(body)
                except (json.JSONDecodeError, TypeError):
                    return body
            return body
        if param_type == "query":
            return request.query.get(meta.key) if meta.key else request.query
        if param_type == "param":
            return request.path_params.get(meta.key, "") if meta.key else request.path_params
        if param_type == "headers":
            return request.headers.get(meta.key) if meta.key else request.headers
        if param_type == "cookies":
            return request.cookies.get(meta.key, "") if meta.key else request.cookies
        if param_type == "header":
            return request.headers.get(meta.key) if meta.key else None
        if param_type == "cookie":
            return request.cookies.get(meta.key, "") if meta.key else None
        if param_type == "query_param":
            return request.query.get(meta.key) if meta.key else None
        if param_type == "auth":
            return request.auth
        if param_type == "token":
            auth_header = request.headers.get("authorization", "")
            if isinstance(auth_header, str) and auth_header.startswith("Bearer "):
                return auth_header[7:]
            return auth_header
        if param_type == "request":
            return request
        if param_type == "requestId":
            return request.request_id

    if isinstance(context, WebSocketHandlerContext) and context.message is not None:
        msg = context.message
        if param_type == "connectionId":
            return msg.connection_id
        if param_type == "messageBody":
            validated = context.metadata.get("validated_message_body")
            if validated is not None:
                return validated
            return msg.json_body
        if param_type == "messageId":
            return msg.message_id
        if param_type == "requestContext":
            return msg.request_context
        if param_type == "eventType":
            return msg.event_type

    if isinstance(context, ConsumerHandlerContext) and context.event is not None:
        event = context.event
        if param_type == "messages":
            return event.messages
        if param_type == "consumerEvent":
            return event
        if param_type == "consumerVendor":
            return event.vendor
        if param_type == "consumerTraceContext":
            return event.trace_context

    if isinstance(context, ScheduleHandlerContext) and context.event is not None:
        sched_event = context.event
        if param_type == "scheduleInput":
            return sched_event.input
        if param_type == "scheduleId":
            return sched_event.schedule_id
        if param_type == "scheduleExpression":
            return sched_event.schedule
        if param_type == "scheduleEventInput":
            return sched_event

    if param_type == "payload":
        return getattr(context, "payload", None)
    if param_type == "invokeContext":
        return context

    return None


_SENTINEL = object()


def _get_validated(param_type: str, key: str | None, context: Any) -> Any:
    """Check context metadata for a pre-validated value from ValidationLayer.

    Returns ``_SENTINEL`` if no validated value is found.
    """
    meta_key = _VALIDATED_METADATA_KEYS.get(param_type)
    if not meta_key:
        return _SENTINEL

    validated = context.metadata.get(meta_key)
    if validated is None:
        return _SENTINEL

    if key and isinstance(validated, dict):
        return validated.get(key)
    return validated
