"""Serialize scanned module metadata to Handler Manifest JSON v1.0.0.

Produces canonical annotation keys that match the Celerity CLI's manifest schema:

- ``celerity.handler.http`` / ``.method`` / ``.path``
- ``celerity.handler.websocket`` / ``.route`` / ``.eventType``
- ``celerity.handler.consumer`` / ``.source`` / ``.route``
- ``celerity.handler.schedule`` / ``.source`` / ``.expression``
- ``celerity.handler.custom`` / ``.name``
- ``celerity.handler.guard.custom`` / ``.protectedBy``
- ``celerity.handler.public``
- ``celerity.handler.metadata.<key>``
- ``celerity.handler.resource.ref``
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from celerity.cli.identity import (
    derive_class_handler_function,
    derive_class_handler_name,
    derive_class_resource_name,
    derive_code_location,
    derive_function_handler_function,
    derive_function_resource_name,
)
from celerity.cli.types import (
    ClassHandlerEntry,
    DependencyGraph,
    DependencyNode,
    FunctionHandlerEntry,
    GuardHandlerEntry,
    HandlerManifest,
    HandlerSpec,
)
from celerity.common.path_utils import join_handler_path
from celerity.metadata.keys import (
    CONSUMER_HANDLER,
    CUSTOM_METADATA,
    GUARD_PROTECTEDBY,
    HTTP_METHOD,
    INVOKE,
    PUBLIC,
    ROUTE_PATH,
    SCHEDULE_HANDLER,
    USE_RESOURCE,
    WEBSOCKET_EVENT,
)

if TYPE_CHECKING:
    from .metadata_app import (
        ScannedClassHandler,
        ScannedGuardHandler,
        ScannedMethod,
        ScannedModule,
    )


def serialize_manifest(
    scanned: ScannedModule,
    source_file: str,
    *,
    project_root: str,
) -> HandlerManifest:
    """Convert a ``ScannedModule`` to a ``HandlerManifest``.

    Args:
        scanned: The scanned module metadata.
        source_file: The path to the user's app module file.
        project_root: The project root directory for relative paths.

    Returns:
        A ``HandlerManifest`` ready for JSON serialization.
    """
    handlers: list[ClassHandlerEntry] = []
    function_handlers: list[FunctionHandlerEntry] = []
    guard_handlers: list[GuardHandlerEntry] = []

    for class_handler in scanned.class_handlers:
        entries = _serialize_class_handlers(class_handler, source_file, project_root)
        handlers.extend(entries)

    for fn_handler in scanned.function_handlers:
        fn_entry = _serialize_function_handler(fn_handler, source_file, project_root)
        if fn_entry:
            function_handlers.append(fn_entry)

    for guard in scanned.guard_handlers:
        guard_entry = _serialize_guard(guard, source_file, project_root)
        if guard_entry:
            guard_handlers.append(guard_entry)

    return HandlerManifest(
        handlers=handlers,
        function_handlers=function_handlers,
        guard_handlers=guard_handlers,
        dependency_graph=_serialize_dependency_graph(scanned),
    )


# ---------------------------------------------------------------------------
# Class handler serialization
# ---------------------------------------------------------------------------


def _serialize_class_handlers(
    handler: ScannedClassHandler,
    source_file: str,
    project_root: str,
) -> list[ClassHandlerEntry]:
    """Serialize all methods of a scanned class handler."""
    entries: list[ClassHandlerEntry] = []

    for method in handler.methods:
        annotations = _build_method_annotations(method, handler)
        _append_shared_annotations(annotations, handler, method)

        entries.append(
            ClassHandlerEntry(
                resource_name=derive_class_resource_name(handler.class_name, method.method_name),
                class_name=handler.class_name,
                method_name=method.method_name,
                source_file=source_file,
                handler_type=method.handler_type,
                annotations=annotations,
                spec=HandlerSpec(
                    handler_name=derive_class_handler_name(
                        handler.class_name,
                        method.method_name,
                    ),
                    code_location=derive_code_location(source_file, project_root),
                    handler=derive_class_handler_function(
                        source_file,
                        handler.class_name,
                        method.method_name,
                    ),
                ),
            )
        )

    return entries


def _build_method_annotations(
    method: ScannedMethod,
    handler: ScannedClassHandler,
) -> dict[str, str | list[str] | bool]:
    """Build type-specific annotations for a handler method."""
    meta = method.metadata

    if method.handler_type == "http":
        return _build_http_annotations(meta, handler.controller_metadata)

    if method.handler_type == "websocket":
        return _build_websocket_annotations(meta)

    if method.handler_type == "consumer":
        return _build_consumer_annotations(meta, handler.controller_metadata)

    if method.handler_type == "schedule":
        return _build_schedule_annotations(meta)

    if method.handler_type == "custom":
        return _build_custom_annotations(meta)

    return {}


def _build_http_annotations(
    meta: dict[str, Any],
    controller_meta: dict[str, Any],
) -> dict[str, str | list[str] | bool]:
    prefix = controller_meta.get("prefix", "")
    route_path = meta.get(ROUTE_PATH, "/")
    full_path = join_handler_path(prefix, route_path)

    return {
        "celerity.handler.http": True,
        "celerity.handler.http.method": meta.get(HTTP_METHOD, "GET"),
        "celerity.handler.http.path": full_path,
    }


def _build_websocket_annotations(
    meta: dict[str, Any],
) -> dict[str, str | list[str] | bool]:
    ws_event = meta.get(WEBSOCKET_EVENT, {})
    annotations: dict[str, str | list[str] | bool] = {
        "celerity.handler.websocket": True,
    }
    if isinstance(ws_event, dict):
        if ws_event.get("route"):
            annotations["celerity.handler.websocket.route"] = ws_event["route"]
        if ws_event.get("event_type"):
            annotations["celerity.handler.websocket.eventType"] = ws_event["event_type"]
    return annotations


def _build_consumer_annotations(
    meta: dict[str, Any],
    controller_meta: dict[str, Any],
) -> dict[str, str | list[str] | bool]:
    annotations: dict[str, str | list[str] | bool] = {
        "celerity.handler.consumer": True,
    }
    source = controller_meta.get("source")
    if source:
        annotations["celerity.handler.consumer.source"] = source

    consumer_handler = meta.get(CONSUMER_HANDLER, {})
    if isinstance(consumer_handler, dict) and consumer_handler.get("route"):
        annotations["celerity.handler.consumer.route"] = consumer_handler["route"]

    return annotations


def _build_schedule_annotations(
    meta: dict[str, Any],
) -> dict[str, str | list[str] | bool]:
    schedule_meta = meta.get(SCHEDULE_HANDLER, {})
    annotations: dict[str, str | list[str] | bool] = {
        "celerity.handler.schedule": True,
    }
    if isinstance(schedule_meta, dict):
        if schedule_meta.get("source"):
            annotations["celerity.handler.schedule.source"] = schedule_meta["source"]
        if schedule_meta.get("schedule"):
            annotations["celerity.handler.schedule.expression"] = schedule_meta["schedule"]
    elif isinstance(schedule_meta, str):
        annotations["celerity.handler.schedule.expression"] = schedule_meta
    return annotations


def _build_custom_annotations(
    meta: dict[str, Any],
) -> dict[str, str | list[str] | bool]:
    invoke_meta = meta.get(INVOKE, {})
    annotations: dict[str, str | list[str] | bool] = {
        "celerity.handler.custom": True,
    }
    if isinstance(invoke_meta, dict) and invoke_meta.get("name"):
        annotations["celerity.handler.custom.name"] = invoke_meta["name"]
    elif isinstance(invoke_meta, str):
        annotations["celerity.handler.custom.name"] = invoke_meta
    return annotations


# ---------------------------------------------------------------------------
# Shared annotation helpers
# ---------------------------------------------------------------------------


def _append_shared_annotations(
    annotations: dict[str, str | list[str] | bool],
    handler: ScannedClassHandler,
    method: ScannedMethod,
) -> None:
    """Append guard, public, custom metadata, and resource ref annotations."""
    meta = method.metadata

    # Guards: merge class-level + method-level.
    method_guards: list[str] = meta.get(GUARD_PROTECTEDBY, [])
    all_guards = [*handler.class_protected_by, *method_guards]
    if all_guards:
        annotations["celerity.handler.guard.protectedBy"] = all_guards

    # Public.
    if meta.get(PUBLIC):
        annotations["celerity.handler.public"] = True

    # Custom metadata: merge class-level + method-level.
    method_custom: dict[str, Any] = meta.get(CUSTOM_METADATA, {})
    all_custom = {**handler.class_custom_metadata, **method_custom}
    for key, value in all_custom.items():
        if value is not None:
            annotations[f"celerity.handler.metadata.{key}"] = _serialize_annotation_value(value)

    # Resource refs: merge + deduplicate.
    method_resources: list[str] = meta.get(USE_RESOURCE, [])
    all_resources = list(dict.fromkeys([*handler.class_resource_refs, *method_resources]))
    if all_resources:
        annotations["celerity.handler.resource.ref"] = all_resources


# ---------------------------------------------------------------------------
# Function handler serialization
# ---------------------------------------------------------------------------


def _serialize_function_handler(
    fn_handler: Any,
    source_file: str,
    project_root: str,
) -> FunctionHandlerEntry | None:
    supported = {"http", "websocket", "consumer", "schedule", "custom"}
    if fn_handler.type not in supported:
        return None

    meta = fn_handler.metadata
    export_name = meta.get("handler_name", meta.get("name", "handler"))
    custom_metadata: dict[str, Any] = meta.get("custom_metadata", {})

    annotations: dict[str, str | list[str] | bool] = {}
    _build_function_type_annotations(annotations, fn_handler.type, meta)

    for key, value in custom_metadata.items():
        if value is not None:
            annotations[f"celerity.handler.metadata.{key}"] = _serialize_annotation_value(value)

    return FunctionHandlerEntry(
        resource_name=derive_function_resource_name(export_name),
        export_name=export_name,
        source_file=source_file,
        handler_type=fn_handler.type,
        annotations=annotations if annotations else None,
        spec=HandlerSpec(
            handler_name=export_name,
            code_location=derive_code_location(source_file, project_root),
            handler=derive_function_handler_function(source_file, export_name),
        ),
    )


def _build_function_type_annotations(
    annotations: dict[str, str | list[str] | bool],
    handler_type: str,
    meta: dict[str, Any],
) -> None:
    if handler_type == "http":
        path = meta.get("path")
        method = meta.get("method")
        if path is not None and method is not None:
            annotations["celerity.handler.http"] = True
            annotations["celerity.handler.http.method"] = method
            annotations["celerity.handler.http.path"] = path
    elif handler_type == "websocket":
        annotations["celerity.handler.websocket"] = True
        route = meta.get("route")
        if route:
            annotations["celerity.handler.websocket.route"] = route
    elif handler_type == "consumer":
        annotations["celerity.handler.consumer"] = True
        route = meta.get("route")
        if route:
            annotations["celerity.handler.consumer.route"] = route
    elif handler_type == "schedule":
        annotations["celerity.handler.schedule"] = True
        source = meta.get("source")
        if source:
            annotations["celerity.handler.schedule.source"] = source
        schedule = meta.get("schedule")
        if schedule:
            annotations["celerity.handler.schedule.expression"] = schedule
    elif handler_type == "custom":
        annotations["celerity.handler.custom"] = True
        name = meta.get("name")
        if name:
            annotations["celerity.handler.custom.name"] = name


# ---------------------------------------------------------------------------
# Guard serialization
# ---------------------------------------------------------------------------


def _serialize_guard(
    guard: ScannedGuardHandler,
    source_file: str,
    project_root: str,
) -> GuardHandlerEntry | None:
    annotations: dict[str, str | list[str] | bool] = {
        "celerity.handler.guard.custom": guard.guard_name,
    }

    for key, value in guard.custom_metadata.items():
        if value is not None:
            annotations[f"celerity.handler.metadata.{key}"] = _serialize_annotation_value(value)

    if guard.guard_type == "class":
        class_name = guard.class_name or ""
        method_name = "validate"
        return GuardHandlerEntry(
            resource_name=derive_class_resource_name(class_name, method_name),
            guard_name=guard.guard_name,
            source_file=source_file,
            guard_type="class",
            class_name=class_name,
            annotations=annotations,
            spec=HandlerSpec(
                handler_name=derive_class_handler_name(class_name, method_name),
                code_location=derive_code_location(source_file, project_root),
                handler=derive_class_handler_function(source_file, class_name, method_name),
            ),
        )

    export_name = guard.export_name or guard.guard_name
    return GuardHandlerEntry(
        resource_name=derive_function_resource_name(export_name),
        guard_name=guard.guard_name,
        source_file=source_file,
        guard_type="function",
        export_name=export_name,
        annotations=annotations,
        spec=HandlerSpec(
            handler_name=export_name,
            code_location=derive_code_location(source_file, project_root),
            handler=derive_function_handler_function(source_file, export_name),
        ),
    )


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _serialize_annotation_value(value: Any) -> str | list[str] | bool:
    """Serialize an annotation value for the manifest."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value
    if isinstance(value, list) and all(isinstance(v, str) for v in value):
        return value
    return json.dumps(value)


def _serialize_dependency_graph(scanned: ScannedModule) -> DependencyGraph:
    """Serialize the provider dependency graph."""
    nodes: list[DependencyNode] = []
    for provider in scanned.providers:
        nodes.append(
            DependencyNode(
                token=_serialize_token(provider.token),
                token_type=_token_type(provider.token),
                provider_type=provider.provider_type,
                dependencies=[_serialize_token(d) for d in provider.dependencies],
            )
        )
    return DependencyGraph(nodes=nodes)


def _serialize_token(token: Any) -> str:
    """Convert a DI token to a string for the manifest."""
    if isinstance(token, type):
        return token.__name__
    if isinstance(token, str):
        return token
    return str(token)


def _token_type(token: Any) -> str:
    """Determine the token type for the manifest."""
    if isinstance(token, type):
        return "class"
    if isinstance(token, str):
        return "string"
    return "string"
