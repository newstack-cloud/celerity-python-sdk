"""Walk the module graph collecting decorator metadata WITHOUT constructing instances.

Reads @module, @controller,
@guard, @consumer, @ws_controller metadata to produce handler entries
for the manifest. No DI resolution or handler execution occurs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from celerity.bootstrap.module_graph import build_module_graph
from celerity.di.dependency_tokens import get_class_dependency_tokens
from celerity.metadata.keys import (
    _META_ATTR,
    CONSUMER,
    CONSUMER_HANDLER,
    CONTROLLER,
    CUSTOM_METADATA,
    GUARD_CUSTOM,
    GUARD_PROTECTEDBY,
    HTTP_METHOD,
    INVOKE,
    SCHEDULE_HANDLER,
    USE_RESOURCE,
    WEBSOCKET_CONTROLLER,
    WEBSOCKET_EVENT,
    get_metadata,
)
from celerity.types.module import GuardDefinition

logger = logging.getLogger("celerity.cli")


@dataclass
class ScannedMethod:
    """A single handler method discovered from decorator metadata."""

    method_name: str
    handler_type: str
    metadata: dict[str, Any]


@dataclass
class ScannedClassHandler:
    """A controller class with its discovered handler methods."""

    class_name: str
    module_path: str
    controller_type: str
    controller_metadata: dict[str, Any]
    methods: list[ScannedMethod] = field(default_factory=list)
    class_protected_by: list[str] = field(default_factory=list)
    class_custom_metadata: dict[str, Any] = field(default_factory=dict)
    class_resource_refs: list[str] = field(default_factory=list)


@dataclass
class ScannedFunctionHandler:
    """A function-based handler from a module definition."""

    type: str
    metadata: dict[str, Any]
    id: str | None = None


@dataclass
class ScannedGuardHandler:
    """A guard discovered from a @guard decorator or function guard."""

    guard_name: str
    source: str
    guard_type: str
    class_name: str | None = None
    export_name: str | None = None
    custom_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ScannedProvider:
    """A provider with its resolved dependency tokens."""

    token: Any
    provider_type: str
    dependencies: list[Any] = field(default_factory=list)


@dataclass
class ScannedModule:
    """Result of scanning a module graph for metadata."""

    class_handlers: list[ScannedClassHandler] = field(default_factory=list)
    function_handlers: list[ScannedFunctionHandler] = field(default_factory=list)
    guard_handlers: list[ScannedGuardHandler] = field(default_factory=list)
    providers: list[ScannedProvider] = field(default_factory=list)


def build_scanned_module(root_module: type) -> ScannedModule:
    """Walk the module graph and collect all handler metadata.

    Reuses ``build_module_graph`` from the bootstrap package which handles
    circular import detection and recursive module walking.

    Does NOT:
    - Construct any class instances
    - Resolve DI dependencies
    - Execute any handler or guard logic

    DOES:
    - Import all referenced modules (decorators execute at import time)
    - Read @module metadata to walk the graph
    - Read @controller, @guard, @consumer, @ws_controller class metadata
    - Read @get, @post, @on_connect, @message_handler, etc. method metadata
    - Read @protected_by, @use_layer, @set_handler_metadata, @use_resource metadata
    """
    graph = build_module_graph(root_module)

    class_handlers: list[ScannedClassHandler] = []
    function_handlers: list[ScannedFunctionHandler] = []
    guard_handlers: list[ScannedGuardHandler] = []
    providers: list[ScannedProvider] = []
    seen_tokens: set[Any] = set()

    for node in graph.values():
        for provider in node.providers:
            _scan_provider(provider, providers, seen_tokens)

        for controller_class in node.controllers:
            scanned = _scan_controller_class(controller_class)
            if scanned:
                class_handlers.append(scanned)
            _scan_class_provider(controller_class, providers, seen_tokens)

        for guard in node.guards:
            if isinstance(guard, type):
                scanned_guard = _scan_guard_class(guard)
                if scanned_guard:
                    guard_handlers.append(scanned_guard)
                _scan_class_provider(guard, providers, seen_tokens)
            elif isinstance(guard, GuardDefinition):
                guard_handlers.append(
                    ScannedGuardHandler(
                        guard_name=guard.name,
                        source="function",
                        guard_type="function",
                        export_name=guard.name,
                        custom_metadata=(guard.metadata or {}).get("customMetadata", {}),
                    )
                )

        for fn_handler in node.function_handlers:
            function_handlers.append(
                ScannedFunctionHandler(
                    type=fn_handler.type,
                    metadata=fn_handler.metadata,
                    id=fn_handler.id,
                )
            )

    logger.debug(
        "scanned: %d class handlers, %d function handlers, %d guards, %d providers",
        len(class_handlers),
        len(function_handlers),
        len(guard_handlers),
        len(providers),
    )

    return ScannedModule(class_handlers, function_handlers, guard_handlers, providers)


def _scan_controller_class(cls: type) -> ScannedClassHandler | None:
    """Scan a controller class for handler methods."""
    controller_type, controller_metadata = _extract_controller_type(cls)
    if controller_type is None:
        return None

    module_path = _get_module_path(cls)

    scanned = ScannedClassHandler(
        class_name=cls.__name__,
        module_path=module_path,
        controller_type=controller_type,
        controller_metadata=controller_metadata,
        class_protected_by=get_metadata(cls, GUARD_PROTECTEDBY) or [],
        class_custom_metadata=get_metadata(cls, CUSTOM_METADATA) or {},
        class_resource_refs=get_metadata(cls, USE_RESOURCE) or [],
    )

    for method_name in _get_method_names(cls):
        method_fn = getattr(cls, method_name, None)
        if method_fn is None:
            continue
        method_meta: dict[str, Any] = getattr(method_fn, _META_ATTR, {})
        if not method_meta:
            continue

        methods = _classify_method(method_name, method_meta, controller_type)
        scanned.methods.extend(methods)

    return scanned if scanned.methods else None


def _extract_controller_type(cls: type) -> tuple[str | None, dict[str, Any]]:
    """Determine the controller type from class metadata."""
    ctrl_meta = get_metadata(cls, CONTROLLER)
    if ctrl_meta is not None:
        return "http", ctrl_meta if isinstance(ctrl_meta, dict) else {}

    ws_meta = get_metadata(cls, WEBSOCKET_CONTROLLER)
    if ws_meta is not None:
        return "websocket", ws_meta if isinstance(ws_meta, dict) else {}

    consumer_meta = get_metadata(cls, CONSUMER)
    if consumer_meta is not None:
        return "consumer", consumer_meta if isinstance(consumer_meta, dict) else {}

    return None, {}


def _classify_method(
    method_name: str,
    method_meta: dict[str, Any],
    controller_type: str,
) -> list[ScannedMethod]:
    """Classify a method's handler type(s) from its metadata.

    A method can produce multiple entries: its primary handler type
    plus cross-cutting decorators like @schedule_handler and @invoke.
    """
    methods: list[ScannedMethod] = []

    # Primary handler type based on controller type.
    if controller_type == "http" and HTTP_METHOD in method_meta:
        methods.append(ScannedMethod(method_name, "http", method_meta))
    elif controller_type == "websocket" and WEBSOCKET_EVENT in method_meta:
        methods.append(ScannedMethod(method_name, "websocket", method_meta))
    elif controller_type == "consumer" and CONSUMER_HANDLER in method_meta:
        methods.append(ScannedMethod(method_name, "consumer", method_meta))

    # Cross-cutting: @schedule_handler on any controller type.
    if SCHEDULE_HANDLER in method_meta:
        methods.append(ScannedMethod(method_name, "schedule", method_meta))

    # Cross-cutting: @invoke on any controller type.
    if INVOKE in method_meta:
        methods.append(ScannedMethod(method_name, "custom", method_meta))

    return methods


def _scan_guard_class(cls: type) -> ScannedGuardHandler | None:
    """Scan a guard class for its metadata."""
    guard_name = get_metadata(cls, GUARD_CUSTOM)
    if guard_name is None:
        return None

    return ScannedGuardHandler(
        guard_name=guard_name,
        source="class",
        guard_type="class",
        class_name=cls.__name__,
        custom_metadata=get_metadata(cls, CUSTOM_METADATA) or {},
    )


def _get_module_path(cls: type) -> str:
    """Get the source module path of a class."""
    module = getattr(cls, "__module__", "")
    return module


def _get_method_names(cls: type) -> list[str]:
    """Get user-defined method names, excluding dunder methods."""
    return [
        name for name in dir(cls) if not name.startswith("_") and callable(getattr(cls, name, None))
    ]


def _scan_provider(
    provider: Any,
    providers: list[ScannedProvider],
    seen: set[Any],
) -> None:
    """Scan a provider entry from the module graph."""
    if isinstance(provider, type):
        _scan_class_provider(provider, providers, seen)
    else:
        # Structured provider (ClassProvider, FactoryProvider, ValueProvider).
        token = getattr(provider, "provide", provider)
        if token in seen:
            return
        seen.add(token)

        if hasattr(provider, "use_class"):
            providers.append(
                ScannedProvider(
                    token=token,
                    provider_type="class",
                    dependencies=get_class_dependency_tokens(provider.use_class),
                )
            )
        elif hasattr(provider, "use_factory"):
            inject = getattr(provider, "inject", None) or []
            providers.append(
                ScannedProvider(
                    token=token,
                    provider_type="factory",
                    dependencies=list(inject),
                )
            )
        elif hasattr(provider, "use_value"):
            providers.append(
                ScannedProvider(
                    token=token,
                    provider_type="value",
                    dependencies=[],
                )
            )


def _scan_class_provider(
    cls: type,
    providers: list[ScannedProvider],
    seen: set[Any],
) -> None:
    """Scan a class as a provider, extracting its constructor dependencies."""
    if cls in seen:
        return
    seen.add(cls)
    providers.append(
        ScannedProvider(
            token=cls,
            provider_type="class",
            dependencies=get_class_dependency_tokens(cls),
        )
    )
