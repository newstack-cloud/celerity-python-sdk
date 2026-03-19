"""HTTP handler and guard scanners."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from celerity.common.path_utils import join_handler_path
from celerity.handlers.param_extractor import build_validation_schemas, extract_param_metadata
from celerity.handlers.scanners._utils import (
    collect_custom_metadata,
    collect_layers,
    collect_protected_by,
    get_method_metadata,
    get_method_names,
)
from celerity.layers.validate import validate
from celerity.metadata.keys import (
    CONTROLLER,
    GUARD_CUSTOM,
    HTTP_METHOD,
    PUBLIC,
    ROUTE_PATH,
    get_metadata,
)
from celerity.types.handler import ResolvedGuard, ResolvedHttpHandler

if TYPE_CHECKING:
    from celerity.bootstrap.module_graph import ModuleGraph
    from celerity.di.container import Container
    from celerity.handlers.registry import HandlerRegistry

logger = logging.getLogger("celerity.scanner.http")


async def scan_http_handlers(
    graph: ModuleGraph,
    container: Container,
    registry: HandlerRegistry,
) -> None:
    """Scan all controllers in the module graph for HTTP handlers.

    For each ``@controller`` class, resolves the instance from the DI
    container, inspects methods for ``@get``/``@post``/etc. metadata,
    builds full paths, and registers ``ResolvedHttpHandler`` entries.

    Args:
        graph: The built module graph.
        container: The DI container for resolving controller instances.
        registry: The handler registry to register handlers in.
    """
    for node in graph.values():
        for controller_class in node.controllers:
            ctrl_meta = get_metadata(controller_class, CONTROLLER)
            if ctrl_meta is None:
                continue

            instance = await container.resolve(controller_class)
            prefix = ctrl_meta.get("prefix", "") if isinstance(ctrl_meta, dict) else ""

            for method_name in get_method_names(controller_class):
                method_meta = get_method_metadata(controller_class, method_name)
                http_method = method_meta.get(HTTP_METHOD)
                if http_method is None:
                    continue

                route_path = method_meta.get(ROUTE_PATH, "/")
                full_path = join_handler_path(prefix, route_path)
                is_public = method_meta.get(PUBLIC, False)

                param_metadata = extract_param_metadata(getattr(controller_class, method_name))
                layers = collect_layers(controller_class, method_meta)
                validation_schemas = build_validation_schemas(param_metadata)
                if validation_schemas:
                    layers.insert(0, validate(validation_schemas))

                logger.debug(
                    "scan: %s %s (%s.%s)",
                    http_method,
                    full_path,
                    controller_class.__name__,
                    method_name,
                )
                handler = ResolvedHttpHandler(
                    handler_fn=getattr(instance, method_name),
                    handler_instance=instance,
                    controller_class=controller_class,
                    path=full_path,
                    method=http_method,
                    protected_by=collect_protected_by(controller_class, method_meta),
                    is_public=is_public,
                    layers=layers,
                    param_metadata=param_metadata,
                    custom_metadata=collect_custom_metadata(controller_class, method_meta),
                )
                registry.register(handler)

        for fn_handler in node.function_handlers:
            if fn_handler.type == "http":
                logger.debug(
                    "scan function: %s %s",
                    fn_handler.metadata.get("method"),
                    fn_handler.metadata.get("path"),
                )
                http_handler = ResolvedHttpHandler(
                    handler_fn=fn_handler.handler,
                    is_function_handler=True,
                    id=fn_handler.id,
                    path=fn_handler.metadata.get("path"),
                    method=fn_handler.metadata.get("method"),
                    custom_metadata=fn_handler.metadata,
                )
                registry.register(http_handler)


async def scan_http_guards(
    graph: ModuleGraph,
    container: Container,
    registry: HandlerRegistry,
) -> None:
    """Scan all guard classes in the module graph.

    For each ``@guard`` class, resolves the instance and finds the
    ``validate`` method for guard evaluation.

    Args:
        graph: The built module graph.
        container: The DI container for resolving guard instances.
        registry: The handler registry to register guards in.
    """
    for node in graph.values():
        for guard_entry in node.guards:
            if not isinstance(guard_entry, type):
                continue

            guard_name = get_metadata(guard_entry, GUARD_CUSTOM)
            if guard_name is None:
                continue

            logger.debug("scan guard: %s (name=%s)", guard_entry.__name__, guard_name)
            instance = await container.resolve(guard_entry)
            validate_fn = getattr(instance, "validate", None)
            if validate_fn is None:
                continue

            resolved = ResolvedGuard(
                name=guard_name,
                handler_fn=validate_fn,
                handler_instance=instance,
                guard_class=guard_entry,
                param_metadata=extract_param_metadata(
                    guard_entry.validate  # type: ignore[attr-defined]
                ),
            )
            registry.register_guard(resolved)
