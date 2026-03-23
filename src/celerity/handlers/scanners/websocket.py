"""WebSocket handler scanner."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from celerity.handlers.param_extractor import build_validation_schemas, extract_param_metadata
from celerity.handlers.scanners._utils import (
    collect_custom_metadata,
    collect_layers,
    collect_protected_by,
    get_method_metadata,
    get_method_names,
)
from celerity.layers.validate import validate
from celerity.metadata.keys import WEBSOCKET_CONTROLLER, WEBSOCKET_EVENT, get_metadata
from celerity.types.handler import ResolvedWebSocketHandler

if TYPE_CHECKING:
    from celerity.bootstrap.module_graph import ModuleGraph
    from celerity.di.container import Container
    from celerity.handlers.registry import HandlerRegistry

logger = logging.getLogger("celerity.scanner.ws")


async def scan_websocket_handlers(
    graph: ModuleGraph,
    container: Container,
    registry: HandlerRegistry,
) -> None:
    """Scan ``@ws_controller`` classes for WebSocket handlers.

    Discovers ``@on_connect``, ``@on_message``, and ``@on_disconnect``
    methods and registers them as ``ResolvedWebSocketHandler`` entries.

    Args:
        graph: The built module graph.
        container: The DI container for resolving controller instances.
        registry: The handler registry to register handlers in.
    """
    for node in graph.values():
        for controller_class in node.controllers:
            if not get_metadata(controller_class, WEBSOCKET_CONTROLLER):
                continue

            for method_name in get_method_names(controller_class):
                method_meta = get_method_metadata(controller_class, method_name)
                ws_event = method_meta.get(WEBSOCKET_EVENT)
                if ws_event is None:
                    continue

                logger.debug(
                    "scan: %s (%s.%s)",
                    ws_event.get("route"),
                    controller_class.__name__,
                    method_name,
                )
                param_metadata = extract_param_metadata(getattr(controller_class, method_name))
                layers = collect_layers(controller_class, method_meta)
                validation_schemas = build_validation_schemas(param_metadata)
                if validation_schemas:
                    layers.insert(0, validate(validation_schemas))

                handler = ResolvedWebSocketHandler(
                    handler_fn=getattr(controller_class, method_name),
                    controller_class=controller_class,
                    route=ws_event.get("route", "$default"),
                    protected_by=collect_protected_by(controller_class, method_meta),
                    layers=layers,
                    param_metadata=param_metadata,
                    custom_metadata=collect_custom_metadata(controller_class, method_meta),
                )
                registry.register(handler)
