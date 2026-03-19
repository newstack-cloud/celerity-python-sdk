"""Custom (invoke) handler scanner."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from celerity.handlers.param_extractor import extract_param_metadata
from celerity.handlers.scanners._utils import (
    collect_custom_metadata,
    collect_layers,
    get_method_metadata,
    get_method_names,
)
from celerity.metadata.keys import INVOKE
from celerity.types.handler import ResolvedCustomHandler

if TYPE_CHECKING:
    from celerity.bootstrap.module_graph import ModuleGraph
    from celerity.di.container import Container
    from celerity.handlers.registry import HandlerRegistry

logger = logging.getLogger("celerity.scanner.custom")


async def scan_custom_handlers(
    graph: ModuleGraph,
    container: Container,
    registry: HandlerRegistry,
) -> None:
    """Scan all controller classes for ``@invoke`` methods.

    Args:
        graph: The built module graph.
        container: The DI container for resolving controller instances.
        registry: The handler registry to register handlers in.
    """
    for node in graph.values():
        for controller_class in node.controllers:
            instance = await container.resolve(controller_class)

            for method_name in get_method_names(controller_class):
                method_meta = get_method_metadata(controller_class, method_name)
                invoke_meta = method_meta.get(INVOKE)
                if invoke_meta is None:
                    continue

                logger.debug(
                    "scan: name=%s (%s.%s)",
                    invoke_meta.get("name"),
                    controller_class.__name__,
                    method_name,
                )
                handler = ResolvedCustomHandler(
                    handler_fn=getattr(instance, method_name),
                    handler_instance=instance,
                    controller_class=controller_class,
                    name=invoke_meta.get("name", method_name),
                    layers=collect_layers(controller_class, method_meta),
                    param_metadata=extract_param_metadata(getattr(controller_class, method_name)),
                    custom_metadata=collect_custom_metadata(controller_class, method_meta),
                )
                registry.register(handler)

        for fn_handler in node.function_handlers:
            if fn_handler.type == "custom":
                custom_handler = ResolvedCustomHandler(
                    handler_fn=fn_handler.handler,
                    is_function_handler=True,
                    id=fn_handler.id,
                    name=fn_handler.metadata.get("name", ""),
                    custom_metadata=fn_handler.metadata,
                )
                registry.register(custom_handler)
