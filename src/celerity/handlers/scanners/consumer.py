"""Consumer handler scanner."""

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
from celerity.metadata.keys import CONSUMER, CONSUMER_HANDLER, get_metadata
from celerity.types.handler import ResolvedConsumerHandler

if TYPE_CHECKING:
    from celerity.bootstrap.module_graph import ModuleGraph
    from celerity.di.container import Container
    from celerity.handlers.registry import HandlerRegistry

logger = logging.getLogger("celerity.scanner.consumer")


async def scan_consumer_handlers(
    graph: ModuleGraph,
    container: Container,
    registry: HandlerRegistry,
) -> None:
    """Scan ``@consumer`` classes for ``@message_handler`` methods.

    Args:
        graph: The built module graph.
        container: The DI container for resolving controller instances.
        registry: The handler registry to register handlers in.
    """
    for node in graph.values():
        for controller_class in node.controllers:
            consumer_meta = get_metadata(controller_class, CONSUMER)
            if consumer_meta is None:
                continue

            source = consumer_meta.get("source", "") if isinstance(consumer_meta, dict) else ""

            for method_name in get_method_names(controller_class):
                method_meta = get_method_metadata(controller_class, method_name)
                handler_meta = method_meta.get(CONSUMER_HANDLER)
                if handler_meta is None:
                    continue

                route = handler_meta.get("route", "")
                handler_tag = f"{source}:{route}" if route else source

                logger.debug(
                    "scan: tag=%s (%s.%s)", handler_tag, controller_class.__name__, method_name
                )
                handler = ResolvedConsumerHandler(
                    handler_fn=getattr(controller_class, method_name),
                    controller_class=controller_class,
                    handler_tag=handler_tag,
                    layers=collect_layers(controller_class, method_meta),
                    param_metadata=extract_param_metadata(getattr(controller_class, method_name)),
                    custom_metadata=collect_custom_metadata(controller_class, method_meta),
                )
                registry.register(handler)

        for fn_handler in node.function_handlers:
            if fn_handler.type == "consumer":
                consumer_handler = ResolvedConsumerHandler(
                    handler_fn=fn_handler.handler,
                    is_function_handler=True,
                    id=fn_handler.id,
                    handler_tag=fn_handler.metadata.get("handler_tag", ""),
                    custom_metadata=fn_handler.metadata,
                )
                registry.register(consumer_handler)
