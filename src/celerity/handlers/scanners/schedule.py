"""Schedule handler scanner."""

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
from celerity.metadata.keys import SCHEDULE_HANDLER
from celerity.types.handler import ResolvedScheduleHandler

if TYPE_CHECKING:
    from celerity.bootstrap.module_graph import ModuleGraph
    from celerity.di.container import Container
    from celerity.handlers.registry import HandlerRegistry

logger = logging.getLogger("celerity.scanner.schedule")


async def scan_schedule_handlers(
    graph: ModuleGraph,
    container: Container,
    registry: HandlerRegistry,
) -> None:
    """Scan all controller classes for ``@schedule_handler`` methods.

    Schedule handlers are cross-cutting: they can appear on any
    controller type (``@controller``, ``@consumer``, etc.).

    Args:
        graph: The built module graph.
        container: The DI container for resolving controller instances.
        registry: The handler registry to register handlers in.
    """
    for node in graph.values():
        for controller_class in node.controllers:
            for method_name in get_method_names(controller_class):
                method_meta = get_method_metadata(controller_class, method_name)
                schedule_meta = method_meta.get(SCHEDULE_HANDLER)
                if schedule_meta is None:
                    continue

                source = schedule_meta.get("source", "")
                schedule_expr = schedule_meta.get("schedule", "")
                handler_tag = source or schedule_expr or method_name

                logger.debug(
                    "scan: tag=%s (%s.%s)", handler_tag, controller_class.__name__, method_name
                )
                handler = ResolvedScheduleHandler(
                    handler_fn=getattr(controller_class, method_name),
                    controller_class=controller_class,
                    handler_tag=handler_tag,
                    layers=collect_layers(controller_class, method_meta),
                    param_metadata=extract_param_metadata(getattr(controller_class, method_name)),
                    custom_metadata=collect_custom_metadata(controller_class, method_meta),
                )
                registry.register(handler)

        for fn_handler in node.function_handlers:
            if fn_handler.type == "schedule":
                schedule_handler = ResolvedScheduleHandler(
                    handler_fn=fn_handler.handler,
                    is_function_handler=True,
                    id=fn_handler.id,
                    handler_tag=fn_handler.metadata.get("handler_tag", ""),
                    custom_metadata=fn_handler.metadata,
                )
                registry.register(schedule_handler)
