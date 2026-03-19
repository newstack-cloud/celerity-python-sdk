"""Application bootstrap entry point."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import celerity.common.debug as _  # noqa: F401
from celerity.bootstrap.module_graph import walk_module_graph
from celerity.di.container import Container
from celerity.handlers.registry import HandlerRegistry
from celerity.handlers.scanners.consumer import scan_consumer_handlers
from celerity.handlers.scanners.custom import scan_custom_handlers
from celerity.handlers.scanners.http import scan_http_guards, scan_http_handlers
from celerity.handlers.scanners.schedule import scan_schedule_handlers
from celerity.handlers.scanners.websocket import scan_websocket_handlers

if TYPE_CHECKING:
    from celerity.bootstrap.module_graph import ModuleGraph

logger = logging.getLogger("celerity.bootstrap")


async def bootstrap(
    root_module: type,
) -> tuple[Container, HandlerRegistry, ModuleGraph]:
    """Bootstrap the DI container and handler registry from a root module.

    Performs the full application bootstrap sequence:

    1. Create ``Container`` and ``HandlerRegistry``
    2. Walk the module graph (build + register providers)
    3. Scan HTTP handlers and guards
    4. Scan WebSocket handlers
    5. Scan consumer handlers
    6. Scan schedule handlers
    7. Scan custom handlers

    Args:
        root_module: The root ``@module``-decorated class.

    Returns:
        A tuple of ``(container, registry, graph)``.

    Example::

        @module(controllers=[OrderController], providers=[OrderService])
        class AppModule:
            pass

        container, registry, graph = await bootstrap(AppModule)
    """
    logger.debug("bootstrap: starting from %s", root_module.__name__)
    container = Container()
    registry = HandlerRegistry()

    graph = walk_module_graph(root_module, container)
    await scan_http_handlers(graph, container, registry)
    await scan_http_guards(graph, container, registry)
    await scan_websocket_handlers(graph, container, registry)
    await scan_consumer_handlers(graph, container, registry)
    await scan_schedule_handlers(graph, container, registry)
    await scan_custom_handlers(graph, container, registry)

    logger.debug(
        "bootstrap: complete — %d handlers, %d guards",
        len(registry.get_all_handlers()),
        len(registry.get_all_guards()),
    )
    return container, registry, graph
