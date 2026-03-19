"""Module graph builder and DI registrar."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from celerity.metadata.keys import MODULE, get_metadata

if TYPE_CHECKING:
    from celerity.di.container import Container
    from celerity.types.container import Provider
    from celerity.types.module import (
        FunctionHandlerDefinition,
        GuardDefinition,
        ModuleMetadata,
    )

logger = logging.getLogger("celerity.bootstrap")


@dataclass
class ModuleNode:
    """A single node in the module dependency graph."""

    module_class: type
    own_tokens: set[Any] = field(default_factory=set)
    exports: set[Any] = field(default_factory=set)
    imports: list[type] = field(default_factory=list)
    controllers: list[type] = field(default_factory=list)
    function_handlers: list[FunctionHandlerDefinition] = field(default_factory=list)
    guards: list[type | GuardDefinition] = field(default_factory=list)
    providers: list[type | Provider] = field(default_factory=list)
    layers: list[Any] = field(default_factory=list)


type ModuleGraph = dict[type, ModuleNode]


def build_module_graph(root_module: type) -> ModuleGraph:
    """Walk the module tree depth-first, collecting metadata.

    Args:
        root_module: The root ``@module``-decorated class.

    Returns:
        A dict mapping each module class to its ``ModuleNode``.

    Raises:
        RuntimeError: If a circular module import is detected.

    Example::

        @module(controllers=[OrderController], providers=[OrderService])
        class AppModule:
            pass

        graph = build_module_graph(AppModule)
    """
    logger.debug("build_module_graph: starting from %s", root_module.__name__)
    graph: ModuleGraph = {}
    resolving: set[type] = set()

    def walk(module_class: type, import_chain: list[type]) -> None:
        if module_class in graph:
            return

        if module_class in resolving:
            cycle = " -> ".join(m.__name__ for m in [*import_chain, module_class])
            raise RuntimeError(f"Circular module import detected: {cycle}")

        resolving.add(module_class)
        metadata: ModuleMetadata | None = get_metadata(module_class, MODULE)

        if metadata is None:
            resolving.discard(module_class)
            graph[module_class] = ModuleNode(module_class=module_class)
            return

        logger.debug(
            "walk %s: %d providers, %d controllers, %d guards, %d imports",
            module_class.__name__,
            len(metadata.providers or []),
            len(metadata.controllers or []),
            len(metadata.guards or []),
            len(metadata.imports or []),
        )

        for imported in metadata.imports or []:
            walk(imported, [*import_chain, module_class])

        own_tokens: set[Any] = set()
        for provider in metadata.providers or []:
            own_tokens.add(provider)

        for ctrl in metadata.controllers or []:
            own_tokens.add(ctrl)

        for g in metadata.guards or []:
            if isinstance(g, type):
                own_tokens.add(g)

        resolving.discard(module_class)
        graph[module_class] = ModuleNode(
            module_class=module_class,
            own_tokens=own_tokens,
            exports=set(metadata.exports or []),
            imports=metadata.imports or [],
            controllers=metadata.controllers or [],
            function_handlers=metadata.function_handlers or [],
            guards=metadata.guards or [],
            providers=metadata.providers or [],
            layers=metadata.layers or [],
        )

    walk(root_module, [])
    logger.debug("build_module_graph: complete — %d modules", len(graph))
    return graph


def register_module_graph(graph: ModuleGraph, container: Container) -> None:
    """Register all providers from the module graph into a DI container.

    Args:
        graph: The module graph built by ``build_module_graph``.
        container: The DI container to register providers in.
    """

    for node in graph.values():
        for provider in node.providers:
            if isinstance(provider, type):
                container.register_class(provider)
            else:
                container.register(provider, provider)

        for ctrl in node.controllers:
            if not container.has(ctrl):
                container.register_class(ctrl)

        for g in node.guards:
            if isinstance(g, type) and not container.has(g):
                container.register_class(g)


def walk_module_graph(root_module: type, container: Container) -> ModuleGraph:
    """Build the module graph and register all providers in one call.

    Args:
        root_module: The root ``@module``-decorated class.
        container: The DI container to register providers in.

    Returns:
        The built module graph.
    """
    graph = build_module_graph(root_module)
    register_module_graph(graph, container)
    return graph
