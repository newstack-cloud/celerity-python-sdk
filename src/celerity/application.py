"""Application classes and factory."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from celerity.bootstrap.bootstrap import bootstrap
from celerity.layers.dispose import dispose_layers
from celerity.layers.system import create_default_system_layers

if TYPE_CHECKING:
    from celerity.bootstrap.module_graph import ModuleGraph
    from celerity.di.container import Container
    from celerity.handlers.registry import HandlerRegistry

logger = logging.getLogger("celerity.factory")


class CelerityApplication:
    """Standard application with handler registry and DI container.

    Created via ``CelerityFactory.create()``. Holds the bootstrapped
    container, handler registry, and layer pipeline configuration.
    """

    def __init__(
        self,
        registry: HandlerRegistry,
        container: Container,
        graph: ModuleGraph,
        system_layers: list[Any] | None = None,
    ) -> None:
        self.registry = registry
        self.container = container
        self.graph = graph
        self.system_layers = system_layers or []

    async def close(self) -> None:
        """Shut down the application, disposing layers and closing resources."""
        await dispose_layers(self.system_layers)
        await self.container.close_all()

    def get_container(self) -> Container:
        """Get the DI container."""
        return self.container

    def get_registry(self) -> HandlerRegistry:
        """Get the handler registry."""
        return self.registry


class CelerityFactory:
    """Factory for creating application instances."""

    @staticmethod
    async def create(
        root_module: type,
        *,
        layers: list[Any] | None = None,
    ) -> CelerityApplication:
        """Create and bootstrap a ``CelerityApplication``.

        Args:
            root_module: The root ``@module``-decorated class.
            layers: Optional application-level layers to add after
                system layers.

        Returns:
            A bootstrapped application ready to handle requests.

        Example::

            app = await CelerityFactory.create(AppModule)
            # use app.registry, app.container
            await app.close()
        """
        container, registry, graph = await bootstrap(root_module)
        system_layers = create_default_system_layers()
        logger.debug("create: %d system layers", len(system_layers))
        if layers:
            system_layers.extend(layers)
        return CelerityApplication(registry, container, graph, system_layers)
