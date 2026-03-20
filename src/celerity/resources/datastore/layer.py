"""DatastoreLayer system layer."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from celerity.config.service import CONFIG_SERVICE_TOKEN, RESOURCE_CONFIG_NAMESPACE
from celerity.resources._common import capture_resource_links, get_links_of_type
from celerity.resources._tokens import default_token, resource_token
from celerity.resources.datastore.factory import create_datastore_client
from celerity.types.layer import CelerityLayer

if TYPE_CHECKING:
    from celerity.resources.datastore.types import DatastoreClient

logger = logging.getLogger("celerity.datastore")


class DatastoreLayer(CelerityLayer):
    """System layer for datastore resources.

    Reads ``CELERITY_RESOURCE_LINKS``, creates a shared
    ``DatastoreClient``, resolves per-resource table names from
    the config service, and registers ``Datastore`` handles in the
    DI container.
    """

    def __init__(self) -> None:
        self._initialized = False
        self._client: DatastoreClient | None = None

    async def handle(self, context: Any, next_handler: Any) -> Any:
        if not self._initialized:
            container = context.container
            await self._init(container)
            self._initialized = True
        return await next_handler()

    async def _init(self, container: Any) -> None:
        links = capture_resource_links()
        datastore_links = get_links_of_type(links, "datastore")
        if not datastore_links:
            return

        # One shared client for all datastore resources.
        client = create_datastore_client()
        self._client = client

        config_service = await container.resolve(CONFIG_SERVICE_TOKEN)
        resource_config = config_service.namespace(RESOURCE_CONFIG_NAMESPACE)

        for resource_name, config_key in datastore_links.items():
            # The config key resolves to the actual table name.
            actual_name = await resource_config.get(config_key)
            table_name = actual_name or resource_name
            ds_handle = client.datastore(resource_name, table_name=table_name)
            container.register_value(
                resource_token("datastore", resource_name),
                ds_handle,
            )

        if len(datastore_links) == 1:
            only_name = next(iter(datastore_links))
            only_token = resource_token("datastore", only_name)
            default = await container.resolve(only_token)
            container.register_value(default_token("datastore"), default)

        logger.debug("datastore: registered %d datastore resource(s)", len(datastore_links))

    async def dispose(self) -> None:
        """Close the DatastoreClient."""
        if self._client:
            await self._client.close()
