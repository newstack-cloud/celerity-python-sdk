"""ObjectStorageLayer system layer."""

from __future__ import annotations

import contextlib
import logging
from typing import TYPE_CHECKING, Any

from celerity.config.service import CONFIG_SERVICE_TOKEN, RESOURCE_CONFIG_NAMESPACE
from celerity.resources._common import capture_resource_links, get_links_of_type
from celerity.resources._tokens import default_token, resource_token
from celerity.resources.bucket.factory import create_object_storage
from celerity.telemetry.helpers import TRACER_TOKEN
from celerity.types.layer import CelerityLayer

if TYPE_CHECKING:
    from celerity.resources.bucket.types import ObjectStorage

logger = logging.getLogger("celerity.bucket")


class ObjectStorageLayer(CelerityLayer):
    """System layer for bucket resources.

    Reads the Celerity CLI-generated resource links file, creates ``ObjectStorage``
    instances, and registers per-bucket ``Bucket`` handles in the
    DI container.
    """

    def __init__(self) -> None:
        self._initialized = False
        self._storages: list[ObjectStorage] = []

    async def handle(self, context: Any, next_handler: Any) -> Any:
        if not self._initialized:
            container = context.container
            await self._init(container)
            self._initialized = True
        return await next_handler()

    async def _init(self, container: Any) -> None:
        links = capture_resource_links()
        bucket_links = get_links_of_type(links, "bucket")
        if not bucket_links:
            return

        tracer = None
        with contextlib.suppress(Exception):
            tracer = await container.resolve(TRACER_TOKEN)

        config_service = await container.resolve(CONFIG_SERVICE_TOKEN)
        resource_config = config_service.namespace(RESOURCE_CONFIG_NAMESPACE)

        # Resolve all resource name → physical ID mappings first.
        resource_ids: dict[str, str] = {}
        for resource_name, config_key in bucket_links.items():
            physical_id = await resource_config.get(config_key)
            resource_ids[resource_name] = physical_id or resource_name

        # One shared ObjectStorage client with the full mapping.
        storage = create_object_storage(tracer=tracer, resource_ids=resource_ids)
        self._storages.append(storage)

        for resource_name in bucket_links:
            bucket_handle = storage.bucket(resource_name)
            container.register_value(
                resource_token("bucket", resource_name),
                bucket_handle,
            )

        if len(bucket_links) == 1:
            only_name = next(iter(bucket_links))
            only_token = resource_token("bucket", only_name)
            default = await container.resolve(only_token)
            container.register_value(default_token("bucket"), default)

        logger.debug("bucket: registered %d bucket resource(s)", len(bucket_links))

    async def dispose(self) -> None:
        """Close all ObjectStorage clients."""
        for storage in self._storages:
            await storage.close()
