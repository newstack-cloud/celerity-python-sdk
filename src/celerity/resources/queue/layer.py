"""QueueLayer system layer."""

from __future__ import annotations

import contextlib
import logging
from typing import TYPE_CHECKING, Any

from celerity.config.service import CONFIG_SERVICE_TOKEN, RESOURCE_CONFIG_NAMESPACE
from celerity.resources._common import capture_resource_links, get_links_of_type
from celerity.resources._tokens import default_token, resource_token
from celerity.resources.queue.factory import create_queue_client
from celerity.telemetry.helpers import TRACER_TOKEN
from celerity.types.layer import CelerityLayer

if TYPE_CHECKING:
    from celerity.resources.queue.types import QueueClient

logger = logging.getLogger("celerity.queue")


class QueueLayer(CelerityLayer):
    """System layer for queue resources.

    Reads ``CELERITY_RESOURCE_LINKS``, creates a shared
    ``QueueClient``, resolves per-resource queue URLs from
    the config service, and registers ``Queue`` handles in the
    DI container.
    """

    def __init__(self) -> None:
        self._initialized = False
        self._client: QueueClient | None = None

    async def handle(self, context: Any, next_handler: Any) -> Any:
        if not self._initialized:
            container = context.container
            await self._init(container)
            self._initialized = True
        return await next_handler()

    async def _init(self, container: Any) -> None:
        links = capture_resource_links()
        queue_links = get_links_of_type(links, "queue")
        if not queue_links:
            return

        # Resolve tracer if available (registered by TelemetryLayer).
        tracer = None
        with contextlib.suppress(Exception):
            tracer = await container.resolve(TRACER_TOKEN)

        config_service = await container.resolve(CONFIG_SERVICE_TOKEN)
        resource_config = config_service.namespace(RESOURCE_CONFIG_NAMESPACE)

        # Resolve all resource name → physical ID mappings first.
        resource_ids: dict[str, str] = {}
        for resource_name, config_key in queue_links.items():
            physical_id = await resource_config.get(config_key)
            resource_ids[resource_name] = physical_id or resource_name

        # One shared client with the full mapping.
        client = create_queue_client(tracer=tracer, resource_ids=resource_ids)
        self._client = client

        for resource_name in queue_links:
            queue_handle = client.queue(resource_name)
            container.register_value(
                resource_token("queue", resource_name),
                queue_handle,
            )

        if len(queue_links) == 1:
            only_name = next(iter(queue_links))
            only_token = resource_token("queue", only_name)
            default = await container.resolve(only_token)
            container.register_value(default_token("queue"), default)

        logger.debug("queue: registered %d queue resource(s)", len(queue_links))

    async def dispose(self) -> None:
        """Close the QueueClient."""
        if self._client:
            await self._client.close()
