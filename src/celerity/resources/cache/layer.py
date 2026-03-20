"""CacheLayer system layer."""

from __future__ import annotations

import contextlib
import logging
from typing import TYPE_CHECKING, Any

from celerity.config.service import CONFIG_SERVICE_TOKEN, RESOURCE_CONFIG_NAMESPACE
from celerity.resources._common import (
    capture_resource_links,
    detect_runtime_mode,
    get_links_of_type,
)
from celerity.resources._tokens import default_token, resource_token
from celerity.resources.cache.config import resolve_connection_config
from celerity.resources.cache.credentials import resolve_cache_credentials
from celerity.resources.cache.providers.redis.client import (
    create_redis_cache_client,
)
from celerity.telemetry.helpers import TRACER_TOKEN
from celerity.types.layer import CelerityLayer

if TYPE_CHECKING:
    from celerity.resources.cache.types import CacheClient

logger = logging.getLogger("celerity.cache")


class CacheLayer(CelerityLayer):
    """System layer for cache resources.

    On first request, resolves cache resource links, creates the
    Redis client, and registers per-resource cache handles in the
    DI container.
    """

    def __init__(self) -> None:
        self._initialized = False
        self._client: CacheClient | None = None

    async def handle(self, context: Any, next_handler: Any) -> Any:
        if not self._initialized:
            container = context.container
            await self._init(container)
            self._initialized = True
        return await next_handler()

    async def _init(self, container: Any) -> None:
        links = capture_resource_links()
        cache_links = get_links_of_type(links, "cache")
        if not cache_links:
            return

        config_service = await container.resolve(CONFIG_SERVICE_TOKEN)
        resource_config = config_service.namespace(RESOURCE_CONFIG_NAMESPACE)
        runtime_mode = detect_runtime_mode()

        # Resolve tracer if available (registered by TelemetryLayer).
        tracer = None
        with contextlib.suppress(Exception):
            tracer = await container.resolve(TRACER_TOKEN)

        # Resolve all resource name → key prefix mappings first.
        key_prefixes: dict[str, str] = {}
        for resource_name, config_key in cache_links.items():
            key_prefix_raw = await resource_config.get(f"{config_key}_keyPrefix")
            key_prefixes[resource_name] = key_prefix_raw or ""

        # Create one shared Redis client with the full mapping.
        first_config_key = next(iter(cache_links.values()))
        connection_info, auth = await resolve_cache_credentials(resource_config, first_config_key)
        connection_config = resolve_connection_config(runtime_mode)
        client = await create_redis_cache_client(
            connection_info, auth, connection_config, tracer=tracer, key_prefixes=key_prefixes
        )
        self._client = client

        for resource_name in cache_links:
            cache_handle = client.cache(resource_name)
            container.register_value(
                resource_token("cache", resource_name),
                cache_handle,
            )

        if len(cache_links) == 1:
            only_name = next(iter(cache_links))
            only_token = resource_token("cache", only_name)
            default = await container.resolve(only_token)
            container.register_value(default_token("cache"), default)

        logger.debug("cache: registered %d cache resource(s)", len(cache_links))

    async def dispose(self) -> None:
        if self._client:
            await self._client.close()
