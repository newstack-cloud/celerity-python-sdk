"""Cache parameter types and helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from celerity.resources._tokens import default_token, resource_token
from celerity.resources.cache.types import Cache

if TYPE_CHECKING:
    from celerity.types.container import ServiceContainer


class CacheParam:
    """DI marker for cache injection.

    Used inside ``Annotated[...]`` to tell the DI resolver which
    cache resource to inject.

    Usage::

        from typing import Annotated
        from celerity.resources.cache import Cache, CacheParam

        # Default cache:
        CacheResource = Annotated[Cache, CacheParam()]

        # Named cache:
        SessionCache = Annotated[Cache, CacheParam("session")]
    """

    resource_type: str = "cache"

    def __init__(self, name: str | None = None) -> None:
        self.resource_name = name


def cache_token(resource_name: str) -> str:
    """Create a DI token for a named cache resource."""
    return resource_token("cache", resource_name)


DEFAULT_CACHE_TOKEN = default_token("cache")

CacheResource = Annotated[Cache, CacheParam()]
"""Default cache injection type.

Type checker sees ``Cache``, DI resolves via ``celerity:cache:default``.
"""


async def get_cache(
    container: ServiceContainer,
    resource_name: str | None = None,
) -> Cache:
    """Resolve a Cache handle from the container without DI.

    Args:
        container: The DI container.
        resource_name: Optional resource name. If ``None``, resolves the
            default cache.
    """
    token = cache_token(resource_name) if resource_name else DEFAULT_CACHE_TOKEN
    result: Cache = await container.resolve(token)
    return result
