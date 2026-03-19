"""DI token factories and resource marker resolution."""

from __future__ import annotations

from typing import Any

RESOURCE_TOKEN_PREFIXES = (
    "celerity:config:",
    "celerity:datastore:",
    "celerity:bucket:",
    "celerity:cache:",
    "celerity:queue:",
    "celerity:topic:",
    "celerity:sqlDatabase:",
)


def resource_token(resource_type: str, resource_name: str) -> str:
    """Create a per-resource DI token.

    Example::

        resource_token("cache", "app-cache") -> "celerity:cache:app-cache"
    """
    return f"celerity:{resource_type}:{resource_name}"


def default_token(resource_type: str) -> str:
    """Create a default DI token for a resource type.

    Used when exactly one resource of a type exists.

    Example::

        default_token("cache") -> "celerity:cache:default"
    """
    return f"celerity:{resource_type}:default"


def resolve_marker_token(marker: Any) -> str | None:
    """Resolve a DI token from a resource marker instance.

    Markers are objects with ``resource_type`` and optional
    ``resource_name`` attributes, used inside ``Annotated[Type, Marker]``.

    Examples::

        resolve_marker_token(CacheParam())           -> "celerity:cache:default"
        resolve_marker_token(CacheParam("session"))   -> "celerity:cache:session"
        resolve_marker_token(ConfigParam("appConfig")) -> "celerity:config:appConfig"

    Returns:
        The DI token string, or ``None`` if the object is not a
        resource marker.
    """
    rt = getattr(marker, "resource_type", None)
    if rt is None:
        return None

    name = getattr(marker, "resource_name", None)
    if name:
        return resource_token(rt, name)
    return default_token(rt)


def is_resource_marker(obj: Any) -> bool:
    """Check if an object is a resource marker (has ``resource_type``)."""
    return hasattr(obj, "resource_type")


def is_resource_layer_token(token: Any) -> bool:
    """Check if a token will be lazily registered by a resource layer.

    Used by the CLI dependency validator and runtime to skip tokens
    that aren't registered at bootstrap time but will be available
    once resource layers run on first request.
    """
    if not isinstance(token, str):
        return False
    return any(token.startswith(prefix) for prefix in RESOURCE_TOKEN_PREFIXES)
