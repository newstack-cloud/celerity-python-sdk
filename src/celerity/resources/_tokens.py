"""DI token factories and resource layer token detection."""

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

    Used when exactly one resource of a type exists, enabling the
    no-argument form of resource parameter decorators.

    Example::

        default_token("cache") -> "celerity:cache:default"
    """
    return f"celerity:{resource_type}:default"


def resolve_param_token(hint: Any) -> str | None:
    """Resolve a DI token from a ``__celerity_param__`` type hint.

    If the hint has a ``__celerity_param__`` attribute with a ``type``
    and optional ``key``, returns the corresponding DI token. This
    enables parameter types like ``Config["appConfig"]`` and
    ``CacheResource["my-cache"]`` to work in both handler parameters
    AND constructor/factory injection.

    Returns:
        The DI token string, or ``None`` if the hint is not a
        param type or has no resolvable token.
    """
    meta = getattr(hint, "__celerity_param__", None)
    if meta is None:
        return None

    param_type: str = getattr(meta, "type", "")
    key: str | None = getattr(meta, "key", None)

    if not param_type:
        return None

    if key:
        return resource_token(param_type, key)
    return default_token(param_type)


def is_resource_layer_token(token: Any) -> bool:
    """Check if a token will be lazily registered by a resource layer.

    Used by the CLI dependency validator and runtime to skip tokens
    that aren't registered at bootstrap time but will be available
    once resource layers run on first request.
    """
    if not isinstance(token, str):
        return False
    return any(token.startswith(prefix) for prefix in RESOURCE_TOKEN_PREFIXES)
