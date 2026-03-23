"""Extract constructor dependency tokens from type hints."""

from __future__ import annotations

import inspect
from typing import Annotated, Any, get_args, get_origin, get_type_hints

from celerity.decorators.injectable import _InjectMarker
from celerity.metadata.keys import INJECT, get_metadata
from celerity.resources._tokens import is_resource_marker, resolve_marker_token

# Registry of (namespace_token, model_type) pairs discovered during
# dependency token extraction.  The config layer reads this after
# bootstrap to register FactoryProviders that parse each namespace
# into the requested model.
pending_parsed_configs: list[tuple[str, type]] = []


def resolve_resource_token(base_type: type, marker: object) -> str | None:
    """Resolve a DI token from an ``Annotated`` resource marker.

    For ``ConfigParam`` markers where the base type is a Pydantic model
    (has ``model_validate``), produces a parsed-config token and records
    the ``(namespace_token, model_type)`` pair in
    ``pending_parsed_configs`` so the config layer can register a factory.

    For all other resource markers, delegates to ``resolve_marker_token``.

    Returns:
        The DI token string, or ``None`` if the marker is not recognised.
    """
    marker_token = resolve_marker_token(marker)
    if marker_token is None:
        return None

    if (
        getattr(marker, "resource_type", None) == "config"
        and isinstance(base_type, type)
        and hasattr(base_type, "model_validate")
    ):
        parsed_token = f"{marker_token}:parsed:{base_type.__qualname__}"
        pending_parsed_configs.append((marker_token, base_type))
        return parsed_token

    return marker_token


def get_class_dependency_tokens(target: type) -> list[Any]:
    """Extract constructor dependency tokens from a class's type hints.

    Resolution order for each parameter:

    1. Class-level ``@inject({index: token})`` override.
    2. ``Annotated[Type, inject(token)]`` -- explicit DI token marker.
    3. ``Annotated[Type, ResourceMarker]`` -- resource marker (e.g.
       ``Annotated[Cache, CacheParam()]``) resolved to a DI token.
    4. ``Annotated[Type, ...]`` -- falls back to the base type.
    5. The bare type hint itself.

    Args:
        target: The class to inspect.

    Returns:
        An ordered list of DI tokens matching the constructor parameters
        (excluding ``self``).

    Example::

        @injectable()
        class OrderService:
            def __init__(self, db: DatabaseClient, cache: CacheResource) -> None:
                ...

        tokens = get_class_dependency_tokens(OrderService)
        # [DatabaseClient, "celerity:cache:default"]
    """
    init = target.__init__  # type: ignore[misc]
    if init is object.__init__:
        return []

    try:
        hints = get_type_hints(init, include_extras=True)
    except Exception:
        return []

    sig = inspect.signature(init)
    params = [p for p in sig.parameters.values() if p.name != "self"]

    inject_overrides: dict[int, Any] = get_metadata(target, INJECT) or {}

    tokens: list[Any] = []
    for i, param in enumerate(params):
        if i in inject_overrides:
            tokens.append(inject_overrides[i])
            continue

        hint = hints.get(param.name)
        if hint is None:
            continue

        if get_origin(hint) is Annotated:
            args = get_args(hint)

            # Check for explicit @inject marker first.
            inject_marker = next(
                (a for a in args[1:] if isinstance(a, _InjectMarker)),
                None,
            )
            if inject_marker is not None:
                tokens.append(inject_marker.token)
                continue

            # Check for resource marker (CacheParam, ConfigParam, etc.)
            resource_marker = next(
                (a for a in args[1:] if is_resource_marker(a)),
                None,
            )
            if resource_marker is not None:
                token = resolve_resource_token(args[0], resource_marker)
                if token is not None:
                    tokens.append(token)
                    continue

            # Fall back to the base type.
            tokens.append(args[0])
            continue

        tokens.append(hint)

    return tokens
