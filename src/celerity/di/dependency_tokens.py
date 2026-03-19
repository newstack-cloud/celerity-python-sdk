"""Extract constructor dependency tokens from type hints."""

from __future__ import annotations

import inspect
from typing import Annotated, Any, get_args, get_origin, get_type_hints

from celerity.decorators.injectable import _InjectMarker
from celerity.metadata.keys import INJECT, get_metadata
from celerity.resources._tokens import resolve_param_token


def get_class_dependency_tokens(target: type) -> list[Any]:
    """Extract constructor dependency tokens from a class's type hints.

    Resolution order for each parameter:

    1. Class-level ``@inject({index: token})`` override.
    2. ``Annotated[Type, inject(token)]`` marker in the type hint.
    3. ``__celerity_param__`` on the type hint (e.g. ``Config["appConfig"]``,
       ``CacheResource["my-cache"]``) -- resolved to a DI token.
    4. The bare type hint itself.

    Args:
        target: The class to inspect.

    Returns:
        An ordered list of DI tokens matching the constructor parameters
        (excluding ``self``).

    Example::

        @injectable()
        class OrderService:
            def __init__(self, db: DatabaseClient, cache: CacheClient) -> None:
                ...

        tokens = get_class_dependency_tokens(OrderService)
        # [DatabaseClient, CacheClient]

        @injectable()
        class SettingsService:
            def __init__(self, config: Config["appConfig"]) -> None:
                ...

        tokens = get_class_dependency_tokens(SettingsService)
        # ["celerity:config:appConfig"]
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
            inject_marker = next(
                (a for a in args[1:] if isinstance(a, _InjectMarker)),
                None,
            )
            if inject_marker is not None:
                tokens.append(inject_marker.token)
            else:
                tokens.append(args[0])
            continue

        # Check for __celerity_param__ (Config["name"], CacheResource, etc.)
        param_token = resolve_param_token(hint)
        if param_token is not None:
            tokens.append(param_token)
            continue

        tokens.append(hint)

    return tokens
