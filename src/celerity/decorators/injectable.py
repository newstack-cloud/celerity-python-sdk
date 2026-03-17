"""Injectable and inject decorators for DI."""

from collections.abc import Callable
from typing import Any, overload

from celerity.metadata.keys import (
    INJECT,
    INJECTABLE,
    get_metadata,
    set_metadata,
)


def injectable() -> Callable[[type], type]:
    """Mark a class as injectable into the DI container.

    Any class that should be resolved by the dependency injection
    container must be decorated with ``@injectable()``.

    Returns:
        A class decorator that registers the class for DI.

    Example::

        @injectable()
        class OrderService:
            def __init__(self, db: DatabaseClient) -> None:
                self.db = db
    """

    def decorator(cls: type) -> type:
        set_metadata(cls, INJECTABLE, True)
        return cls

    return decorator


@overload
def inject(token: dict[int, Any]) -> Callable[[type], type]: ...


@overload
def inject(token: str | type) -> "_InjectMarker": ...


def inject(
    token: dict[int, Any] | str | type,
) -> "Callable[[type], type] | _InjectMarker":
    """Override the DI token for constructor parameters.

    Has two modes:

    **Mode 1** -- Class decorator with an index map::

        @injectable()
        @inject({0: DB_TOKEN, 2: CACHE_TOKEN})
        class OrderService:
            def __init__(self, db, logger, cache): ...

    **Mode 2** -- Annotated marker for a single parameter::

        from typing import Annotated

        @injectable()
        class OrderService:
            def __init__(
                self, db: Annotated[DatabaseClient, inject(DB_TOKEN)]
            ): ...

    Args:
        token: A dict mapping parameter indices to tokens (mode 1),
            or a single token value (mode 2).

    Returns:
        A class decorator (mode 1) or an ``_InjectMarker`` instance
        (mode 2) for use inside ``Annotated``.
    """
    if isinstance(token, dict):

        def decorator(cls: type) -> type:
            existing: dict[int, Any] = get_metadata(cls, INJECT) or {}
            existing.update(token)
            set_metadata(cls, INJECT, existing)
            return cls

        return decorator

    return _InjectMarker(token)


class _InjectMarker:
    """Marker used with ``typing.Annotated`` for per-parameter injection.

    This class is not intended to be instantiated directly. Use
    ``inject(token)`` inside an ``Annotated`` type hint instead.

    Example::

        from typing import Annotated

        class MyService:
            def __init__(
                self, db: Annotated[DatabaseClient, inject(DB_TOKEN)]
            ): ...
    """

    __slots__ = ("token",)

    def __init__(self, token: str | type) -> None:
        self.token: str | type = token
