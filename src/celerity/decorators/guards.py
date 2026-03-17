"""Guard decorators."""

from collections.abc import Callable
from typing import Any, TypeVar

from celerity.metadata.keys import (
    _META_ATTR,
    GUARD_CUSTOM,
    GUARD_PROTECTEDBY,
    INJECTABLE,
    PUBLIC,
    get_metadata,
    set_metadata,
)

_FuncT = TypeVar("_FuncT", bound=Callable[..., object])


def guard(name: str) -> Callable[[type], type]:
    """Mark a class as a custom auth guard.

    A guard class must implement an ``can_activate`` method that receives
    the request context and returns a boolean or raises an exception.

    Args:
        name: The guard identifier used in ``@protected_by``.

    Returns:
        A class decorator that registers the guard.

    Example::

        @guard("jwt")
        @injectable()
        class JwtGuard:
            async def can_activate(self, context: ExecutionContext) -> bool:
                token = context.get_token()
                return self.auth_service.verify(token)
    """

    def decorator(cls: type) -> type:
        set_metadata(cls, GUARD_CUSTOM, name)
        if not get_metadata(cls, INJECTABLE):
            set_metadata(cls, INJECTABLE, True)
        return cls

    return decorator


def protected_by(name: str) -> Callable[[Any], Any]:
    """Declare that a handler or controller is protected by a named guard.

    Multiple ``@protected_by`` decorators stack in declaration order
    (topmost runs first). Works at both class level and method level.

    Args:
        name: The guard identifier to apply.

    Returns:
        A decorator applicable to a class or method.

    Example::

        @controller("/admin")
        @protected_by("jwt")
        class AdminController:
            @get("/stats")
            async def stats(self) -> HandlerResponse: ...

            @get("/health")
            @public()
            async def health(self) -> HandlerResponse: ...

        @controller("/orders")
        class OrderController:
            @post("/")
            @protected_by("api-key")
            async def create_order(self, body: Body[CreateOrderInput]) -> HandlerResponse: ...
    """

    def decorator(target: Any) -> Any:
        if isinstance(target, type):
            existing: list[str] = get_metadata(target, GUARD_PROTECTEDBY) or []
            set_metadata(target, GUARD_PROTECTEDBY, [name, *existing])
        else:
            if not hasattr(target, _META_ATTR):
                setattr(target, _META_ATTR, {})
            meta: dict[str, Any] = getattr(target, _META_ATTR)
            existing = meta.get(GUARD_PROTECTEDBY, [])
            meta[GUARD_PROTECTEDBY] = [name, *existing]
        return target

    return decorator


def public() -> Callable[[_FuncT], _FuncT]:
    """Mark a method as public, opting out of the default guard chain.

    When a controller has guards applied via ``@protected_by``, individual
    handler methods can be exempted by applying ``@public()``.

    Returns:
        A method decorator.

    Example::

        @controller("/api")
        @protected_by("jwt")
        class ApiController:
            @get("/health")
            @public()
            async def health(self) -> HandlerResponse: ...
    """

    def decorator(fn: _FuncT) -> _FuncT:
        if not hasattr(fn, _META_ATTR):
            setattr(fn, _META_ATTR, {})
        getattr(fn, _META_ATTR)[PUBLIC] = True
        return fn

    return decorator
