"""HTTP method decorators."""

from collections.abc import Callable
from typing import TypeVar

from celerity.metadata.keys import _META_ATTR, HTTP_METHOD, ROUTE_PATH

_FuncT = TypeVar("_FuncT", bound=Callable[..., object])


def _method_decorator(method: str, path: str = "/") -> Callable[[_FuncT], _FuncT]:
    """Factory for HTTP method decorators.

    Args:
        method: The HTTP method verb (e.g. ``"GET"``, ``"POST"``).
        path: The route path pattern for the handler.

    Returns:
        A method decorator that attaches HTTP method and route metadata.
    """

    def decorator(fn: _FuncT) -> _FuncT:
        if not hasattr(fn, _META_ATTR):
            setattr(fn, _META_ATTR, {})
        getattr(fn, _META_ATTR)[HTTP_METHOD] = method
        getattr(fn, _META_ATTR)[ROUTE_PATH] = path
        return fn

    return decorator


def get(path: str = "/") -> Callable[[_FuncT], _FuncT]:
    """Mark a method as a GET handler.

    Args:
        path: The route path pattern. Defaults to ``"/"``.

    Returns:
        A method decorator.

    Example::

        @get("/users/{user_id}")
        async def get_user(self, user_id: Param[str]) -> HandlerResponse: ...
    """
    return _method_decorator("GET", path)


def post(path: str = "/") -> Callable[[_FuncT], _FuncT]:
    """Mark a method as a POST handler.

    Args:
        path: The route path pattern. Defaults to ``"/"``.

    Returns:
        A method decorator.

    Example::

        @post("/users")
        async def create_user(self, body: Body[CreateUserInput]) -> HandlerResponse: ...
    """
    return _method_decorator("POST", path)


def put(path: str = "/") -> Callable[[_FuncT], _FuncT]:
    """Mark a method as a PUT handler.

    Args:
        path: The route path pattern. Defaults to ``"/"``.

    Returns:
        A method decorator.

    Example::

        @put("/users/{user_id}")
        async def update_user(
            self, user_id: Param[str], body: Body[UpdateUserInput],
        ) -> HandlerResponse: ...
    """
    return _method_decorator("PUT", path)


def patch(path: str = "/") -> Callable[[_FuncT], _FuncT]:
    """Mark a method as a PATCH handler.

    Args:
        path: The route path pattern. Defaults to ``"/"``.

    Returns:
        A method decorator.

    Example::

        @patch("/users/{user_id}")
        async def patch_user(
            self, user_id: Param[str], body: Body[PatchUserInput],
        ) -> HandlerResponse: ...
    """
    return _method_decorator("PATCH", path)


def delete(path: str = "/") -> Callable[[_FuncT], _FuncT]:
    """Mark a method as a DELETE handler.

    Args:
        path: The route path pattern. Defaults to ``"/"``.

    Returns:
        A method decorator.

    Example::

        @delete("/users/{user_id}")
        async def delete_user(self, user_id: Param[str]) -> HandlerResponse: ...
    """
    return _method_decorator("DELETE", path)


def head(path: str = "/") -> Callable[[_FuncT], _FuncT]:
    """Mark a method as a HEAD handler.

    Args:
        path: The route path pattern. Defaults to ``"/"``.

    Returns:
        A method decorator.

    Example::

        @head("/users/{user_id}")
        async def check_user(self, user_id: Param[str]) -> HandlerResponse: ...
    """
    return _method_decorator("HEAD", path)


def options(path: str = "/") -> Callable[[_FuncT], _FuncT]:
    """Mark a method as an OPTIONS handler.

    Args:
        path: The route path pattern. Defaults to ``"/"``.

    Returns:
        A method decorator.

    Example::

        @options("/users")
        async def user_options(self) -> HandlerResponse: ...
    """
    return _method_decorator("OPTIONS", path)
