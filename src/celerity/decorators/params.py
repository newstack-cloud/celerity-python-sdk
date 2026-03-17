"""HTTP parameter injection types.

Used as type annotations on handler method parameters to declare how
values are extracted from the request. The runtime scanner inspects
these annotations to determine injection behaviour.

The ``_AnnotatedParam`` mechanism provides generic type wrappers that
carry ``ParamMeta`` describing the injection source (body, query, param,
etc.). Schema validation and key extraction are handled at the scanner
level based on the inner type argument, so no ``schema`` or ``key``
arguments need to be passed at the annotation site.

Example::

    @get("/{order_id}")
    async def get_order(
        self,
        order_id: Param[str],
        body: Body[CreateOrderInput],
        auth: Auth,
    ) -> HandlerResponse:
        ...
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from celerity.types.common import Schema

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ParamMeta:
    """Metadata describing how a parameter should be injected.

    Attributes:
        type: The injection source identifier (e.g. ``"body"``,
            ``"query"``, ``"param"``).
        key: Optional key within the source to extract a specific
            value. Reserved for scanner-level use.
        schema: Optional schema for validation. Reserved for
            scanner-level use.
    """

    type: str
    key: str | None = None
    schema: Schema[Any] | None = None


class _AnnotatedParam[T]:
    """Base for generic parameter types that carry injection metadata.

    Subclasses set a class-level ``_meta`` attribute with a ``ParamMeta``
    instance. When subscripted (e.g. ``Body[MyModel]``), a new type is
    dynamically created that carries both the ``ParamMeta`` and the
    inner type argument. The runtime scanner reads
    ``__celerity_param__`` to determine injection behaviour.

    Schema and key functionality is resolved at the scanner level from
    the inner type argument, not from the annotation itself.
    """

    _meta: ParamMeta
    __celerity_param__: ParamMeta

    def __class_getitem__(cls, item: Any) -> type:
        if not hasattr(cls, "_meta"):
            return cls
        name = item.__name__ if isinstance(item, type) else str(item)
        return type(
            f"{cls.__name__}[{name}]",
            (),
            {
                "__celerity_param__": cls._meta,
                "__origin__": cls,
                "__args__": (item,),
            },
        )


# -- HTTP parameter types --


class Body(_AnnotatedParam[T]):
    """Extract the parsed request body.

    The inner type argument is used by the scanner for validation.

    Example::

        @post("/orders")
        async def create(self, body: Body[CreateOrderInput]) -> HandlerResponse: ...
    """

    _meta = ParamMeta(type="body")


class Query(_AnnotatedParam[T]):
    """Extract query string parameters.

    The inner type argument is used by the scanner for validation.

    Example::

        @get("/search")
        async def search(self, filters: Query[SearchFilters]) -> HandlerResponse: ...
    """

    _meta = ParamMeta(type="query")


class Param(_AnnotatedParam[T]):
    """Extract a path parameter.

    Example::

        @get("/users/{user_id}")
        async def get_user(self, user_id: Param[str]) -> HandlerResponse: ...
    """

    _meta = ParamMeta(type="param")


class Headers(_AnnotatedParam[T]):
    """Extract request headers.

    Example::

        @get("/data")
        async def get_data(self, headers: Headers[dict]) -> HandlerResponse: ...
    """

    _meta = ParamMeta(type="headers")


class Cookies(_AnnotatedParam[T]):
    """Extract request cookies.

    Example::

        @get("/preferences")
        async def get_prefs(self, cookies: Cookies[dict]) -> HandlerResponse: ...
    """

    _meta = ParamMeta(type="cookies")


# -- Non-generic parameter types --


class Auth:
    """Extract the decoded auth payload (identity from guards).

    Example::

        @get("/me")
        async def get_profile(self, auth: Auth) -> HandlerResponse: ...
    """

    __celerity_param__ = ParamMeta(type="auth")


class Token:
    """Extract the raw auth token string.

    Example::

        @get("/verify")
        async def verify(self, token: Token) -> HandlerResponse: ...
    """

    __celerity_param__ = ParamMeta(type="token")


class Req:
    """Extract the full HttpRequest object.

    Example::

        @get("/debug")
        async def debug(self, req: Req) -> HandlerResponse: ...
    """

    __celerity_param__ = ParamMeta(type="request")


class RequestId:
    """Extract the unique request ID.

    Example::

        @get("/trace")
        async def trace(self, request_id: RequestId) -> HandlerResponse: ...
    """

    __celerity_param__ = ParamMeta(type="requestId")
