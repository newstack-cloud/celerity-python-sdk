"""Function-based guard factory."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from celerity.types.module import GuardDefinition

if TYPE_CHECKING:
    from collections.abc import Callable


def create_guard(
    *,
    name: str,
    handler: Callable[..., Any],
    inject: list[Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> GuardDefinition:
    """Create a function-based guard definition.

    Args:
        name: The guard identifier used in ``@protected_by``.
        handler: The guard function that receives ``GuardInput``
            and returns ``GuardResult``.
        inject: Optional DI tokens for injected dependencies.
        metadata: Optional custom metadata.

    Returns:
        A ``GuardDefinition`` for registration in a module.

    Example::

        async def admin_guard(guard_input):
            if "admin" in guard_input.auth.get("roles", []):
                return GuardResult.allow(auth={"role": "admin"})
            return GuardResult.forbidden("Admin access required")

        @module(guards=[create_guard(name="admin", handler=admin_guard)])
        class AppModule:
            pass
    """
    meta = dict(metadata or {})
    if inject:
        meta["inject"] = inject
    return GuardDefinition(name=name, handler=handler, metadata=meta)
