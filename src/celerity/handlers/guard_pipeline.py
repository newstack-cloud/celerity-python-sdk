"""Single guard execution pipeline."""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from celerity.errors.http_exception import HttpError
from celerity.metadata.store import HandlerMetadataStore
from celerity.types.guard import GuardInput
from celerity.types.guard import GuardResult as UserGuardResult

if TYPE_CHECKING:
    from celerity.types.container import ServiceContainer
    from celerity.types.handler import ResolvedGuard

logger = logging.getLogger("celerity.pipeline.guard")


@dataclass
class GuardPipelineOptions:
    """Options for executing a single guard."""

    container: ServiceContainer
    handler_metadata: dict[str, Any] | None = None


@dataclass
class GuardResult:
    """Outcome of a guard pipeline execution.

    Mirrors the runtime's expected result format:
    - allowed=True: ``auth`` contains claims to store under ``request.auth.<guardName>``
    - allowed=False: ``status_code`` and ``message`` describe the rejection
    """

    allowed: bool
    auth: dict[str, Any] | None = None
    status_code: int | None = None
    message: str | None = None
    details: Any = None


async def execute_guard_pipeline(
    guard: ResolvedGuard,
    guard_input: GuardInput,
    options: GuardPipelineOptions,
) -> GuardResult:
    """Execute a single guard handler and return its result.

    The runtime manages guard chain orchestration (ordering, auth
    accumulation, short-circuiting on denial). This function is the
    SDK callback that the runtime invokes once per guard.

    Args:
        guard: The resolved guard to execute.
        guard_input: Input assembled by the runtime (token, request info,
            accumulated auth from prior guards).
        options: Pipeline options including container and handler metadata.

    Returns:
        A ``GuardResult`` indicating whether access is allowed.
    """
    metadata = HandlerMetadataStore({
        **(guard.custom_metadata or {}),
        **(options.handler_metadata or {}),
    })

    try:
        result = (
            await _invoke_function_guard(guard, guard_input, metadata, options)
            if guard.is_function_guard
            else await _invoke_class_guard(guard, guard_input, metadata, options)
        )

        return _to_pipeline_result(guard.name, result)
    except HttpError as exc:
        logger.debug("guard %s — rejected %d: %s", guard.name, exc.status_code, exc.message)
        return GuardResult(
            allowed=False,
            status_code=exc.status_code,
            message=exc.message,
        )
    except Exception:
        logger.debug("guard %s — unexpected error", guard.name, exc_info=True)
        return GuardResult(allowed=False, status_code=401, message="Unauthorized")


def _to_pipeline_result(guard_name: str, result: Any) -> GuardResult:
    """Convert a guard handler return value to a pipeline ``GuardResult``.

    Supports two return styles:
    - ``UserGuardResult`` (explicit allow/deny with ``allowed`` field)
    - Raw dict or value (Node SDK style — return auth claims to allow,
      raise ``HttpError`` to deny)
    """
    if isinstance(result, UserGuardResult):
        if result.allowed:
            logger.debug("guard %s — allowed", guard_name)
            return GuardResult(allowed=True, auth=result.auth or {})
        logger.debug(
            "guard %s — rejected %d: %s",
            guard_name, result.status_code, result.message,
        )
        return GuardResult(
            allowed=False,
            status_code=result.status_code or 403,
            message=result.message or "Forbidden",
        )

    # Raw return (dict or other value) — treated as allowed with auth claims.
    logger.debug("guard %s — allowed", guard_name)
    auth = result if isinstance(result, dict) else (result or {})
    return GuardResult(allowed=True, auth=auth)


async def _invoke_class_guard(
    guard: ResolvedGuard,
    guard_input: GuardInput,
    metadata: HandlerMetadataStore,
    options: GuardPipelineOptions,
) -> Any:
    """Invoke a class-based guard (``@guard`` decorator).

    The scanner stores ``handler_fn`` as a bound method on the resolved
    instance, so ``self`` is already captured. Decorated parameters are
    filled from their annotation type; undecorated parameters receive the
    full ``GuardHandlerContext``.
    """
    from celerity.types.context import GuardHandlerContext

    guard_context = GuardHandlerContext(
        token=guard_input.token,
        auth=guard_input.auth,
        request=guard_input,
        metadata=metadata,
        container=options.container,
    )

    # handler_fn is a bound method — inspect its non-self parameters.
    sig = inspect.signature(guard.handler_fn)
    params = [p for p in sig.parameters.values() if p.name != "self"]
    decorated_indices = {m.index for m in guard.param_metadata}

    args: list[Any] = [None] * len(params)

    for meta in guard.param_metadata:
        if meta.type == "token":
            args[meta.index] = guard_context.token
        elif meta.type == "auth":
            args[meta.index] = guard_context.auth

    # Undecorated params receive the full guard context.
    for i in range(len(params)):
        if i not in decorated_indices:
            args[i] = guard_context

    result = guard.handler_fn(*args)
    if inspect.isawaitable(result):
        result = await result
    return result


async def _invoke_function_guard(
    guard: ResolvedGuard,
    guard_input: GuardInput,
    metadata: HandlerMetadataStore,
    options: GuardPipelineOptions,
) -> Any:
    """Invoke a function-based guard (``create_guard()``)."""
    from celerity.types.context import GuardContext

    ctx = GuardContext(
        metadata=metadata,
        container=options.container,
        auth=guard_input.auth,
    )

    deps: list[Any] = []
    if guard.inject_tokens:
        for token in guard.inject_tokens:
            deps.append(await options.container.resolve(token))

    result = guard.handler_fn(guard_input, ctx, *deps)
    if inspect.isawaitable(result):
        result = await result
    return result


async def _resolve_guard_instance(
    guard: ResolvedGuard, container: ServiceContainer,
) -> object | None:
    """Lazily resolve the guard instance from the DI container."""
    if guard.handler_instance is not None:
        return guard.handler_instance
    if guard.guard_class is None:
        return None
    instance: object = await container.resolve(guard.guard_class)
    guard.handler_instance = instance
    return instance
