"""Async DI container with singleton scope and lifecycle management."""

from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any

from celerity.di.dependency_tokens import get_class_dependency_tokens
from celerity.metadata.keys import INJECTABLE, get_metadata
from celerity.types.container import ClassProvider, FactoryProvider, ServiceContainer, ValueProvider

if TYPE_CHECKING:
    from collections.abc import Callable

    from celerity.types.container import Provider

logger = logging.getLogger("celerity.di")

_CLOSE_METHODS = ("close", "aclose", "end", "quit", "disconnect", "destroy")


class Container(ServiceContainer):
    """Async DI container with singleton scope and lifecycle management.

    Resolves dependencies via:

    - ``ClassProvider``: constructs a class, resolving its ``__init__``
      type hints as dependencies.
    - ``FactoryProvider``: calls a factory function with injected deps.
    - ``ValueProvider``: returns a pre-built value.
    - Auto-resolution: ``@injectable()`` classes resolved via type hints
      without explicit registration.

    All resolved instances are cached as singletons.

    Example::

        container = Container()
        container.register(DatabaseClient, ClassProvider(use_class=PostgresClient))
        container.register_value("API_KEY", "sk-123")

        db = await container.resolve(DatabaseClient)
        key = await container.resolve("API_KEY")

        await container.close_all()
    """

    def __init__(self) -> None:
        self._providers: dict[Any, Provider] = {}
        self._instances: dict[Any, Any] = {}
        self._resolving: set[Any] = set()
        self._close_stack: list[tuple[Any, Callable[[], Any]]] = []
        self._tracked: set[Any] = set()
        self._resolve_hooks: list[Callable[[Any, Container], bool]] = []

    def register(self, token: Any, provider: Provider) -> None:
        """Register a provider for a token.

        Args:
            token: The DI token (class or string).
            provider: A ``ClassProvider``, ``FactoryProvider``, or
                ``ValueProvider`` instance.
        """
        logger.debug("register %s (%s)", _token_str(token), type(provider).__name__)
        self._providers[token] = provider

    def register_class(self, target: type) -> None:
        """Register a class as its own provider.

        Args:
            target: The class to register. It will be constructed via
                its ``__init__`` type hints when resolved.
        """
        logger.debug("register %s (class)", target.__name__)
        self._providers[target] = ClassProvider(use_class=target)

    def add_resolve_hook(self, hook: Callable[[Any, Container], bool]) -> None:
        """Register a hook called when a token has no provider.

        The hook receives ``(token, container)`` and should return ``True``
        if it registered a provider for the token (so resolution can retry),
        or ``False`` to let the container raise as normal.
        """
        self._resolve_hooks.append(hook)

    def register_value(self, token: Any, value: Any) -> None:
        """Register a pre-built value.

        Args:
            token: The DI token.
            value: The value to return when this token is resolved.
        """
        logger.debug("register_value %s", _token_str(token))
        self._instances[token] = value
        self._track_closeable(token, value)

    async def resolve(self, token: Any) -> Any:
        """Resolve a dependency by token.

        Returns a cached singleton if the token has already been resolved.

        Args:
            token: The DI token to resolve.

        Returns:
            The resolved instance.

        Raises:
            RuntimeError: If a circular dependency is detected or no
                provider is registered for the token.
        """
        if token in self._instances:
            logger.debug("resolve %s → cached", _token_str(token))
            return self._instances[token]

        if token in self._resolving:
            path = " -> ".join(
                t.__name__ if isinstance(t, type) else str(t) for t in [*self._resolving, token]
            )
            raise RuntimeError(f"Circular dependency detected: {path}")

        logger.debug("resolve %s → constructing", _token_str(token))
        self._resolving.add(token)
        try:
            provider = self._providers.get(token)
            if provider is None:
                if isinstance(token, type):
                    return await self._construct_class(token)
                # Try resolve hooks (e.g. parsed config registration).
                for hook in self._resolve_hooks:
                    if hook(token, self):
                        provider = self._providers.get(token)
                        break
                if provider is None:
                    raise RuntimeError(
                        f"No provider registered for {_token_str(token)}.\n"
                        "Ensure the module providing it is included in your "
                        "root module's imports, or register a provider for it "
                        "directly."
                    )

            instance = await self._create_from_provider(provider)
            self._instances[token] = instance
            if not isinstance(provider, ValueProvider):
                self._track_closeable(token, instance, provider.on_close)
            return instance
        finally:
            self._resolving.discard(token)

    def has(self, token: Any) -> bool:
        """Check if a token has a provider or cached instance.

        Args:
            token: The DI token to check.
        """
        return token in self._instances or token in self._providers

    def get_dependencies(self, token: Any) -> set[Any]:
        """Get direct dependency tokens for a registered provider.

        Args:
            token: The DI token.

        Returns:
            A set of dependency tokens, or an empty set if unresolvable.
        """
        provider = self._providers.get(token)
        if provider is None:
            if isinstance(token, type):
                return set(get_class_dependency_tokens(token))
            return set()
        if isinstance(provider, ClassProvider):
            return set(get_class_dependency_tokens(provider.use_class))
        if isinstance(provider, FactoryProvider):
            return set(provider.inject or [])
        return set()

    async def close_all(self) -> None:
        """Close all tracked resources in reverse registration order.

        Exceptions from individual close calls are logged but do not
        prevent other resources from being closed.
        """
        logger.debug("close_all: %d resources", len(self._close_stack))
        for token, close_fn in reversed(self._close_stack):
            try:
                logger.debug("closing %s", _token_str(token))
                result = close_fn()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("Error closing %s", _token_str(token))
        self._close_stack.clear()
        self._tracked.clear()

    def validate_dependencies(self) -> None:
        """Validate that all registered providers can be resolved.

        Collects ALL missing dependencies and reports them in a single
        error with per-consumer detail, making it easy to identify what
        needs to be registered or imported.

        Raises:
            RuntimeError: If missing providers are detected, with a
                detailed listing of each consumer and its unresolved
                dependency.
        """
        missing: list[tuple[str, str]] = []
        visited: set[Any] = set()

        def walk(token: Any) -> None:
            if token in visited:
                return
            visited.add(token)

            for dep in self.get_dependencies(token):
                if self.has(dep) or isinstance(dep, type):
                    walk(dep)
                else:
                    missing.append((_token_str(token), _token_str(dep)))

        for token in self._providers:
            walk(token)

        if missing:
            details = "\n".join(
                f"  {consumer} requires {dep} — no provider registered" for consumer, dep in missing
            )
            raise RuntimeError(
                "Unresolvable dependencies detected during bootstrap:\n\n"
                f"{details}\n\n"
                "For each unresolved dependency, check that the module "
                "providing it is included in your root module's imports, "
                "or register a provider for it directly."
            )

    async def _construct_class(self, target: type) -> Any:
        if target in self._instances:
            return self._instances[target]

        is_injectable = get_metadata(target, INJECTABLE) is True
        init = target.__init__  # type: ignore[misc]

        if init is object.__init__:
            instance = target()
            self._instances[target] = instance
            self._track_closeable(target, instance)
            return instance

        sig = inspect.signature(init)
        params = [p for p in sig.parameters.values() if p.name != "self"]

        if not is_injectable and len(params) > 0:
            raise RuntimeError(
                f"Class {target.__name__} has constructor parameters but is not "
                f"decorated with @injectable(). Add @injectable() to enable DI."
            )

        dep_tokens = get_class_dependency_tokens(target)
        logger.debug(
            "construct %s deps=[%s]", target.__name__, ", ".join(_token_str(t) for t in dep_tokens)
        )
        sig_params = [p for p in sig.parameters.values() if p.name != "self"]

        deps: list[Any] = []
        for i, token_for_dep in enumerate(dep_tokens):
            param_name = sig_params[i].name if i < len(sig_params) else f"#{i}"
            try:
                deps.append(await self.resolve(token_for_dep))
            except RuntimeError as exc:
                raise RuntimeError(
                    f"Failed to resolve parameter '{param_name}' "
                    f"(position {i}) of {target.__name__}.\n"
                    f"  Token: {_token_str(token_for_dep)}\n"
                    f"  Cause: {exc}"
                ) from exc

        instance = target(*deps)
        self._instances[target] = instance
        self._track_closeable(target, instance)
        return instance

    async def _create_from_provider(self, provider: Provider) -> Any:
        if isinstance(provider, ValueProvider):
            return provider.use_value
        if isinstance(provider, ClassProvider):
            return await self._construct_class(provider.use_class)
        if isinstance(provider, FactoryProvider):
            deps = [await self.resolve(t) for t in (provider.inject or [])]
            result = provider.use_factory(*deps)
            if inspect.isawaitable(result):
                result = await result
            return result
        msg = f"Invalid provider type: {type(provider)}"
        raise TypeError(msg)

    def _track_closeable(
        self,
        token: Any,
        value: Any,
        on_close: Callable[[Any], Any] | None = None,
    ) -> None:
        if token in self._tracked:
            return
        if on_close:
            self._close_stack.append((token, lambda: on_close(value)))
            self._tracked.add(token)
            return
        close_fn = _detect_close_method(value)
        if close_fn:
            self._close_stack.append((token, close_fn))
            self._tracked.add(token)


def _token_str(token: Any) -> str:
    if isinstance(token, type):
        return token.__name__
    return str(token)


def _detect_close_method(value: object) -> Callable[[], Any] | None:
    for method_name in _CLOSE_METHODS:
        method = getattr(value, method_name, None)
        if callable(method):
            return method  # type: ignore[no-any-return]
    return None
