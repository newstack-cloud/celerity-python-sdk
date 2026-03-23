"""Service container ABC and provider types."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from celerity.types.common import InjectionToken


@dataclass
class ClassProvider:
    """Provide a dependency by constructing a class."""

    use_class: type
    on_close: Callable[[Any], Any] | None = None


@dataclass
class FactoryProvider:
    """Provide a dependency via a factory function."""

    use_factory: Callable[..., Any]
    inject: list[InjectionToken] | None = None
    on_close: Callable[[Any], Any] | None = None


@dataclass
class ValueProvider:
    """Provide a pre-built value as a dependency."""

    use_value: Any
    on_close: Callable[[Any], Any] | None = None


type Provider = ClassProvider | FactoryProvider | ValueProvider


class ServiceContainer(ABC):
    """Abstract DI container contract."""

    @abstractmethod
    async def resolve(self, token: InjectionToken) -> Any: ...

    @abstractmethod
    def register(self, token: InjectionToken, provider: Provider) -> None: ...

    @abstractmethod
    def has(self, token: InjectionToken) -> bool: ...

    @abstractmethod
    def add_resolve_hook(self, hook: Callable[[Any, ServiceContainer], bool]) -> None:
        """Register a hook called when a token has no provider."""
        ...

    @abstractmethod
    async def close_all(self) -> None: ...
