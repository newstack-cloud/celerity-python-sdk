"""Module system types."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from celerity.types.common import InjectionToken
    from celerity.types.container import Provider


@dataclass
class FunctionHandlerDefinition:
    """A function-based handler registered via a module."""

    type: str
    handler: Callable[..., Any]
    metadata: dict[str, Any] = field(default_factory=dict)
    id: str | None = None


@dataclass
class GuardDefinition:
    """A function-based guard registered via a module."""

    name: str
    handler: Callable[..., Any]
    metadata: dict[str, Any] | None = None


@dataclass
class ModuleMetadata:
    """Metadata stored by the @module decorator."""

    controllers: list[type] | None = None
    function_handlers: list[FunctionHandlerDefinition] | None = None
    guards: list[type | GuardDefinition] | None = None
    providers: list[type | Provider] | None = None
    imports: list[type] | None = None
    exports: list[InjectionToken] | None = None
    layers: list[Any] | None = None
