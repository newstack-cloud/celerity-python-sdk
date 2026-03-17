"""Handler types: registry entries, parameter metadata, and resolved handlers."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from celerity.types.common import InjectionToken, Schema
    from celerity.types.layer import CelerityLayer


class HandlerType(StrEnum):
    HTTP = "http"
    WEBSOCKET = "websocket"
    CONSUMER = "consumer"
    SCHEDULE = "schedule"
    CUSTOM = "custom"


type HttpMethod = str
"""HTTP method string: GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS."""


@dataclass
class ParamMetadata:
    """Metadata for a single handler parameter injection."""

    index: int
    type: str
    key: str | None = None
    schema: Schema[Any] | None = None


@dataclass
class ResolvedHandlerBase:
    """Base fields shared by all resolved handler types."""

    handler_fn: Callable[..., Any]
    handler_instance: object | None = None
    controller_class: type | None = None
    is_function_handler: bool = False
    inject_tokens: list[InjectionToken] | None = None
    layers: list[CelerityLayer | type[CelerityLayer]] = field(default_factory=list)
    param_metadata: list[ParamMetadata] = field(default_factory=list)
    custom_metadata: dict[str, Any] = field(default_factory=dict)
    id: str | None = None


@dataclass
class ResolvedHttpHandler(ResolvedHandlerBase):
    type: HandlerType = HandlerType.HTTP
    path: str | None = None
    method: str | None = None
    protected_by: list[str] = field(default_factory=list)
    is_public: bool = False


@dataclass
class ResolvedWebSocketHandler(ResolvedHandlerBase):
    type: HandlerType = HandlerType.WEBSOCKET
    route: str = ""
    protected_by: list[str] = field(default_factory=list)
    is_public: bool = False


@dataclass
class ResolvedConsumerHandler(ResolvedHandlerBase):
    type: HandlerType = HandlerType.CONSUMER
    handler_tag: str = ""


@dataclass
class ResolvedScheduleHandler(ResolvedHandlerBase):
    type: HandlerType = HandlerType.SCHEDULE
    handler_tag: str = ""


@dataclass
class ResolvedCustomHandler(ResolvedHandlerBase):
    type: HandlerType = HandlerType.CUSTOM
    name: str = ""


type ResolvedHandler = (
    ResolvedHttpHandler
    | ResolvedWebSocketHandler
    | ResolvedConsumerHandler
    | ResolvedScheduleHandler
    | ResolvedCustomHandler
)


@dataclass
class ResolvedGuard:
    """A guard that has been resolved from the module graph."""

    name: str
    handler_fn: Callable[..., Any]
    handler_instance: object | None = None
    guard_class: type | None = None
    param_metadata: list[ParamMetadata] = field(default_factory=list)
    custom_metadata: dict[str, Any] = field(default_factory=dict)
    inject_tokens: list[InjectionToken] | None = None
    is_function_guard: bool = False
