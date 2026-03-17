"""Handler context types passed through the pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from celerity.metadata.store import HandlerMetadataStore
    from celerity.types.consumer import ConsumerEventInput
    from celerity.types.container import ServiceContainer
    from celerity.types.http import HttpRequest
    from celerity.types.schedule import ScheduleEventInput
    from celerity.types.websocket import WebSocketMessage


@dataclass
class BaseHandlerContext:
    """Base context shared by all handler pipelines."""

    metadata: HandlerMetadataStore
    container: ServiceContainer
    logger: Any | None = None


@dataclass
class HttpHandlerContext(BaseHandlerContext):
    """Context for HTTP handler pipelines."""

    request: HttpRequest | None = None


@dataclass
class WebSocketHandlerContext(BaseHandlerContext):
    """Context for WebSocket handler pipelines."""

    message: WebSocketMessage | None = None


@dataclass
class ConsumerHandlerContext(BaseHandlerContext):
    """Context for consumer handler pipelines."""

    event: ConsumerEventInput | None = None


@dataclass
class ScheduleHandlerContext(BaseHandlerContext):
    """Context for schedule handler pipelines."""

    event: ScheduleEventInput | None = None
