"""WebSocket types."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class WebSocketEventType(StrEnum):
    CONNECT = "connect"
    MESSAGE = "message"
    DISCONNECT = "disconnect"


class WebSocketMessageType(StrEnum):
    JSON = "json"
    BINARY = "binary"


@dataclass
class WebSocketRequestContext:
    """Connection-level context from the WebSocket upgrade request."""

    request_id: str
    request_time: float
    path: str
    protocol_version: str
    headers: dict[str, str | list[str]]
    user_agent: str | None = None
    client_ip: str = ""
    query: dict[str, str | list[str]] | None = None
    cookies: dict[str, str] | None = None
    auth: dict[str, Any] | None = None
    trace_context: dict[str, str] | None = None


@dataclass
class WebSocketMessage:
    """A single WebSocket event (connect, message, or disconnect)."""

    message_type: WebSocketMessageType
    event_type: WebSocketEventType
    connection_id: str
    message_id: str
    json_body: Any = None
    binary_body: bytes | None = None
    request_context: WebSocketRequestContext | None = None
    trace_context: dict[str, str] | None = None


@dataclass
class WebSocketSendOptions:
    """Options for sending a WebSocket message."""

    message_id: str | None = None
    message_type: WebSocketMessageType | None = None


class WebSocketSender(ABC):
    """Interface for sending messages to connected WebSocket clients."""

    @abstractmethod
    async def send_message(
        self,
        connection_id: str,
        data: Any,
        options: WebSocketSendOptions | None = None,
    ) -> None: ...


WEBSOCKET_SENDER_TOKEN = "celerity:websocket-sender"
"""DI token for WebSocketSender injection."""
