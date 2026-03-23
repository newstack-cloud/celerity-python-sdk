"""Queue resource ABCs and message types."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celerity.resources.serialise import MessageBody


class QueueClient(ABC):
    """Top-level queue client managing provider connections.

    Resource name to provider-specific identifier mapping (e.g. SQS
    queue URL) is resolved at construction time so ``queue()`` takes
    only the logical resource name.
    """

    @abstractmethod
    def queue(self, name: str) -> Queue:
        """Get a queue handle for a named queue resource.

        Args:
            name: The logical resource name. The provider maps this
                to a physical identifier internally.
        """

    @abstractmethod
    async def close(self) -> None:
        """Close the underlying provider connection."""


class Queue(ABC):
    """Per-queue handle for sending messages.

    The ``body`` argument accepts a plain string, a dict, a dataclass,
    a Pydantic model, or any JSON-serializable value.  Non-string values
    are automatically serialized to a JSON string before being sent.
    """

    @abstractmethod
    async def send_message(
        self,
        body: MessageBody,
        options: SendMessageOptions | None = None,
    ) -> str:
        """Send a single message to the queue.

        Returns the message ID assigned by the provider.
        """

    @abstractmethod
    async def send_message_batch(
        self,
        entries: list[BatchSendEntry],
    ) -> BatchSendResult:
        """Send a batch of messages to the queue.

        Automatically chunks batches larger than the provider limit
        (10 for SQS). Returns a result containing successful and
        failed entries.
        """


@dataclass(frozen=True, slots=True)
class SendMessageOptions:
    """Options for sending a single message."""

    group_id: str | None = None
    deduplication_id: str | None = None
    delay_seconds: int | None = None
    attributes: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class BatchSendEntry:
    """A single entry in a batch send request."""

    id: str
    body: MessageBody
    group_id: str | None = None
    deduplication_id: str | None = None
    delay_seconds: int | None = None
    attributes: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class BatchSendSuccess:
    """A successfully sent message in a batch."""

    id: str
    message_id: str


@dataclass(frozen=True, slots=True)
class BatchSendFailure:
    """A failed message in a batch."""

    id: str
    code: str
    message: str
    sender_fault: bool


@dataclass(frozen=True, slots=True)
class BatchSendResult:
    """Result of a batch send operation."""

    successful: list[BatchSendSuccess] = field(default_factory=list)
    failed: list[BatchSendFailure] = field(default_factory=list)
