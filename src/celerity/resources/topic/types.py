"""Topic resource ABCs and publish types."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


class TopicClient(ABC):
    """Top-level topic client managing the underlying connection.

    Resource name to provider-specific identifier mapping (e.g. SNS
    topic ARN) is resolved at construction time so ``topic()`` takes
    only the logical resource name.
    """

    @abstractmethod
    def topic(self, name: str) -> Topic:
        """Get a topic handle for a named resource.

        Args:
            name: The logical resource name. The provider maps this
                to a physical identifier internally.
        """

    @abstractmethod
    async def close(self) -> None:
        """Close the underlying connection."""


class Topic(ABC):
    """Per-resource topic handle for publishing messages."""

    @abstractmethod
    async def publish(
        self,
        body: str,
        options: PublishOptions | None = None,
    ) -> str:
        """Publish a single message to the topic.

        Returns the message ID assigned by the provider.
        """

    @abstractmethod
    async def publish_batch(
        self,
        entries: list[BatchPublishEntry],
    ) -> BatchPublishResult:
        """Publish a batch of messages to the topic.

        Auto-chunks entries exceeding the provider batch limit (10 for SNS).
        Returns a result containing successful and failed entries.
        """


@dataclass(frozen=True, slots=True)
class PublishOptions:
    """Options for a single publish operation."""

    group_id: str | None = None
    deduplication_id: str | None = None
    subject: str | None = None
    attributes: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class BatchPublishEntry:
    """A single entry in a batch publish request."""

    id: str
    body: str
    group_id: str | None = None
    deduplication_id: str | None = None
    subject: str | None = None
    attributes: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class BatchPublishSuccess:
    """A single successful entry in a batch publish result."""

    id: str
    message_id: str


@dataclass(frozen=True, slots=True)
class BatchPublishFailure:
    """A single failed entry in a batch publish result."""

    id: str
    code: str
    message: str
    sender_fault: bool


@dataclass(frozen=True, slots=True)
class BatchPublishResult:
    """Result of a batch publish operation."""

    successful: list[BatchPublishSuccess] = field(default_factory=list)
    failed: list[BatchPublishFailure] = field(default_factory=list)
