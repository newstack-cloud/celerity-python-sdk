"""Consumer (message queue / event source) types."""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class SourceType(StrEnum):
    BUCKET = "bucket"
    DATASTORE = "datastore"
    QUEUE = "queue"
    TOPIC = "topic"


class BucketEventType(StrEnum):
    CREATED = "created"
    DELETED = "deleted"
    METADATA_UPDATED = "metadataUpdated"


class DatastoreEventType(StrEnum):
    INSERTED = "inserted"
    MODIFIED = "modified"
    REMOVED = "removed"


@dataclass
class ConsumerMessage:
    """A single message from a consumer event source."""

    message_id: str
    body: str
    source: str
    source_type: str | None = None
    source_name: str | None = None
    event_type: str | None = None
    message_attributes: dict[str, Any] | None = None
    vendor: Any = None


@dataclass
class ConsumerEventInput:
    """Batch of messages delivered to a consumer handler."""

    handler_tag: str
    messages: list[ConsumerMessage]
    vendor: Any = None
    trace_context: dict[str, str] | None = None


@dataclass
class MessageProcessingFailure:
    """Identifies a message that failed processing."""

    message_id: str
    error_message: str | None = None


@dataclass
class EventResult:
    """Result of processing a consumer or schedule event."""

    success: bool
    failures: list[MessageProcessingFailure] | None = None
    error_message: str | None = None


@dataclass
class ValidatedConsumerMessage[T]:
    """A consumer message with a parsed and validated body."""

    message_id: str
    body: str
    source: str
    parsed_body: T | None = None
    source_type: str | None = None
    source_name: str | None = None
    event_type: str | None = None
    message_attributes: dict[str, Any] | None = None
    vendor: Any = None


@dataclass
class BucketEvent:
    """Parsed body for bucket change events."""

    key: str
    size: int | None = None
    e_tag: str | None = None


@dataclass
class DatastoreEvent:
    """Parsed body for datastore change events."""

    keys: dict[str, Any] = field(default_factory=dict)
    new_item: dict[str, Any] | None = None
    old_item: dict[str, Any] | None = None
