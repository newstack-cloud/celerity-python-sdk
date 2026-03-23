"""Queue resource package."""

from celerity.resources.queue.errors import QueueError
from celerity.resources.queue.params import (
    DEFAULT_QUEUE_TOKEN,
    QueueParam,
    QueueResource,
    get_queue,
    queue_token,
)
from celerity.resources.queue.types import (
    BatchSendEntry,
    BatchSendFailure,
    BatchSendResult,
    BatchSendSuccess,
    Queue,
    QueueClient,
    SendMessageOptions,
)
from celerity.resources.serialise import MessageBody

__all__ = [
    "DEFAULT_QUEUE_TOKEN",
    "BatchSendEntry",
    "BatchSendFailure",
    "BatchSendResult",
    "BatchSendSuccess",
    "MessageBody",
    "Queue",
    "QueueClient",
    "QueueError",
    "QueueParam",
    "QueueResource",
    "SendMessageOptions",
    "get_queue",
    "queue_token",
]
