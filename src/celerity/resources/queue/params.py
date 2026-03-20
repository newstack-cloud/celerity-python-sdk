"""Queue parameter types and helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from celerity.resources._tokens import default_token, resource_token
from celerity.resources.queue.types import Queue

if TYPE_CHECKING:
    from celerity.types.container import ServiceContainer


class QueueParam:
    """DI marker for queue injection.

    Used inside ``Annotated[...]`` to tell the DI resolver which
    queue resource to inject.

    Usage::

        from typing import Annotated
        from celerity.resources.queue import Queue, QueueParam

        # Default queue:
        QueueResource = Annotated[Queue, QueueParam()]

        # Named queue:
        OrdersQueue = Annotated[Queue, QueueParam("orders")]
    """

    resource_type: str = "queue"

    def __init__(self, name: str | None = None) -> None:
        self.resource_name = name


def queue_token(resource_name: str) -> str:
    """Create a DI token for a named queue resource."""
    return resource_token("queue", resource_name)


DEFAULT_QUEUE_TOKEN = default_token("queue")

QueueResource = Annotated[Queue, QueueParam()]
"""Default queue injection type.

Type checker sees ``Queue``, DI resolves via ``celerity:queue:default``.
"""


async def get_queue(
    container: ServiceContainer,
    resource_name: str | None = None,
) -> Queue:
    """Resolve a Queue handle from the container without DI.

    Args:
        container: The DI container.
        resource_name: Optional resource name. If ``None``, resolves the
            default queue.
    """
    token = queue_token(resource_name) if resource_name else DEFAULT_QUEUE_TOKEN
    result: Queue = await container.resolve(token)
    return result
