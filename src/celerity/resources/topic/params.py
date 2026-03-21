"""Topic parameter types and helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from celerity.resources._tokens import default_token, resource_token
from celerity.resources.topic.types import Topic

if TYPE_CHECKING:
    from celerity.types.container import ServiceContainer


class TopicParam:
    """DI marker for topic injection.

    Used inside ``Annotated[...]`` to tell the DI resolver which
    topic resource to inject.
    """

    resource_type: str = "topic"

    def __init__(self, name: str | None = None) -> None:
        self.resource_name = name


def topic_token(resource_name: str) -> str:
    """Create a DI token for a named topic resource."""
    return resource_token("topic", resource_name)


DEFAULT_TOPIC_TOKEN = default_token("topic")

TopicResource = Annotated[Topic, TopicParam()]
"""Default topic injection type.

Type checker sees ``Topic``, DI resolves via ``celerity:topic:default``.
"""


async def get_topic(
    container: ServiceContainer,
    resource_name: str | None = None,
) -> Topic:
    """Resolve a Topic handle from the container without DI.

    Args:
        container: The DI container.
        resource_name: Optional resource name. If ``None``, resolves the
            default topic.
    """
    token = topic_token(resource_name) if resource_name else DEFAULT_TOPIC_TOKEN
    result: Topic = await container.resolve(token)
    return result
