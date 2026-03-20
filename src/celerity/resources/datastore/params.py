"""Datastore parameter types and helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from celerity.resources._tokens import default_token, resource_token
from celerity.resources.datastore.types import Datastore

if TYPE_CHECKING:
    from celerity.types.container import ServiceContainer


class DatastoreParam:
    """DI marker for datastore injection.

    Used inside ``Annotated[...]`` to tell the DI resolver which
    datastore resource to inject.

    Usage::

        from typing import Annotated
        from celerity.resources.datastore import Datastore, DatastoreParam

        # Default datastore:
        DatastoreResource = Annotated[Datastore, DatastoreParam()]

        # Named datastore:
        OrdersDb = Annotated[Datastore, DatastoreParam("orders")]
    """

    resource_type: str = "datastore"

    def __init__(self, name: str | None = None) -> None:
        self.resource_name = name


def datastore_token(resource_name: str) -> str:
    """Create a DI token for a named datastore resource."""
    return resource_token("datastore", resource_name)


DEFAULT_DATASTORE_TOKEN = default_token("datastore")

DatastoreResource = Annotated[Datastore, DatastoreParam()]
"""Default datastore injection type.

Type checker sees ``Datastore``, DI resolves via ``celerity:datastore:default``.
"""


async def get_datastore(
    container: ServiceContainer,
    resource_name: str | None = None,
) -> Datastore:
    """Resolve a Datastore handle from the container without DI.

    Args:
        container: The DI container.
        resource_name: Optional resource name. If ``None``, resolves the
            default datastore.
    """
    token = datastore_token(resource_name) if resource_name else DEFAULT_DATASTORE_TOKEN
    result: Datastore = await container.resolve(token)
    return result
