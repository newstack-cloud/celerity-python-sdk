"""Bucket parameter types and helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated

from celerity.resources._tokens import default_token, resource_token
from celerity.resources.bucket.types import Bucket

if TYPE_CHECKING:
    from celerity.types.container import ServiceContainer


class BucketParam:
    """DI marker for bucket injection.

    Used inside ``Annotated[...]`` to tell the DI resolver which
    bucket resource to inject.

    Usage::

        from typing import Annotated
        from celerity.resources.bucket import Bucket, BucketParam

        # Default bucket:
        BucketResource = Annotated[Bucket, BucketParam()]

        # Named bucket:
        ImagesBucket = Annotated[Bucket, BucketParam("images")]
    """

    resource_type: str = "bucket"

    def __init__(self, name: str | None = None) -> None:
        self.resource_name = name


def bucket_token(resource_name: str) -> str:
    """Create a DI token for a named bucket resource."""
    return resource_token("bucket", resource_name)


DEFAULT_BUCKET_TOKEN = default_token("bucket")

BucketResource = Annotated[Bucket, BucketParam()]
"""Default bucket injection type.

Type checker sees ``Bucket``, DI resolves via ``celerity:bucket:default``.
"""


async def get_bucket(
    container: ServiceContainer,
    resource_name: str | None = None,
) -> Bucket:
    """Resolve a Bucket handle from the container without DI.

    Args:
        container: The DI container.
        resource_name: Optional resource name. If ``None``, resolves the
            default bucket.
    """
    token = bucket_token(resource_name) if resource_name else DEFAULT_BUCKET_TOKEN
    result: Bucket = await container.resolve(token)
    return result
