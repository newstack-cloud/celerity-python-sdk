"""AWS Systems Manager Parameter Store config backend."""

from __future__ import annotations

import logging

from celerity.config.backends.types import ConfigBackend

logger = logging.getLogger("celerity.config")


class AwsParameterStoreBackend(ConfigBackend):
    """Fetches config from AWS SSM Parameter Store.

    Reads all parameters under the store ID path using
    ``GetParametersByPath`` with decryption enabled.
    Uses ``aioboto3`` for async I/O.
    """

    async def fetch(self, store_id: str) -> dict[str, str]:
        import aioboto3

        session = aioboto3.Session()
        result: dict[str, str] = {}

        async with session.client("ssm") as client:
            paginator = client.get_paginator("get_parameters_by_path")
            async for page in paginator.paginate(
                Path=store_id,
                Recursive=True,
                WithDecryption=True,
            ):
                for param in page.get("Parameters", []):
                    name = param["Name"]
                    key = name[len(store_id) :].lstrip("/")
                    result[key] = param["Value"]

        logger.debug("parameter-store: fetched %d params from %s", len(result), store_id)
        return result
