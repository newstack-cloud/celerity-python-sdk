"""AWS Secrets Manager config backend."""

from __future__ import annotations

import json
import logging

from celerity.config.backends.types import ConfigBackend

logger = logging.getLogger("celerity.config")


class AwsSecretsManagerBackend(ConfigBackend):
    """Fetches config from AWS Secrets Manager.

    Reads the secret value as JSON and returns key-value pairs.
    Uses ``aioboto3`` for async I/O.
    """

    async def fetch(self, store_id: str) -> dict[str, str]:
        import aioboto3

        session = aioboto3.Session()
        async with session.client("secretsmanager") as client:
            response = await client.get_secret_value(SecretId=store_id)
            secret_string = response.get("SecretString", "{}")

        try:
            data = json.loads(secret_string)
        except (json.JSONDecodeError, TypeError):
            logger.warning("secrets-manager: invalid JSON in secret %s", store_id)
            return {}

        if isinstance(data, dict):
            result = {k: str(v) for k, v in data.items()}
            logger.debug("secrets-manager: fetched %d keys from %s", len(result), store_id)
            return result

        return {}
