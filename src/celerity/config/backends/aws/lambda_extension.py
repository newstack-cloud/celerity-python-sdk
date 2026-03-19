"""AWS Lambda Extensions cache backend for Secrets Manager."""

from __future__ import annotations

import json
import logging
import os

from celerity.config.backends.types import ConfigBackend

logger = logging.getLogger("celerity.config")


class AwsLambdaExtensionBackend(ConfigBackend):
    """Fetches secrets via the AWS Parameters and Secrets Lambda Extension.

    The extension runs as a sidecar on ``localhost`` and caches secret
    values, avoiding direct Secrets Manager API calls on every invocation.
    Falls back to the direct Secrets Manager backend if the extension
    is not available.
    """

    def __init__(self) -> None:
        port = os.environ.get("PARAMETERS_SECRETS_EXTENSION_HTTP_PORT", "2773")
        self._base_url = f"http://localhost:{port}"
        self._session_token = os.environ.get("AWS_SESSION_TOKEN", "")

    async def fetch(self, store_id: str) -> dict[str, str]:
        url = f"{self._base_url}/secretsmanager/get?secretId={store_id}"
        try:
            return await self._fetch_from_extension(url)
        except Exception:
            logger.debug("lambda-extension: falling back to direct SDK for %s", store_id)
            from celerity.config.backends.aws.secrets_manager import AwsSecretsManagerBackend

            fallback = AwsSecretsManagerBackend()
            return await fallback.fetch(store_id)

    async def _fetch_from_extension(self, url: str) -> dict[str, str]:
        """Fetch from the Lambda extension HTTP endpoint using aiohttp."""
        import aiohttp

        headers = {"X-Aws-Parameters-Secrets-Token": self._session_token}
        async with (
            aiohttp.ClientSession() as session,
            session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp,
        ):
            resp.raise_for_status()
            body = await resp.text()

        data = json.loads(body)
        secret_string = data.get("SecretString", "{}")
        parsed = json.loads(secret_string)
        if isinstance(parsed, dict):
            result = {k: str(v) for k, v in parsed.items()}
            logger.debug("lambda-extension: fetched %d keys", len(result))
            return result
        return {}
