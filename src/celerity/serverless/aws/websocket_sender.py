"""WebSocket sender using API Gateway Management API."""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import Any

logger = logging.getLogger("celerity.serverless.aws")


class ApiGatewayWebSocketSender:
    """Send messages to connected WebSocket clients via API Gateway.

    Args:
        endpoint_url: The API Gateway Management API endpoint URL
            (e.g. ``https://{api-id}.execute-api.{region}.amazonaws.com/{stage}``).
    """

    def __init__(self, endpoint_url: str) -> None:
        self._endpoint_url = endpoint_url

    @asynccontextmanager
    async def _client(self) -> Any:
        import aioboto3

        session = aioboto3.Session()
        async with session.client(
            "apigatewaymanagementapi",
            endpoint_url=self._endpoint_url,
        ) as client:
            yield client

    async def send_message(
        self,
        connection_id: str,
        data: Any,
        **kwargs: Any,
    ) -> None:
        """Send a message to a connected WebSocket client.

        Args:
            connection_id: The WebSocket connection ID.
            data: The message data (will be JSON-encoded if not a string).
            **kwargs: Additional options (reserved for future use).
        """
        payload = data if isinstance(data, str) else json.dumps(data)
        async with self._client() as client:
            await client.post_to_connection(
                ConnectionId=connection_id,
                Data=payload.encode("utf-8"),
            )
        logger.debug("sent message to connection %s", connection_id)
