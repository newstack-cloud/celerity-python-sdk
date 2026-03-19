"""WebSocket sender using API Gateway Management API."""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger("celerity.serverless.aws")


class ApiGatewayWebSocketSender:
    """Send messages to connected WebSocket clients via API Gateway.

    Uses ``boto3`` to call the API Gateway Management API. The client
    is created lazily on first use.

    Args:
        endpoint_url: The API Gateway Management API endpoint URL
            (e.g. ``https://{api-id}.execute-api.{region}.amazonaws.com/{stage}``).
    """

    def __init__(self, endpoint_url: str) -> None:
        self._endpoint_url = endpoint_url
        self._client: Any = None

    def _get_client(self) -> Any:
        if self._client is None:
            import boto3

            self._client = boto3.client(
                "apigatewaymanagementapi",
                endpoint_url=self._endpoint_url,
            )
        return self._client

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
        client = self._get_client()
        payload = data if isinstance(data, str) else json.dumps(data)
        client.post_to_connection(
            ConnectionId=connection_id,
            Data=payload.encode("utf-8"),
        )
        logger.debug("sent message to connection %s", connection_id)
