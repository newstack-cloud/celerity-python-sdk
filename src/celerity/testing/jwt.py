"""JWT token generation via the dev auth server."""

from __future__ import annotations

import os
from typing import Any

import httpx


async def generate_test_token(
    *,
    sub: str = "test-user",
    claims: dict[str, Any] | None = None,
    expires_in: str = "1h",
) -> str:
    """Generate an RS256-signed JWT by calling the local dev auth server.

    The dev auth server runs as a sidecar in ``celerity dev test`` /
    ``celerity dev run`` and is accessible via the ``CELERITY_DEV_AUTH_BASE_URL``
    env var.

    Args:
        sub: JWT subject claim.
        claims: Arbitrary claims spread into the JWT payload.
        expires_in: Token lifetime as a Go-style duration string.

    Returns:
        The signed access token string.

    Raises:
        httpx.HTTPStatusError: If the dev auth server returns an error.
        httpx.ConnectError: If the dev auth server is unreachable.
    """
    base_url = os.environ.get("CELERITY_DEV_AUTH_BASE_URL", "http://localhost:9099")

    body: dict[str, Any] = {"sub": sub}
    if claims:
        body["claims"] = claims
    if expires_in != "1h":
        body["expiresIn"] = expires_in

    async with httpx.AsyncClient() as client:
        response = await client.post(f"{base_url}/token", json=body)
        response.raise_for_status()
        data: dict[str, Any] = response.json()
        return str(data.get("access_token", ""))
