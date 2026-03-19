"""ElastiCache IAM authentication token provider."""

from __future__ import annotations

import time


class ElastiCacheTokenProvider:
    """Generates SigV4 presigned tokens for ElastiCache IAM auth.

    Creates a presigned GET request to::

        https://{cache_id}/?Action=connect&User={user_id}

    Tokens are valid for 15 minutes.  Cached for ~14 minutes to
    ensure refresh before expiry.
    """

    _TOKEN_TTL_SECONDS = 15 * 60
    _REFRESH_BUFFER_SECONDS = 60

    def __init__(self, cache_id: str, user_id: str, region: str) -> None:
        self._cache_id = cache_id
        self._user_id = user_id
        self._region = region
        self._cached_token: str | None = None
        self._token_expiry: float = 0.0

    async def get_token(self) -> str:
        """Return a fresh or cached IAM auth token."""
        now = time.monotonic()
        if self._cached_token and now < self._token_expiry:
            return self._cached_token

        token = self._generate_token()
        self._cached_token = token
        self._token_expiry = now + self._TOKEN_TTL_SECONDS - self._REFRESH_BUFFER_SECONDS
        return token

    def _generate_token(self) -> str:
        """Generate a new SigV4 presigned token using botocore."""
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.session import Session

        session = Session()
        resolved = session.get_credentials()
        if resolved is None:
            raise RuntimeError("No AWS credentials found for ElastiCache IAM auth")
        credentials = resolved.get_frozen_credentials()

        url = f"https://{self._cache_id}/?Action=connect&User={self._user_id}"
        request = AWSRequest(method="GET", url=url)
        SigV4Auth(credentials, "elasticache", self._region).add_auth(request)

        # The signed URL (with auth in query string) is the token.
        return request.url or ""
