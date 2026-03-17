"""Guard input and result types."""

from dataclasses import dataclass
from typing import Any, Self


@dataclass
class GuardInput:
    """Input provided to a guard's validate method."""

    token: str
    method: str
    path: str
    headers: dict[str, str | list[str]]
    query: dict[str, str | list[str]]
    cookies: dict[str, str]
    body: str | None
    request_id: str
    client_ip: str
    auth: dict[str, Any]
    handler_name: str | None = None


@dataclass
class GuardResult:
    """Outcome of a guard evaluation."""

    allowed: bool
    auth: dict[str, Any] | None = None
    status_code: int | None = None
    message: str | None = None

    @classmethod
    def allow(cls, auth: dict[str, Any] | None = None) -> Self:
        """Create an allow result with optional auth claims."""
        return cls(allowed=True, auth=auth)

    @classmethod
    def forbidden(cls, message: str = "") -> Self:
        """Create a forbidden (403) deny result."""
        return cls(allowed=False, status_code=403, message=message)

    @classmethod
    def unauthorized(cls, message: str = "") -> Self:
        """Create an unauthorized (401) deny result."""
        return cls(allowed=False, status_code=401, message=message)
