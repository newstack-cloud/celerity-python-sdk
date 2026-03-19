"""Empty config backend (returns no data)."""

from __future__ import annotations

from celerity.config.backends.types import ConfigBackend


class EmptyConfigBackend(ConfigBackend):
    """Backend that returns an empty dict.

    Used when no config store is configured or the platform
    is not recognized.
    """

    async def fetch(self, store_id: str) -> dict[str, str]:
        return {}
