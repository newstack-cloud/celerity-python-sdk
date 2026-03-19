"""Config backend ABC."""

from __future__ import annotations

from abc import ABC, abstractmethod


class ConfigBackend(ABC):
    """Backend for fetching configuration data from a store."""

    @abstractmethod
    async def fetch(self, store_id: str) -> dict[str, str]:
        """Fetch all key-value pairs from a config store.

        Args:
            store_id: The store identifier (e.g., Parameter Store path,
                Secrets Manager secret ID).

        Returns:
            Dict of config key-value pairs.
        """
