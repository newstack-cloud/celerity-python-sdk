"""Runtime metadata store passed through the handler pipeline."""

from typing import Any


class HandlerMetadataStore:
    """Key-value bag for request-scoped metadata.

    Instantiated per-request in the handler pipeline. Carries validated
    body/query/params and custom metadata through layers and handlers.

    Args:
        initial: Optional initial key-value pairs.
    """

    def __init__(self, initial: dict[str, Any] | None = None) -> None:
        self._data: dict[str, Any] = dict(initial) if initial else {}

    def get(self, key: str) -> Any:
        """Get a metadata value by key.

        Args:
            key: The metadata key.

        Returns:
            The stored value, or ``None`` if not set.
        """
        return self._data.get(key)

    def set(self, key: str, value: Any) -> None:
        """Set a metadata value.

        Args:
            key: The metadata key.
            value: The value to store.
        """
        self._data[key] = value

    def has(self, key: str) -> bool:
        """Check if a key exists.

        Args:
            key: The metadata key.

        Returns:
            ``True`` if the key is present.
        """
        return key in self._data
