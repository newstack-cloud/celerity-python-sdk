"""ConfigService and ConfigNamespace ABCs."""

from __future__ import annotations

from abc import ABC, abstractmethod

CONFIG_SERVICE_TOKEN = "ConfigService"
"""DI token for the ConfigService."""

RESOURCE_CONFIG_NAMESPACE = "resources"
"""Well-known namespace containing deploy-time infrastructure identifiers."""


class ConfigNamespace(ABC):
    """A namespace within the configuration store.

    Each namespace maps string keys to string values. The "resources"
    namespace contains infrastructure identifiers for all resource types
    (e.g., ``ordersDb_host``, ``appCache_port``).
    """

    @abstractmethod
    async def get(self, key: str) -> str | None:
        """Get a config value by key, or ``None`` if not set."""

    @abstractmethod
    async def get_or_throw(self, key: str) -> str:
        """Get a config value by key, raising if not found.

        Raises:
            KeyError: If the key is not found.
        """

    @abstractmethod
    async def get_all(self) -> dict[str, str]:
        """Get all key-value pairs in this namespace."""


class ConfigServiceImpl(ConfigNamespace):
    """In-memory config namespace backed by a dict.

    Used by ConfigLayer after fetching values from the backend.
    Supports lazy refresh for long-lived runtime mode.
    """

    def __init__(self, data: dict[str, str] | None = None) -> None:
        self._data: dict[str, str] = data or {}

    async def get(self, key: str) -> str | None:
        return self._data.get(key)

    async def get_or_throw(self, key: str) -> str:
        value = self._data.get(key)
        if value is None:
            msg = f"Config key not found: {key}"
            raise KeyError(msg)
        return value

    async def get_all(self) -> dict[str, str]:
        return dict(self._data)

    def set_data(self, data: dict[str, str]) -> None:
        """Replace the backing data (used during refresh)."""
        self._data = data


class ConfigService:
    """Provides access to named configuration namespaces.

    Namespaces are registered at init time by the ConfigLayer.
    The ``"resources"`` namespace is always present when resource
    links are configured.
    """

    def __init__(self) -> None:
        self._namespaces: dict[str, ConfigNamespace] = {}
        self._default: ConfigNamespace | None = None

    def register_namespace(self, name: str, ns: ConfigNamespace) -> None:
        """Register a namespace."""
        self._namespaces[name] = ns
        if len(self._namespaces) == 1:
            self._default = ns

    def namespace(self, name: str) -> ConfigNamespace:
        """Get a namespace by name.

        Raises:
            KeyError: If the namespace is not registered.
        """
        ns = self._namespaces.get(name)
        if ns is None:
            msg = f"Config namespace not found: {name}"
            raise KeyError(msg)
        return ns

    def default_namespace(self) -> ConfigNamespace | None:
        """Get the default namespace (when exactly one is registered)."""
        return self._default if len(self._namespaces) == 1 else None

    @property
    def namespace_names(self) -> list[str]:
        """List registered namespace names."""
        return list(self._namespaces)
