"""Config parameter types and helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any

if TYPE_CHECKING:
    from celerity.config.service import ConfigNamespace, ConfigService
    from celerity.types.container import ServiceContainer


def config_namespace_token(name: str) -> str:
    """Create a DI token for a named config namespace.

    Example::

        config_namespace_token("appConfig") -> "celerity:config:appConfig"
    """
    return f"celerity:config:{name}"


class ConfigParam:
    """DI marker for config namespace injection.

    Used inside ``Annotated[...]`` to tell the DI resolver which
    config namespace to inject.

    Usage::

        from typing import Annotated
        from celerity.config import ConfigParam, ConfigNamespace

        # Default config namespace:
        ConfigResource = Annotated[ConfigNamespace, ConfigParam()]

        # Named config namespace:
        AppConfig = Annotated[ConfigNamespace, ConfigParam("appConfig")]

        @injectable()
        class SettingsService:
            def __init__(self, config: AppConfig) -> None:
                self.config = config  # type checker: ConfigNamespace
    """

    resource_type: str = "config"

    def __init__(self, name: str | None = None) -> None:
        self.resource_name = name


ConfigResource = Annotated[Any, ConfigParam()]
"""Default config namespace injection type.

Since config always requires a namespace name, prefer creating
a named alias::

    from typing import Annotated
    from celerity.config import ConfigParam, ConfigNamespace

    AppConfig = Annotated[ConfigNamespace, ConfigParam("appConfig")]
"""


async def get_config_service(container: ServiceContainer) -> ConfigService:
    """Resolve the ConfigService from the container.

    Use this when you need access to the full service (e.g. listing
    namespaces or accessing multiple namespaces).
    """
    from celerity.config.service import CONFIG_SERVICE_TOKEN

    result: ConfigService = await container.resolve(CONFIG_SERVICE_TOKEN)
    return result


async def get_config_namespace(
    container: ServiceContainer,
    namespace: str,
) -> ConfigNamespace:
    """Resolve a config namespace from the container.

    Args:
        container: The DI container.
        namespace: The namespace name (e.g. ``"appConfig"``).
    """
    token = config_namespace_token(namespace)
    result: ConfigNamespace = await container.resolve(token)
    return result
