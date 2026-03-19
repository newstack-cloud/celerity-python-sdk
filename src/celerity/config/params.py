"""Config parameter decorator and DI token helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from celerity.decorators.params import ParamMeta

if TYPE_CHECKING:
    from celerity.config.service import ConfigNamespace, ConfigService
    from celerity.types.container import ServiceContainer


def config_namespace_token(name: str) -> str:
    """Create a DI token for a named config namespace.

    Example::

        config_namespace_token("appConfig") -> "celerity:config:appConfig"
    """
    return f"celerity:config:{name}"


class Config:
    """Parameter type for injecting a ``ConfigNamespace`` into a handler.

    The namespace name is required — there is no default config namespace.

    Usage::

        @get("/settings")
        async def get_settings(self, config: Config["appConfig"]) -> HttpResponse:
            api_key = await config.get("api_key")
            ...
    """

    __celerity_param__ = ParamMeta(type="config")

    def __class_getitem__(cls, namespace_name: str) -> type:
        """Create a typed Config parameter for a specific namespace."""
        return type(
            f"Config[{namespace_name}]",
            (),
            {
                "__celerity_param__": ParamMeta(type="config", key=namespace_name),
            },
        )


async def get_config_service(container: ServiceContainer) -> ConfigService:
    """Programmatic helper to resolve the ConfigService from the container.

    Use this when you need access to the full service (e.g. listing
    namespaces or accessing multiple namespaces).

    Args:
        container: The DI container.

    Returns:
        The ``ConfigService`` instance.
    """
    from celerity.config.service import CONFIG_SERVICE_TOKEN

    result: ConfigService = await container.resolve(CONFIG_SERVICE_TOKEN)
    return result


async def get_config_namespace(
    container: ServiceContainer,
    namespace: str,
) -> ConfigNamespace:
    """Programmatic helper to resolve a config namespace from the container.

    Args:
        container: The DI container.
        namespace: The namespace name (e.g. ``"appConfig"``).

    Returns:
        The ``ConfigNamespace`` instance.
    """
    token = config_namespace_token(namespace)
    result: ConfigNamespace = await container.resolve(token)
    return result
