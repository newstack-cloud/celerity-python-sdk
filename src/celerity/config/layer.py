"""ConfigLayer system layer."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

from celerity.config.backends.empty import EmptyConfigBackend
from celerity.config.service import (
    CONFIG_SERVICE_TOKEN,
    ConfigService,
    ConfigServiceImpl,
)
from celerity.resources._common import detect_platform, detect_runtime_mode
from celerity.types.layer import CelerityLayer

if TYPE_CHECKING:
    from celerity.config.backends.types import ConfigBackend

logger = logging.getLogger("celerity.config")


class ConfigLayer(CelerityLayer):
    """System layer that initialises ConfigService.

    Reads config store settings from environment:

    - ``CELERITY_CONFIG_STORE_ID`` -- single namespace store ID
    - ``CELERITY_CONFIG_STORE_KIND`` -- ``"parameter-store"`` or
      ``"secrets-manager"`` (default ``"secrets-manager"``)
    - ``CELERITY_RUNTIME`` -- present = runtime, absent = functions

    Registers ``ConfigService`` in the DI container under
    ``CONFIG_SERVICE_TOKEN``. Runs once (idempotent init).
    """

    def __init__(self) -> None:
        self._initialized = False

    async def handle(self, context: Any, next_handler: Any) -> Any:
        if not self._initialized:
            container = context.container
            await self._init(container)
            self._initialized = True
        return await next_handler()

    async def _init(self, container: Any) -> None:
        config_service = ConfigService()
        platform = detect_platform()
        runtime_mode = detect_runtime_mode()

        store_id = os.environ.get("CELERITY_CONFIG_STORE_ID")
        store_kind = os.environ.get("CELERITY_CONFIG_STORE_KIND", "secrets-manager")

        if store_id:
            backend = _resolve_backend(platform, store_kind, runtime_mode)
            data = await backend.fetch(store_id)
            ns = ConfigServiceImpl(data)
            config_service.register_namespace("resources", ns)
            logger.debug(
                "config: loaded %d keys from %s (%s/%s)",
                len(data),
                store_id,
                platform,
                store_kind,
            )
        else:
            config_service.register_namespace("resources", ConfigServiceImpl())
            logger.debug("config: no CELERITY_CONFIG_STORE_ID, using empty config")

        # Check for additional namespaces (CELERITY_CONFIG_<NS>_STORE_ID pattern).
        for key, value in os.environ.items():
            if key.startswith("CELERITY_CONFIG_") and key.endswith("_STORE_ID"):
                if key == "CELERITY_CONFIG_STORE_ID":
                    continue
                prefix_len = len("CELERITY_CONFIG_")
                suffix_len = len("_STORE_ID")
                ns_name = key[prefix_len:-suffix_len].lower()
                kind_key = f"CELERITY_CONFIG_{ns_name.upper()}_STORE_KIND"
                ns_kind = os.environ.get(kind_key, store_kind)
                ns_backend = _resolve_backend(platform, ns_kind, runtime_mode)
                ns_data = await ns_backend.fetch(value)
                config_service.register_namespace(ns_name, ConfigServiceImpl(ns_data))
                logger.debug("config: loaded namespace %s (%d keys)", ns_name, len(ns_data))

        container.register_value(CONFIG_SERVICE_TOKEN, config_service)

        # Register each namespace under its own DI token for handler injection.
        from celerity.config.params import config_namespace_token

        for ns_name in config_service.namespace_names:
            registered_ns = config_service.namespace(ns_name)
            container.register_value(config_namespace_token(ns_name), registered_ns)


def _resolve_backend(
    platform: str,
    store_kind: str,
    runtime_mode: str,
) -> ConfigBackend:
    """Select the config backend based on platform and store kind."""
    if platform == "local":
        from celerity.config.backends.local import LocalConfigBackend

        return LocalConfigBackend()

    if platform == "aws":
        if store_kind == "parameter-store":
            from celerity.config.backends.aws.parameter_store import (
                AwsParameterStoreBackend,
            )

            return AwsParameterStoreBackend()

        # secrets-manager (default for AWS)
        if runtime_mode == "functions" and os.environ.get(
            "PARAMETERS_SECRETS_EXTENSION_HTTP_PORT",
        ):
            from celerity.config.backends.aws.lambda_extension import (
                AwsLambdaExtensionBackend,
            )

            return AwsLambdaExtensionBackend()

        from celerity.config.backends.aws.secrets_manager import (
            AwsSecretsManagerBackend,
        )

        return AwsSecretsManagerBackend()

    return EmptyConfigBackend()
