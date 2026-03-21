"""SqlDatabaseLayer system layer."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from celerity.config.service import CONFIG_SERVICE_TOKEN, RESOURCE_CONFIG_NAMESPACE
from celerity.resources._common import (
    capture_resource_links,
    detect_runtime_mode,
    get_links_of_type,
)
from celerity.resources.sql_database.config import resolve_pool_config
from celerity.resources.sql_database.credentials import resolve_database_credentials
from celerity.resources.sql_database.factory import create_credentials, create_sql_database
from celerity.resources.sql_database.params import (
    DEFAULT_SQL_CREDENTIALS_TOKEN,
    DEFAULT_SQL_READER_TOKEN,
    DEFAULT_SQL_WRITER_TOKEN,
    sql_credentials_token,
    sql_instance_token,
    sql_reader_token,
    sql_writer_token,
)
from celerity.types.layer import CelerityLayer

if TYPE_CHECKING:
    from celerity.resources.sql_database.types import SqlDatabaseInstance

logger = logging.getLogger("celerity.sql_database")


class SqlDatabaseLayer(CelerityLayer):
    """System layer for SQL database resources.

    On first request, resolves SQL database resource links, creates
    SqlDatabaseInstance(s), and registers DI tokens for writer/reader
    engines and credentials.
    """

    def __init__(self) -> None:
        self._initialized = False
        self._instances: list[SqlDatabaseInstance] = []

    async def handle(self, context: Any, next_handler: Any) -> Any:
        if not self._initialized:
            container = context.container
            await self._init(container)
            self._initialized = True
        return await next_handler()

    async def _init(self, container: Any) -> None:
        links = capture_resource_links()
        sql_links = get_links_of_type(links, "sqlDatabase")
        if not sql_links:
            return

        config_service = await container.resolve(CONFIG_SERVICE_TOKEN)
        resource_config = config_service.namespace(RESOURCE_CONFIG_NAMESPACE)
        runtime_mode = detect_runtime_mode()

        for resource_name, config_key in sql_links.items():
            connection_info, auth = await resolve_database_credentials(resource_config, config_key)
            pool_config = resolve_pool_config(runtime_mode)
            instance = create_sql_database(connection_info, auth, pool_config)
            self._instances.append(instance)

            credentials = create_credentials(connection_info, auth)

            container.register_value(sql_instance_token(resource_name), instance)
            container.register_value(sql_writer_token(resource_name), instance.writer())
            container.register_value(sql_reader_token(resource_name), instance.reader())
            container.register_value(sql_credentials_token(resource_name), credentials)

        if len(sql_links) == 1:
            only_name = next(iter(sql_links))
            writer = await container.resolve(sql_writer_token(only_name))
            reader = await container.resolve(sql_reader_token(only_name))
            creds = await container.resolve(sql_credentials_token(only_name))
            container.register_value(DEFAULT_SQL_WRITER_TOKEN, writer)
            container.register_value(DEFAULT_SQL_READER_TOKEN, reader)
            container.register_value(DEFAULT_SQL_CREDENTIALS_TOKEN, creds)

        logger.debug("sql_database: registered %d resource(s)", len(sql_links))

    async def dispose(self) -> None:
        for instance in self._instances:
            await instance.close()
