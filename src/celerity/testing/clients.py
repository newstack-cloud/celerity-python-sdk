"""Real resource client factories for integration testing."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

if TYPE_CHECKING:
    from celerity.testing.blueprint import BlueprintResource
    from celerity.testing.discovery import ResourceTokenInfo


async def create_real_clients(
    tokens: list[ResourceTokenInfo],
    blueprint_resources: dict[str, BlueprintResource],
) -> tuple[dict[str, Any], list[Any]]:
    """Create real resource clients from env vars set by ``celerity dev test``.

    Returns:
        A tuple of (handles_dict, closeables_list) where handles_dict maps
        token strings to resource handles and closeables_list contains clients
        that need closing.
    """
    handles: dict[str, Any] = {}
    closeables: list[Any] = []

    by_type: dict[str, list[ResourceTokenInfo]] = {}
    for info in tokens:
        by_type.setdefault(info.type, []).append(info)

    for resource_type, infos in by_type.items():
        if resource_type == "datastore":
            await _create_datastore_handles(infos, blueprint_resources, handles, closeables)
        elif resource_type == "topic":
            await _create_topic_handles(infos, blueprint_resources, handles, closeables)
        elif resource_type == "queue":
            await _create_queue_handles(infos, blueprint_resources, handles, closeables)
        elif resource_type == "cache":
            await _create_cache_handles(infos, handles, closeables)
        elif resource_type == "bucket":
            await _create_bucket_handles(infos, blueprint_resources, handles, closeables)
        elif resource_type in ("sqlDatabase", "sqlDatabase:writer", "sqlDatabase:reader"):
            await _create_sql_handles(infos, blueprint_resources, handles, closeables)
        elif resource_type == "config":
            await _create_config_handles(infos, handles)

    return handles, closeables


def _physical_name(info: ResourceTokenInfo, bp: dict[str, BlueprintResource]) -> str:
    res = bp.get(info.name)
    return res.physical_name if res else info.name


async def _create_datastore_handles(
    infos: list[ResourceTokenInfo],
    bp: dict[str, BlueprintResource],
    handles: dict[str, Any],
    closeables: list[Any],
) -> None:
    from celerity.resources.datastore.factory import create_datastore_client

    resource_ids = {info.name: _physical_name(info, bp) for info in infos}
    client = create_datastore_client(provider="local", resource_ids=resource_ids)
    closeables.append(client)
    for info in infos:
        handles[info.token] = client.datastore(info.name)


async def _create_topic_handles(
    infos: list[ResourceTokenInfo],
    bp: dict[str, BlueprintResource],
    handles: dict[str, Any],
    closeables: list[Any],
) -> None:
    from celerity.resources.topic.factory import create_topic_client

    resource_ids = {info.name: _physical_name(info, bp) for info in infos}
    client = create_topic_client(provider="local", resource_ids=resource_ids)
    closeables.append(client)
    for info in infos:
        handles[info.token] = client.topic(info.name)


async def _create_queue_handles(
    infos: list[ResourceTokenInfo],
    bp: dict[str, BlueprintResource],
    handles: dict[str, Any],
    closeables: list[Any],
) -> None:
    from celerity.resources.queue.factory import create_queue_client

    resource_ids = {info.name: _physical_name(info, bp) for info in infos}
    client = create_queue_client(provider="local", resource_ids=resource_ids)
    closeables.append(client)
    for info in infos:
        handles[info.token] = client.queue(info.name)


async def _create_cache_handles(
    infos: list[ResourceTokenInfo],
    handles: dict[str, Any],
    closeables: list[Any],
) -> None:
    from celerity.resources.cache.config import resolve_connection_config
    from celerity.resources.cache.credentials import (
        CacheConnectionInfo,
        CachePasswordAuth,
    )
    from celerity.resources.cache.providers.redis.client import (
        create_redis_cache_client,
    )

    endpoint = os.environ.get("CELERITY_REDIS_ENDPOINT", "redis://localhost:6379")
    parsed = urlparse(endpoint)
    connection_info = CacheConnectionInfo(
        host=parsed.hostname or "localhost",
        port=parsed.port or 6379,
        tls=False,
        cluster_mode=False,
        auth_mode="password",
        user=None,
        key_prefix="",
        region=None,
    )
    auth = CachePasswordAuth(password=None)
    connection_config = resolve_connection_config("local")
    client = await create_redis_cache_client(
        connection_info,
        auth,
        connection_config,
    )
    closeables.append(client)
    for info in infos:
        handles[info.token] = client.cache(info.name)


async def _create_bucket_handles(
    infos: list[ResourceTokenInfo],
    bp: dict[str, BlueprintResource],
    handles: dict[str, Any],
    closeables: list[Any],
) -> None:
    from celerity.resources.bucket.factory import create_object_storage

    resource_ids = {info.name: _physical_name(info, bp) for info in infos}
    client = create_object_storage(provider="local", resource_ids=resource_ids)
    closeables.append(client)
    for info in infos:
        handles[info.token] = client.bucket(info.name)


async def _create_config_handles(
    infos: list[ResourceTokenInfo],
    handles: dict[str, Any],
) -> None:
    from celerity.config.backends.local import LocalConfigBackend
    from celerity.config.service import ConfigServiceImpl

    backend = LocalConfigBackend()

    for info in infos:
        # Look up the store ID from env: CELERITY_CONFIG_<KEY>_STORE_ID
        env_key = info.name.upper()
        store_id_env = f"CELERITY_CONFIG_{env_key}_STORE_ID"
        store_id = os.environ.get(store_id_env)
        if store_id:
            data = await backend.fetch(store_id)
        else:
            data = {}
        handles[info.token] = ConfigServiceImpl(data)


async def _create_sql_handles(
    infos: list[ResourceTokenInfo],
    bp: dict[str, BlueprintResource],
    handles: dict[str, Any],
    closeables: list[Any],
) -> None:
    conn_str = os.environ.get(
        "CELERITY_LOCAL_SQL_DATABASE_ENDPOINT",
        "postgresql+asyncpg://celerity:celerity@localhost:5432/celerity",
    )

    from celerity.resources.sql_database.config import (
        RUNTIME_POOL,
        build_connection_url,
    )
    from celerity.resources.sql_database.factory import create_sql_database
    from celerity.resources.sql_database.types import (
        SqlConnectionInfo,
        SqlPasswordAuth,
    )

    parsed = urlparse(conn_str)
    scheme = parsed.scheme or ""
    if scheme.startswith("mysql"):
        engine = "mysql"
        default_port = 3306
    else:
        engine = "postgres"
        default_port = 5432
    host = parsed.hostname or "localhost"
    port = parsed.port or default_port
    user = parsed.username or "celerity"
    password = parsed.password or "celerity"

    # Group by resource name so writer/reader for the same resource share an instance.
    by_resource: dict[str, list[ResourceTokenInfo]] = {}
    for info in infos:
        by_resource.setdefault(info.name, []).append(info)

    for resource_infos in by_resource.values():
        database = _physical_name(resource_infos[0], bp)
        sqlalchemy_url = build_connection_url(
            engine,
            user,
            password,
            host,
            port,
            database,
            ssl=False,
        )
        connection_info = SqlConnectionInfo(
            host=host,
            port=port,
            database=database,
            user=user,
            engine=engine,
            ssl=False,
            auth_mode="password",
        )
        auth = SqlPasswordAuth(
            password=password,
            url=sqlalchemy_url,
        )
        instance = create_sql_database(connection_info, auth, RUNTIME_POOL)
        closeables.append(instance)
        for info in resource_infos:
            if "reader" in info.type:
                handles[info.token] = instance.reader() or instance.writer()
            else:
                handles[info.token] = instance.writer()
