"""Integration test for ConfigLayer with real backends."""

from __future__ import annotations

import contextlib
import json
from types import SimpleNamespace
from typing import Any

import boto3
import redis.asyncio as aioredis

from celerity.config.layer import ConfigLayer
from celerity.config.service import CONFIG_SERVICE_TOKEN, ConfigService

from .conftest import LOCALSTACK_ENDPOINT, VALKEY_URL

LOCAL_STORE_KEY = "celerity-integration-test:config"
SSM_PATH = "/celerity-integration-test/config/"
SECRET_ID = "celerity-integration-test/config"


class _MockContainer:
    def __init__(self) -> None:
        self._values: dict[str, Any] = {}

    def register_value(self, token: str, value: Any) -> None:
        self._values[token] = value

    def get(self, token: str) -> Any:
        return self._values.get(token)


class TestConfigLayerWithValkey:
    async def test_loads_config_from_valkey(
        self,
        monkeypatch: Any,
    ) -> None:
        """ConfigLayer loads config from Valkey in local platform mode."""
        client = aioredis.from_url(VALKEY_URL)
        try:
            config = {"cache_host": "redis.local", "cache_port": "6379"}
            await client.set(LOCAL_STORE_KEY, json.dumps(config))

            monkeypatch.setenv("CELERITY_PLATFORM", "local")
            monkeypatch.setenv("CELERITY_CONFIG_STORE_ID", LOCAL_STORE_KEY)
            monkeypatch.setenv("CELERITY_REDIS_ENDPOINT", VALKEY_URL)
            monkeypatch.delenv("CELERITY_RUNTIME", raising=False)

            container = _MockContainer()
            layer = ConfigLayer()

            async def noop() -> None:
                pass

            await layer.handle(SimpleNamespace(container=container), noop)

            svc: ConfigService = container.get(CONFIG_SERVICE_TOKEN)
            ns = svc.namespace("resources")
            assert await ns.get("cache_host") == "redis.local"
            assert await ns.get("cache_port") == "6379"
        finally:
            await client.delete(LOCAL_STORE_KEY)
            await client.close()


class TestConfigLayerWithLocalStack:
    async def test_loads_config_from_parameter_store(
        self,
        monkeypatch: Any,
    ) -> None:
        """ConfigLayer loads config from SSM Parameter Store."""
        ssm = boto3.client(
            "ssm",
            endpoint_url=LOCALSTACK_ENDPOINT,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        try:
            ssm.put_parameter(
                Name=f"{SSM_PATH}db_host",
                Value="pg.example.com",
                Type="String",
                Overwrite=True,
            )

            monkeypatch.setenv("CELERITY_PLATFORM", "aws")
            monkeypatch.setenv("CELERITY_CONFIG_STORE_ID", SSM_PATH)
            monkeypatch.setenv("CELERITY_CONFIG_STORE_KIND", "parameter-store")
            monkeypatch.delenv("CELERITY_RUNTIME", raising=False)

            container = _MockContainer()
            layer = ConfigLayer()

            async def noop() -> None:
                pass

            await layer.handle(SimpleNamespace(container=container), noop)

            svc: ConfigService = container.get(CONFIG_SERVICE_TOKEN)
            ns = svc.namespace("resources")
            assert await ns.get("db_host") == "pg.example.com"
        finally:
            with contextlib.suppress(Exception):
                ssm.delete_parameter(Name=f"{SSM_PATH}db_host")

    async def test_loads_config_from_secrets_manager(
        self,
        monkeypatch: Any,
    ) -> None:
        """ConfigLayer loads config from Secrets Manager."""
        sm = boto3.client(
            "secretsmanager",
            endpoint_url=LOCALSTACK_ENDPOINT,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        try:
            sm.create_secret(
                Name=SECRET_ID,
                SecretString=json.dumps({"api_key": "secret-123"}),
            )

            monkeypatch.setenv("CELERITY_PLATFORM", "aws")
            monkeypatch.setenv("CELERITY_CONFIG_STORE_ID", SECRET_ID)
            monkeypatch.setenv("CELERITY_CONFIG_STORE_KIND", "secrets-manager")
            monkeypatch.delenv("CELERITY_RUNTIME", raising=False)

            container = _MockContainer()
            layer = ConfigLayer()

            async def noop() -> None:
                pass

            await layer.handle(SimpleNamespace(container=container), noop)

            svc: ConfigService = container.get(CONFIG_SERVICE_TOKEN)
            ns = svc.namespace("resources")
            assert await ns.get("api_key") == "secret-123"
        finally:
            with contextlib.suppress(Exception):
                sm.delete_secret(
                    SecretId=SECRET_ID,
                    ForceDeleteWithoutRecovery=True,
                )
