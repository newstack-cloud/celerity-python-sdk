"""Integration tests for the local config backend (Valkey)."""

from __future__ import annotations

import json

import redis.asyncio as aioredis

from celerity.config.backends.local import LocalConfigBackend

from .conftest import VALKEY_URL

STORE_KEY = "celerity-test:config:app"


class TestLocalConfigBackend:
    async def test_fetches_json_config_from_valkey(self) -> None:
        """Store JSON config in Valkey and fetch it via the backend."""
        client = aioredis.from_url(VALKEY_URL)
        try:
            config_data = {
                "appCache_host": "redis.example.com",
                "appCache_port": "6379",
                "ordersDb_host": "dynamodb.example.com",
            }
            await client.set(STORE_KEY, json.dumps(config_data))

            backend = LocalConfigBackend(redis_url=VALKEY_URL)
            result = await backend.fetch(STORE_KEY)

            assert result == config_data
        finally:
            await client.delete(STORE_KEY)
            await client.close()

    async def test_returns_empty_for_missing_key(self) -> None:
        """Missing store key returns empty dict."""
        backend = LocalConfigBackend(redis_url=VALKEY_URL)
        result = await backend.fetch("celerity-test:nonexistent")
        assert result == {}

    async def test_returns_empty_for_non_json_value(self) -> None:
        """Non-JSON value in Valkey returns empty dict."""
        client = aioredis.from_url(VALKEY_URL)
        try:
            await client.set(STORE_KEY, "not-json")

            backend = LocalConfigBackend(redis_url=VALKEY_URL)
            result = await backend.fetch(STORE_KEY)
            assert result == {}
        finally:
            await client.delete(STORE_KEY)
            await client.close()

    async def test_converts_non_string_values(self) -> None:
        """Non-string values in JSON are converted to strings."""
        client = aioredis.from_url(VALKEY_URL)
        try:
            config_data = {"port": 6379, "tls": True, "name": "cache"}
            await client.set(STORE_KEY, json.dumps(config_data))

            backend = LocalConfigBackend(redis_url=VALKEY_URL)
            result = await backend.fetch(STORE_KEY)

            assert result == {"port": "6379", "tls": "True", "name": "cache"}
        finally:
            await client.delete(STORE_KEY)
            await client.close()
