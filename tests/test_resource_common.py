"""Tests for shared resource infrastructure."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

from celerity.resources._common import (
    ResourceLink,
    capture_resource_links,
    detect_platform,
    detect_runtime_mode,
    get_links_of_type,
    get_resource_types,
)
from celerity.resources._tokens import (
    default_token,
    is_resource_layer_token,
    resource_token,
)

# ---------------------------------------------------------------------------
# Resource links
# ---------------------------------------------------------------------------


class TestCaptureResourceLinks:
    def test_parses_valid_json(self, resource_links_file: Callable[[dict[str, Any]], Path]) -> None:
        resource_links_file(
            {
                "orders-db": {"type": "datastore", "configKey": "ordersDb"},
                "app-cache": {"type": "cache", "configKey": "appCache"},
            }
        )
        links = capture_resource_links()

        assert len(links) == 2
        assert links["orders-db"] == ResourceLink(type="datastore", config_key="ordersDb")
        assert links["app-cache"] == ResourceLink(type="cache", config_key="appCache")

    def test_empty_file_returns_empty(
        self, resource_links_file: Callable[[dict[str, Any]], Path]
    ) -> None:
        resource_links_file({})
        assert capture_resource_links() == {}

    def test_missing_file_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("CELERITY_RESOURCE_LINKS_PATH", str(tmp_path / "does-not-exist.json"))
        with pytest.raises(FileNotFoundError, match="resource links file not found"):
            capture_resource_links()

    def test_invalid_json_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        path = tmp_path / "__celerity_resource_links__.json"
        path.write_text("not-json", encoding="utf-8")
        monkeypatch.setenv("CELERITY_RESOURCE_LINKS_PATH", str(path))
        with pytest.raises(ValueError, match="not valid JSON"):
            capture_resource_links()

    def test_skips_malformed_entries(
        self, resource_links_file: Callable[[dict[str, Any]], Path]
    ) -> None:
        resource_links_file(
            {
                "good": {"type": "cache", "configKey": "cacheKey"},
                "bad-missing-type": {"configKey": "x"},
                "bad-missing-key": {"type": "cache"},
                "bad-not-dict": "string",
            }
        )
        links = capture_resource_links()
        assert len(links) == 1
        assert "good" in links


class TestGetLinksOfType:
    def test_filters_by_type(self) -> None:
        links = {
            "db": ResourceLink(type="datastore", config_key="dbKey"),
            "cache": ResourceLink(type="cache", config_key="cacheKey"),
            "other-db": ResourceLink(type="datastore", config_key="otherDbKey"),
        }
        result = get_links_of_type(links, "datastore")
        assert result == {"db": "dbKey", "other-db": "otherDbKey"}

    def test_returns_empty_for_no_match(self) -> None:
        links = {"cache": ResourceLink(type="cache", config_key="key")}
        assert get_links_of_type(links, "datastore") == {}


class TestGetResourceTypes:
    def test_returns_distinct_types(self) -> None:
        links = {
            "a": ResourceLink(type="cache", config_key="k1"),
            "b": ResourceLink(type="datastore", config_key="k2"),
            "c": ResourceLink(type="cache", config_key="k3"),
        }
        assert get_resource_types(links) == {"cache", "datastore"}

    def test_empty_links(self) -> None:
        assert get_resource_types({}) == set()


# ---------------------------------------------------------------------------
# Platform and deploy target
# ---------------------------------------------------------------------------


class TestDetectPlatform:
    def test_reads_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_PLATFORM", "aws")
        assert detect_platform() == "aws"

    def test_defaults_to_other(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_PLATFORM", raising=False)
        assert detect_platform() == "other"


class TestDetectRuntimeMode:
    def test_runtime_when_env_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CELERITY_RUNTIME", "true")
        assert detect_runtime_mode() == "runtime"

    def test_functions_when_env_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CELERITY_RUNTIME", raising=False)
        assert detect_runtime_mode() == "functions"


# ---------------------------------------------------------------------------
# Tokens
# ---------------------------------------------------------------------------


class TestResourceTokens:
    def test_resource_token(self) -> None:
        assert resource_token("cache", "app-cache") == "celerity:cache:app-cache"

    def test_default_token(self) -> None:
        assert default_token("cache") == "celerity:cache:default"

    def test_is_resource_layer_token_matches(self) -> None:
        assert is_resource_layer_token("celerity:cache:app") is True
        assert is_resource_layer_token("celerity:datastore:orders") is True
        assert is_resource_layer_token("celerity:config:resources") is True
        assert is_resource_layer_token("celerity:sqlDatabase:main") is True

    def test_is_resource_layer_token_rejects(self) -> None:
        assert is_resource_layer_token("OrderService") is False
        assert is_resource_layer_token("celerity:unknown:x") is False
        assert is_resource_layer_token(42) is False
        assert is_resource_layer_token(None) is False
