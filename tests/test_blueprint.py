"""Tests for blueprint file loading and resource extraction."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from celerity.testing.blueprint import BlueprintResource, load_blueprint_resources


@pytest.fixture
def tmp_blueprint(tmp_path: Path) -> object:
    """Return a helper that writes a blueprint file and returns its path."""

    def _write(filename: str, content: str) -> str:
        p = tmp_path / filename
        p.write_text(content)
        return str(p)

    return _write


# ---------------------------------------------------------------------------
# YAML blueprints
# ---------------------------------------------------------------------------


class TestYamlBlueprint:
    def test_loads_resources_from_yaml(self, tmp_blueprint: object) -> None:
        path = tmp_blueprint(  # type: ignore[operator]
            "app.blueprint.yaml",
            """\
resources:
  usersDatastore:
    type: "celerity/datastore"
    spec:
      name: "users"
  ordersDatastore:
    type: "celerity/datastore"
    spec:
      name: "orders"
""",
        )
        result = load_blueprint_resources(path)
        assert result == {
            "usersDatastore": BlueprintResource(
                resource_id="usersDatastore",
                type="celerity/datastore",
                physical_name="users",
            ),
            "ordersDatastore": BlueprintResource(
                resource_id="ordersDatastore",
                type="celerity/datastore",
                physical_name="orders",
            ),
        }

    def test_falls_back_to_resource_id_when_no_spec_name(self, tmp_blueprint: object) -> None:
        path = tmp_blueprint(  # type: ignore[operator]
            "app.blueprint.yaml",
            """\
resources:
  myTopic:
    type: "celerity/topic"
    spec:
      description: "A topic without a name"
""",
        )
        result = load_blueprint_resources(path)
        assert result["myTopic"].physical_name == "myTopic"

    def test_skips_resources_without_type(self, tmp_blueprint: object) -> None:
        path = tmp_blueprint(  # type: ignore[operator]
            "app.blueprint.yaml",
            """\
resources:
  valid:
    type: "celerity/datastore"
    spec:
      name: "valid"
  invalid:
    spec:
      name: "missing-type"
""",
        )
        result = load_blueprint_resources(path)
        assert "valid" in result
        assert "invalid" not in result

    def test_returns_empty_for_no_resources_key(self, tmp_blueprint: object) -> None:
        path = tmp_blueprint(  # type: ignore[operator]
            "app.blueprint.yaml",
            "version: 1\n",
        )
        assert load_blueprint_resources(path) == {}

    def test_returns_empty_for_empty_file(self, tmp_blueprint: object) -> None:
        path = tmp_blueprint("app.blueprint.yaml", "")  # type: ignore[operator]
        assert load_blueprint_resources(path) == {}


# ---------------------------------------------------------------------------
# JSON blueprints
# ---------------------------------------------------------------------------


class TestJsonBlueprint:
    def test_loads_plain_json(self, tmp_blueprint: object) -> None:
        path = tmp_blueprint(  # type: ignore[operator]
            "app.blueprint.json",
            """\
{
  "resources": {
    "usersDatastore": {
      "type": "celerity/datastore",
      "spec": { "name": "users" }
    }
  }
}
""",
        )
        result = load_blueprint_resources(path)
        assert result["usersDatastore"].physical_name == "users"


# ---------------------------------------------------------------------------
# JSONC blueprints
# ---------------------------------------------------------------------------


class TestJsoncBlueprint:
    def test_loads_jsonc_with_line_comments(self, tmp_blueprint: object) -> None:
        path = tmp_blueprint(  # type: ignore[operator]
            "app.blueprint.jsonc",
            """\
{
  // Application resources
  "resources": {
    "usersDatastore": {
      "type": "celerity/datastore", // main datastore
      "spec": { "name": "users" }
    }
  }
}
""",
        )
        result = load_blueprint_resources(path)
        assert result["usersDatastore"] == BlueprintResource(
            resource_id="usersDatastore",
            type="celerity/datastore",
            physical_name="users",
        )

    def test_loads_jsonc_with_block_comments(self, tmp_blueprint: object) -> None:
        path = tmp_blueprint(  # type: ignore[operator]
            "app.blueprint.jsonc",
            """\
{
  /* Resource definitions for the application */
  "resources": {
    "eventsTopic": {
      "type": "celerity/topic",
      "spec": {
        "name": "events"
      }
    }
  }
}
""",
        )
        result = load_blueprint_resources(path)
        assert result["eventsTopic"].type == "celerity/topic"
        assert result["eventsTopic"].physical_name == "events"

    def test_loads_jsonc_with_trailing_commas(self, tmp_blueprint: object) -> None:
        path = tmp_blueprint(  # type: ignore[operator]
            "app.blueprint.jsonc",
            """\
{
  "resources": {
    "cache": {
      "type": "celerity/cache",
      "spec": {
        "name": "session-cache",
      },
    },
  },
}
""",
        )
        result = load_blueprint_resources(path)
        assert result["cache"].physical_name == "session-cache"

    def test_comment_like_content_in_strings_preserved(self, tmp_blueprint: object) -> None:
        path = tmp_blueprint(  # type: ignore[operator]
            "app.blueprint.jsonc",
            """\
{
  "resources": {
    "api": {
      "type": "celerity/api",
      "spec": {
        "name": "https://example.com/api"
      }
    }
  }
}
""",
        )
        result = load_blueprint_resources(path)
        assert result["api"].physical_name == "https://example.com/api"


# ---------------------------------------------------------------------------
# Auto-discovery
# ---------------------------------------------------------------------------


class TestAutoDiscovery:
    def test_returns_empty_when_no_blueprint_found(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        assert load_blueprint_resources() == {}

    def test_discovers_yaml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        bp = tmp_path / "app.blueprint.yaml"
        bp.write_text(
            'resources:\n  ds:\n    type: "celerity/datastore"\n    spec:\n      name: "x"\n'
        )
        monkeypatch.chdir(tmp_path)
        result = load_blueprint_resources()
        assert "ds" in result

    def test_discovers_jsonc_over_json(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        jsonc = tmp_path / "app.blueprint.jsonc"
        jsonc.write_text(
            '{\n  "resources": {\n    "fromJsonc": {\n'
            '      "type": "celerity/topic",\n'
            '      "spec": { "name": "jsonc-topic" }\n    }\n  }\n}\n'
        )
        json_file = tmp_path / "app.blueprint.json"
        json_file.write_text(
            '{\n  "resources": {\n    "fromJson": {\n'
            '      "type": "celerity/topic",\n'
            '      "spec": { "name": "json-topic" }\n    }\n  }\n}\n'
        )
        monkeypatch.chdir(tmp_path)
        result = load_blueprint_resources()
        assert "fromJsonc" in result
        assert "fromJson" not in result
