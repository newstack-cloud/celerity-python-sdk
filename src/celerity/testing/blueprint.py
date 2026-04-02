"""Parse blueprint files to map resource IDs to physical names."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import json5
import yaml


@dataclass(frozen=True)
class BlueprintResource:
    """Resource info extracted from the blueprint."""

    resource_id: str
    type: str  # "celerity/datastore", "celerity/topic", etc.
    physical_name: str  # spec.name or fallback to resource_id


def load_blueprint_resources(
    blueprint_path: str | None = None,
) -> dict[str, BlueprintResource]:
    """Parse the app blueprint and extract resource definitions.

    Maps resource IDs (e.g., ``"usersDatastore"``) to physical names
    (e.g., ``"users"`` from ``spec.name``).
    """
    path = blueprint_path or _find_blueprint_path()
    if path is None:
        return {}

    with open(path) as f:
        bp = json5.load(f) if path.endswith((".json", ".jsonc")) else yaml.safe_load(f)

    resources: dict[str, BlueprintResource] = {}
    bp_resources = bp.get("resources") if isinstance(bp, dict) else None
    if not isinstance(bp_resources, dict):
        return resources

    for resource_id, resource in bp_resources.items():
        if not isinstance(resource, dict) or "type" not in resource:
            continue
        spec = resource.get("spec") or {}
        physical_name = spec.get("name", resource_id)
        resources[resource_id] = BlueprintResource(
            resource_id=resource_id,
            type=resource["type"],
            physical_name=physical_name,
        )

    return resources


def _find_blueprint_path() -> str | None:
    cwd = Path(os.getcwd())
    for name in (
        "app.blueprint.yaml",
        "app.blueprint.yml",
        "app.blueprint.jsonc",
        "app.blueprint.json",
    ):
        candidate = cwd / name
        if candidate.exists():
            return str(candidate.resolve())
    return None
