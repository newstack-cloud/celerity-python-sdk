"""Discover resource dependency tokens from module metadata."""

from __future__ import annotations

from dataclasses import dataclass

from celerity.bootstrap.module_graph import build_module_graph
from celerity.di.dependency_tokens import get_class_dependency_tokens
from celerity.resources._tokens import is_resource_layer_token


@dataclass(frozen=True)
class ResourceTokenInfo:
    """Parsed resource token info from a string like 'celerity:datastore:usersDatastore'."""

    token: str
    type: str  # "datastore", "topic", "queue", "cache", "bucket", "sqlDatabase", "config"
    name: str  # "usersDatastore", "userEventsTopic", etc.


def discover_resource_tokens(root_module: type) -> list[ResourceTokenInfo]:
    """Walk the module graph and extract all resource layer tokens.

    Inspects constructor type hints of all providers, controllers, and guards
    in the module (and its imports) to find resource parameter annotations
    like ``DatastoreParam("usersDatastore")``.
    """
    graph = build_module_graph(root_module)
    seen: set[str] = set()
    result: list[ResourceTokenInfo] = []

    for node in graph.values():
        classes: list[type] = list(node.controllers)

        for guard in node.guards:
            if isinstance(guard, type):
                classes.append(guard)

        for provider in node.providers:
            if isinstance(provider, type):
                classes.append(provider)
            elif hasattr(provider, "use_class"):
                classes.append(provider.use_class)

        for cls in classes:
            dep_tokens = get_class_dependency_tokens(cls)
            for dep in dep_tokens:
                if not isinstance(dep, str):
                    continue
                if not is_resource_layer_token(dep):
                    continue
                if dep in seen:
                    continue
                seen.add(dep)

                parsed = _parse_resource_token(dep)
                if parsed is not None:
                    result.append(parsed)

    return result


def _parse_resource_token(token: str) -> ResourceTokenInfo | None:
    """Parse a resource token into its components.

    Handles both 3-part tokens (``celerity:<type>:<name>``) and 4-part
    SQL tokens (``celerity:sqlDatabase:<role>:<name>``).
    """
    parts = token.split(":")
    if len(parts) < 3 or parts[0] != "celerity":
        return None

    # 4-part SQL tokens: celerity:sqlDatabase:writer:auditDatabase
    if parts[1] == "sqlDatabase" and len(parts) == 4:
        return ResourceTokenInfo(
            token=token,
            type=f"sqlDatabase:{parts[2]}",  # e.g. "sqlDatabase:writer"
            name=parts[3],
        )

    # 3-part tokens: celerity:datastore:usersDatastore
    return ResourceTokenInfo(token=token, type=parts[1], name=parts[2])
