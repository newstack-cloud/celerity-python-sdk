"""Handler Manifest types matching the Go CLI's manifest.go schema."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class HandlerSpec:
    """Blueprint spec fields for a handler resource."""

    handler_name: str
    code_location: str
    handler: str
    timeout: int | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "handlerName": self.handler_name,
            "codeLocation": self.code_location,
            "handler": self.handler,
        }
        if self.timeout is not None:
            d["timeout"] = self.timeout
        return d


@dataclass
class ClassHandlerEntry:
    """A handler discovered from a class-based decorator."""

    resource_name: str
    class_name: str
    method_name: str
    source_file: str
    handler_type: str
    annotations: dict[str, str | list[str] | bool]
    spec: HandlerSpec

    def to_dict(self) -> dict[str, Any]:
        return {
            "resourceName": self.resource_name,
            "className": self.class_name,
            "methodName": self.method_name,
            "sourceFile": self.source_file,
            "handlerType": self.handler_type,
            "annotations": self.annotations,
            "spec": self.spec.to_dict(),
        }


@dataclass
class FunctionHandlerEntry:
    """A handler discovered from a function-based export."""

    resource_name: str
    export_name: str
    source_file: str
    handler_type: str
    annotations: dict[str, str | list[str] | bool] | None = None
    spec: HandlerSpec = field(default_factory=lambda: HandlerSpec("", "", ""))

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "resourceName": self.resource_name,
            "exportName": self.export_name,
            "sourceFile": self.source_file,
            "handlerType": self.handler_type,
            "spec": self.spec.to_dict(),
        }
        if self.annotations:
            d["annotations"] = self.annotations
        return d


@dataclass
class GuardHandlerEntry:
    """A custom auth guard discovered from a @guard decorator or function guard."""

    resource_name: str
    guard_name: str
    source_file: str
    guard_type: str
    annotations: dict[str, str | list[str] | bool]
    spec: HandlerSpec
    class_name: str | None = None
    export_name: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "resourceName": self.resource_name,
            "guardName": self.guard_name,
            "sourceFile": self.source_file,
            "guardType": self.guard_type,
            "annotations": self.annotations,
            "spec": self.spec.to_dict(),
        }
        if self.class_name:
            d["className"] = self.class_name
        if self.export_name:
            d["exportName"] = self.export_name
        return d


@dataclass
class DependencyNode:
    """A single node in the dependency graph."""

    token: str
    token_type: str
    provider_type: str
    dependencies: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "token": self.token,
            "tokenType": self.token_type,
            "providerType": self.provider_type,
            "dependencies": self.dependencies,
        }


@dataclass
class DependencyGraph:
    """Full provider dependency graph."""

    nodes: list[DependencyNode] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"nodes": [n.to_dict() for n in self.nodes]}


@dataclass
class HandlerManifest:
    """Output of the extraction CLI matching handler-manifest.v1.schema.json."""

    version: str = "1.0.0"
    handlers: list[ClassHandlerEntry] = field(default_factory=list)
    function_handlers: list[FunctionHandlerEntry] = field(default_factory=list)
    guard_handlers: list[GuardHandlerEntry] = field(default_factory=list)
    dependency_graph: DependencyGraph = field(default_factory=DependencyGraph)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "handlers": [h.to_dict() for h in self.handlers],
            "functionHandlers": [h.to_dict() for h in self.function_handlers],
            "guardHandlers": [g.to_dict() for g in self.guard_handlers],
            "dependencyGraph": self.dependency_graph.to_dict(),
        }
