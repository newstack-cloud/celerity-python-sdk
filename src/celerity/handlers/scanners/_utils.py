"""Shared scanner utilities."""

from __future__ import annotations

from typing import Any

from celerity.metadata.keys import (
    _META_ATTR,
    CUSTOM_METADATA,
    GUARD_PROTECTEDBY,
    LAYER,
    get_metadata,
)


def get_method_names(cls: type) -> list[str]:
    """Get user-defined method names on a class, excluding dunder methods."""
    return [
        name for name in dir(cls) if not name.startswith("_") and callable(getattr(cls, name, None))
    ]


def get_method_metadata(cls: type, method_name: str) -> dict[str, Any]:
    """Get the ``__celerity_metadata__`` dict from a method on a class."""
    method_fn = getattr(cls, method_name, None)
    if method_fn is None:
        return {}
    return getattr(method_fn, _META_ATTR, {})


def collect_protected_by(cls: type, method_meta: dict[str, Any]) -> list[str]:
    """Merge class-level and method-level @protected_by guards."""
    class_guards: list[str] = get_metadata(cls, GUARD_PROTECTEDBY) or []
    method_guards: list[str] = method_meta.get(GUARD_PROTECTEDBY, [])
    return [*class_guards, *method_guards]


def collect_layers(cls: type, method_meta: dict[str, Any]) -> list[Any]:
    """Merge class-level and method-level @use_layer layers."""
    class_layers: list[Any] = get_metadata(cls, LAYER) or []
    method_layers: list[Any] = method_meta.get(LAYER, [])
    return [*class_layers, *method_layers]


def collect_custom_metadata(cls: type, method_meta: dict[str, Any]) -> dict[str, Any]:
    """Merge class-level and method-level @set_handler_metadata."""
    class_meta: dict[str, Any] = get_metadata(cls, CUSTOM_METADATA) or {}
    method_custom: dict[str, Any] = method_meta.get(CUSTOM_METADATA, {})
    return {**class_meta, **method_custom}
