"""
Metadata key constants and get/set primitives.
"""

from typing import Any

# Key constants
CONTROLLER = "celerity:controller"
HTTP_METHOD = "celerity:http-method"
ROUTE_PATH = "celerity:route-path"
PARAM = "celerity:param"
GUARD_PROTECTEDBY = "celerity:guard:protectedBy"
GUARD_CUSTOM = "celerity:guard:custom"
LAYER = "celerity:layer"
MODULE = "celerity:module"
INJECTABLE = "celerity:injectable"
PUBLIC = "celerity:public"
CUSTOM_METADATA = "celerity:custom-metadata"
WEBSOCKET_CONTROLLER = "celerity:websocket-controller"
WEBSOCKET_EVENT = "celerity:websocket-event"
CONSUMER = "celerity:consumer"
CONSUMER_HANDLER = "celerity:consumer-handler"
SCHEDULE_HANDLER = "celerity:schedule-handler"
INVOKE = "celerity:invoke"
INJECT = "celerity:inject"
USE_RESOURCE = "celerity:use-resource"

_META_ATTR = "__celerity_metadata__"
"""Internal attribute name for the metadata dict."""


def get_metadata(target: object, key: str, prop: str | None = None) -> Any:
    """Read a metadata value from a target (class or function).

    Args:
        target: The class or function to read metadata from.
        key: The metadata key to look up.
        prop: If provided, reads method-level metadata stored under
            ``"method:{prop}"`` in the target's metadata dict.

    Returns:
        The stored value, or ``None`` if not set.
    """
    meta = getattr(target, _META_ATTR, None)
    if meta is None:
        return None
    if prop is not None:
        method_meta: dict[str, Any] = meta.get(f"method:{prop}", {})
        return method_meta.get(key)
    return meta.get(key)


def set_metadata(
    target: object,
    key: str,
    value: Any,
    prop: str | None = None,
) -> None:
    """Store a metadata value on a target (class or function).

    Args:
        target: The class or function to store metadata on.
        key: The metadata key.
        value: The value to store.
        prop: If provided, stores method-level metadata under
            ``"method:{prop}"`` in the target's metadata dict.
    """
    if not hasattr(target, _META_ATTR):
        setattr(target, _META_ATTR, {})
    meta: dict[str, Any] = getattr(target, _META_ATTR)
    if prop is not None:
        method_key = f"method:{prop}"
        if method_key not in meta:
            meta[method_key] = {}
        meta[method_key][key] = value
    else:
        meta[key] = value
