"""Handler registry with path-pattern matching."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from celerity.types.handler import (
        ResolvedGuard,
        ResolvedHandler,
    )


logger = logging.getLogger("celerity.registry")


def _get_routing_key_for_log(handler: Any) -> str:
    """Extract a routing key string for debug logging."""
    handler_type = getattr(handler, "type", None)
    if handler_type == "http":
        return f"{getattr(handler, 'method', '?')} {getattr(handler, 'path', '?')}"
    if handler_type == "websocket":
        return getattr(handler, "route", "?")
    if handler_type in ("consumer", "schedule"):
        return getattr(handler, "handler_tag", "?")
    if handler_type == "custom":
        return getattr(handler, "name", "?")
    return "?"


def _compile_path_pattern(path: str) -> re.Pattern[str]:
    """Convert a ``{param}`` path pattern to a regex with named groups.

    Supports two forms:
    - ``{param}`` — matches a single path segment (``[^/]+``)
    - ``{param+}`` — wildcard/greedy, matches one or more segments (``.+``)

    Args:
        path: A route pattern like ``/orders/{id}/items/{item_id}``
            or ``/api/{proxy+}``.

    Returns:
        A compiled regex that matches concrete paths and extracts
        named groups for each parameter.
    """
    escaped = re.escape(path)
    # Wildcard params: {name+} → match one or more segments (greedy).
    pattern = re.sub(r"\\{(\w+)\\\+\\}", r"(?P<\1>.+)", escaped)
    # Standard params: {name} → match a single segment.
    pattern = re.sub(r"\\{(\w+)\\}", r"(?P<\1>[^/]+)", pattern)
    return re.compile(f"^{pattern}$")


class HandlerRegistry:
    """Stores resolved handlers indexed by type and routing key.

    HTTP handlers use path-pattern matching (``{id}`` matches concrete
    values). All other handler types use exact-match lookup.

    Example::

        registry = HandlerRegistry()
        registry.register(handler)
        found = registry.get_handler("http", "GET /orders/123")
    """

    def __init__(self) -> None:
        self._handlers: dict[str, list[ResolvedHandler]] = {}
        self._http_patterns: list[tuple[str, re.Pattern[str], ResolvedHandler]] = []
        self._guards: dict[str, ResolvedGuard] = {}

    def register(self, handler: ResolvedHandler) -> None:
        """Register a resolved handler.

        Args:
            handler: The resolved handler to register.
        """
        logger.debug("register %s %s", handler.type, _get_routing_key_for_log(handler))
        handler_type = handler.type
        if handler_type not in self._handlers:
            self._handlers[handler_type] = []
        self._handlers[handler_type].append(handler)

        if handler_type == "http":
            method = getattr(handler, "method", None)
            path = getattr(handler, "path", None)
            if method and path:
                pattern = _compile_path_pattern(path)
                key = f"{method} {path}"
                self._http_patterns.append((key, pattern, handler))

    def get_handler(self, handler_type: str, routing_key: str) -> ResolvedHandler | None:
        """Look up a handler by type and routing key.

        For HTTP handlers, the routing_key is ``"METHOD /path"`` and
        path-pattern matching is used. For all other types, exact match
        is used against the handler's tag, route, or name.

        Args:
            handler_type: The handler type (``"http"``, ``"websocket"``,
                ``"consumer"``, ``"schedule"``, ``"custom"``).
            routing_key: The routing key to match.

        Returns:
            The matching handler, or ``None`` if not found.
        """
        if handler_type == "http":
            result = self._match_http(routing_key)
        else:
            result = self._match_exact(handler_type, routing_key)
        if result is not None:
            logger.debug("get_handler %s %s → matched", handler_type, routing_key)
        else:
            logger.debug("get_handler %s %s → not found", handler_type, routing_key)
        return result

    def get_handler_by_id(self, handler_type: str, handler_id: str) -> ResolvedHandler | None:
        """Look up a handler by its unique ID.

        Args:
            handler_type: The handler type.
            handler_id: The handler's unique ID.

        Returns:
            The matching handler, or ``None``.
        """
        for handler in self._handlers.get(handler_type, []):
            if handler.id == handler_id:
                return handler
        return None

    def get_handlers_by_type(self, handler_type: str) -> list[ResolvedHandler]:
        """Get all handlers of a specific type.

        Args:
            handler_type: The handler type.
        """
        return list(self._handlers.get(handler_type, []))

    def get_all_handlers(self) -> list[ResolvedHandler]:
        """Get all registered handlers across all types."""
        result: list[ResolvedHandler] = []
        for handlers in self._handlers.values():
            result.extend(handlers)
        return result

    def register_guard(self, guard: ResolvedGuard) -> None:
        """Register a resolved guard.

        Args:
            guard: The resolved guard to register.
        """
        logger.debug("register_guard %s", guard.name)
        self._guards[guard.name] = guard

    def get_guard(self, name: str) -> ResolvedGuard | None:
        """Look up a guard by name.

        Args:
            name: The guard name.
        """
        result = self._guards.get(name)
        if result is not None:
            logger.debug("get_guard %s → matched", name)
        else:
            logger.debug("get_guard %s → not found", name)
        return result

    def get_all_guards(self) -> list[ResolvedGuard]:
        """Get all registered guards."""
        return list(self._guards.values())

    def _match_http(self, routing_key: str) -> ResolvedHandler | None:
        """Match an HTTP routing key like ``"GET /orders/123"``."""
        parts = routing_key.split(" ", 1)
        if len(parts) != 2:
            return None
        method, request_path = parts

        for _key, pattern, handler in self._http_patterns:
            handler_method = getattr(handler, "method", None)
            if handler_method != method:
                continue
            match = pattern.match(request_path)
            if match:
                return handler
        return None

    def _match_exact(self, handler_type: str, routing_key: str) -> ResolvedHandler | None:
        """Match non-HTTP handlers by exact routing key."""
        for handler in self._handlers.get(handler_type, []):
            key = self._get_routing_key(handler)
            if key == routing_key:
                return handler
        return None

    @staticmethod
    def _get_routing_key(handler: Any) -> str | None:
        """Extract the routing key from a handler based on its type."""
        handler_type = getattr(handler, "type", None)
        if handler_type == "websocket":
            return getattr(handler, "route", None)
        if handler_type in ("consumer", "schedule"):
            return getattr(handler, "handler_tag", None)
        if handler_type == "custom":
            return getattr(handler, "name", None)
        return None

    def extract_path_params(self, routing_key: str) -> dict[str, str]:
        """Extract path parameters from an HTTP routing key.

        Args:
            routing_key: An HTTP routing key like ``"GET /orders/123"``.

        Returns:
            A dict of extracted path parameters, or empty dict.
        """
        parts = routing_key.split(" ", 1)
        if len(parts) != 2:
            return {}
        method, request_path = parts

        for _key, pattern, handler in self._http_patterns:
            handler_method = getattr(handler, "method", None)
            if handler_method != method:
                continue
            match = pattern.match(request_path)
            if match:
                return match.groupdict()
        return {}
