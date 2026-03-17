"""Path utilities for handler routing."""


def join_handler_path(prefix: str, path: str) -> str:
    """Join a controller prefix and a method path, normalizing slashes.

    Args:
        prefix: The controller route prefix (e.g. ``"/orders"``).
        path: The method route path (e.g. ``"/{id}"``).

    Returns:
        The joined path with normalized slashes.

    Examples:
        >>> join_handler_path("/orders", "/{id}")
        '/orders/{id}'
        >>> join_handler_path("/orders", "/")
        '/orders'
        >>> join_handler_path("", "/{id}")
        '/{id}'
        >>> join_handler_path("/", "/health")
        '/health'
        >>> join_handler_path("/api", "items")
        '/api/items'
    """
    prefix = prefix.rstrip("/") if prefix else ""
    path = path if path.startswith("/") else f"/{path}"
    if prefix and path == "/":
        return prefix
    return f"{prefix}{path}"
