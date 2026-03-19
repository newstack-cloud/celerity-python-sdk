"""Identity derivation for handler manifest entries.

Produces resource names, handler names, handler function references,
and code locations.
"""

from __future__ import annotations

from pathlib import PurePosixPath


def derive_class_resource_name(class_name: str, method_name: str) -> str:
    """Derive a resource name for a class-based handler method.

    Format: ``camelCase(className) + "_" + methodName``

    Example::

        >>> derive_class_resource_name("OrdersController", "get_order")
        'ordersController_get_order'
    """
    camel = class_name[0].lower() + class_name[1:]
    return f"{camel}_{method_name}"


def derive_class_handler_name(class_name: str, method_name: str) -> str:
    """Derive a handler name for a class-based handler method.

    Format: ``className + "-" + methodName``

    Example::

        >>> derive_class_handler_name("OrdersController", "get_order")
        'OrdersController-get_order'
    """
    return f"{class_name}-{method_name}"


def derive_class_handler_function(
    source_file: str,
    class_name: str,
    method_name: str,
) -> str:
    """Derive the handler function reference for a class-based handler.

    Format: ``moduleBaseName + "." + className + "." + methodName``

    Example::

        >>> derive_class_handler_function("src/handlers/orders.py", "OrdersController", "get_order")
        'orders.OrdersController.get_order'
    """
    base = PurePosixPath(source_file).stem
    return f"{base}.{class_name}.{method_name}"


def derive_function_resource_name(export_name: str) -> str:
    """Derive a resource name for a function-based handler.

    Uses the export name directly.
    """
    return export_name


def derive_function_handler_function(source_file: str, export_name: str) -> str:
    """Derive the handler function reference for a function-based handler.

    Format: ``moduleBaseName + "." + exportName``
    """
    base = PurePosixPath(source_file).stem
    return f"{base}.{export_name}"


def derive_code_location(source_file: str, project_root: str) -> str:
    """Derive the code location from a source file path relative to the project root.

    Returns the directory prefixed with ``"./"``.

    Example::

        >>> derive_code_location("src/handlers/orders.py", "/project")
        './src/handlers'
    """
    try:
        rel = PurePosixPath(source_file).relative_to(project_root)
    except ValueError:
        rel = PurePosixPath(source_file)
    parent = str(rel.parent)
    return "./" if parent == "." else f"./{parent}"
