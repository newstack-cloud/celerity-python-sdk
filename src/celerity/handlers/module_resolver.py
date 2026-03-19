"""Module-based handler resolution."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

from celerity.metadata.keys import MODULE, get_metadata


def resolve_handler_module(module_path: str, project_root: str | None = None) -> type | None:
    """Resolve a root module class from a file path.

    Imports the Python module at the given path and finds the first
    class decorated with ``@module``.

    Args:
        module_path: Path to the Python module file
            (e.g. ``"src/app_module.py"``).
        project_root: Optional project root to add to ``sys.path``
            for import resolution.

    Returns:
        The ``@module``-decorated class, or ``None`` if not found.
    """
    if project_root:
        root = str(Path(project_root).resolve())
        if root not in sys.path:
            sys.path.insert(0, root)

    module_name = _path_to_module_name(module_path)
    imported = importlib.import_module(module_name)

    for name in dir(imported):
        obj = getattr(imported, name)
        if isinstance(obj, type) and get_metadata(obj, MODULE) is not None:
            return obj

    return None


def _path_to_module_name(path: str) -> str:
    """Convert a file path to a Python module name.

    Args:
        path: A file path like ``"src/app_module.py"``.

    Returns:
        A dotted module name like ``"src.app_module"``.
    """
    return str(Path(path).with_suffix("")).replace("/", ".").replace("\\", ".")
