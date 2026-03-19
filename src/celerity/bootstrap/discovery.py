"""Root module discovery for runtime and serverless modes."""

from __future__ import annotations

import importlib
import logging
import os
import sys
from pathlib import Path

from celerity.metadata.keys import MODULE, get_metadata

logger = logging.getLogger("celerity.bootstrap")


def discover_module(module_path: str | None = None) -> type:
    """Discover the root ``@module`` class.

    Resolution order:

    1. Explicit ``module_path`` argument
    2. ``CELERITY_MODULE_PATH`` environment variable
    3. Raises ``RuntimeError``

    The path is converted to a Python module name, imported, and
    scanned for a class decorated with ``@module``.

    Args:
        module_path: Optional explicit path to the module file.

    Returns:
        The root ``@module``-decorated class.

    Raises:
        RuntimeError: If no module path is found or no ``@module``
            class exists in the module.
    """
    resolved = module_path or os.environ.get("CELERITY_MODULE_PATH")
    if not resolved:
        msg = "No module path provided. Set CELERITY_MODULE_PATH or pass module_path explicitly."
        raise RuntimeError(msg)

    logger.debug("discover_module: loading %s", resolved)

    path = Path(resolved)

    # Ensure the parent directory is importable.
    parent = str(path.parent.resolve())
    if parent not in sys.path:
        sys.path.insert(0, parent)

    module_name = _path_to_module_name(path)
    imported = importlib.import_module(module_name)

    for name in dir(imported):
        obj = getattr(imported, name)
        if isinstance(obj, type) and get_metadata(obj, MODULE) is not None:
            logger.debug("discover_module: found %s", obj.__name__)
            return obj

    msg = f"No @module class found in {resolved}"
    raise RuntimeError(msg)


def _path_to_module_name(path: Path) -> str:
    """Convert a file path to a Python module name."""
    return path.with_suffix("").name
