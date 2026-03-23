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

    # Determine the importable module name and the directory to add to
    # sys.path. Non-package directories (those without __init__.py) at
    # the start of the path are treated as filesystem prefixes, not part
    # of the Python module hierarchy.
    #
    # Example: "app/src/app_module.py" where app/ has no __init__.py
    #   → sys.path gets "app/", module name is "src.app_module"
    import_root, module_name = _resolve_import(path)
    root = str(import_root.resolve())
    if root not in sys.path:
        sys.path.insert(0, root)

    imported = importlib.import_module(module_name)

    for name in dir(imported):
        obj = getattr(imported, name)
        if isinstance(obj, type) and get_metadata(obj, MODULE) is not None:
            logger.debug("discover_module: found %s", obj.__name__)
            return obj

    msg = f"No @module class found in {resolved}"
    raise RuntimeError(msg)


def _resolve_import(path: Path) -> tuple[Path, str]:
    """Determine the sys.path root and dotted module name for a file path.

    Walks the path components from the start, skipping non-package
    directories (those without ``__init__.py``) that act as filesystem
    prefixes (e.g. bind-mount points like ``app/``).

    The first directory that contains ``__init__.py`` is treated as the
    top-level Python package. Everything from that point onward forms
    the dotted module name, and the directory *containing* that package
    is the import root to add to ``sys.path``.

    Examples::

        # "app/src/app_module.py" where app/ has no __init__.py, src/ does
        # → import root = "app/", module name = "src.app_module"

        # "src/app_module.py" where src/ has __init__.py
        # → import root = ".", module name = "src.app_module"

        # "app_module.py" (no package)
        # → import root = ".", module name = "app_module"

    Returns:
        A tuple of (import_root, module_name).
    """
    parts = path.with_suffix("").parts

    # Find the first directory that is a Python package.
    for i in range(len(parts) - 1):
        candidate = Path(*parts[: i + 1])
        if (candidate / "__init__.py").exists():
            # Everything before this directory is the import root.
            import_root = candidate.parent
            module_name = ".".join(parts[i:])
            return import_root, module_name

    # No package directory found — use the parent directory as root
    # and just the filename as the module name (original behaviour).
    return path.parent, parts[-1]
