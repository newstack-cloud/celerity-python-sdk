"""CLI entry point: celerity-extract

Usage: celerity-extract --module src/app_module.py --project-root /path/to/project

Outputs Handler Manifest JSON to stdout.
Errors are written to stderr as JSON.
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import sys
from pathlib import Path

from celerity.cli.metadata_app import build_scanned_module
from celerity.cli.serializer import serialize_manifest
from celerity.metadata.keys import MODULE, get_metadata

logger = logging.getLogger("celerity.cli")


def main() -> None:
    """Extract handler manifest from a Celerity application module."""
    parser = argparse.ArgumentParser(description="Extract handler manifest")
    parser.add_argument("--module", required=True, help="Path to the app module file")
    parser.add_argument("--project-root", required=True, help="Project root directory")
    args = parser.parse_args()

    try:
        project_root = Path(args.project_root).resolve()
        sys.path.insert(0, str(project_root))

        module_path = Path(args.module)
        module_name = _path_to_module_name(module_path)
        logger.debug("extract: importing %s", module_name)

        imported = importlib.import_module(module_name)
        root_module = _find_module_class(imported)

        if root_module is None:
            _error_exit(f"No @module class found in {args.module}")
            return

        logger.debug("extract: root module found: %s", root_module.__name__)

        scanned = build_scanned_module(root_module)
        logger.debug(
            "extract: scanned %d class handlers, %d function handlers, %d guards",
            len(scanned.class_handlers),
            len(scanned.function_handlers),
            len(scanned.guard_handlers),
        )

        if not scanned.class_handlers and not scanned.function_handlers:
            print(
                f'Warning: No handlers found in module "{args.module}"',
                file=sys.stderr,
            )

        manifest = serialize_manifest(
            scanned,
            str(module_path),
            project_root=str(project_root),
        )
        json.dump(manifest.to_dict(), sys.stdout, indent=2)
        sys.stdout.write("\n")

    except Exception as exc:
        _error_exit(str(exc))


def _path_to_module_name(path: Path) -> str:
    """Convert a file path to a Python module name.

    Strips the ``.py`` suffix and joins parts with dots.
    e.g., ``src/app_module.py`` -> ``src.app_module``
    """
    parts = list(path.with_suffix("").parts)
    return ".".join(parts)


def _find_module_class(module: object) -> type | None:
    """Find the class with @module metadata in the imported module."""
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, type) and get_metadata(obj, MODULE) is not None:
            return obj
    return None


def _error_exit(message: str) -> None:
    """Write a JSON error to stderr and exit with code 1."""
    print(json.dumps({"error": message}), file=sys.stderr)
    sys.exit(1)
