"""Shared pytest fixtures for Celerity SDK tests."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

from celerity.resources._common import RESOURCE_LINKS_FILENAME


@pytest.fixture(autouse=True)
def resource_links_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Callable[[dict[str, Any]], Path]]:
    """Provide every test with a resource links file the SDK can read.

    ``capture_resource_links()`` fails hard when the file is missing, but
    most unit tests don't care about resource topology — they just need the
    application bootstrap to get past the system-layer construction step.

    This autouse fixture writes an empty ``{}`` file to a per-test temp dir
    and points ``CELERITY_RESOURCE_LINKS_PATH`` at it. Tests that need
    specific links overwrite the contents by calling the returned helper
    with a links mapping.
    """
    links_path: Path = tmp_path / RESOURCE_LINKS_FILENAME
    links_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("CELERITY_RESOURCE_LINKS_PATH", str(links_path))

    def _write(links: dict[str, Any]) -> Path:
        links_path.write_text(json.dumps(links), encoding="utf-8")
        return links_path

    yield _write
