"""Tests for module discovery."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from celerity.bootstrap.discovery import discover_module


class TestDiscoverModule:
    def test_discovers_from_explicit_path(self) -> None:
        """discover_module finds the @module class from an explicit file path."""
        # Use the test fixtures that already exist in the test suite.
        # Create a temporary module file with a @module class.
        with tempfile.TemporaryDirectory() as tmpdir:
            module_file = Path(tmpdir) / "sample_app.py"
            module_file.write_text(
                "from celerity.decorators.module import module\n"
                "\n"
                "@module()\n"
                "class SampleModule:\n"
                "    pass\n"
            )
            result = discover_module(str(module_file))
            assert result.__name__ == "SampleModule"

    def test_discovers_from_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """discover_module falls back to CELERITY_MODULE_PATH env var."""
        with tempfile.TemporaryDirectory() as tmpdir:
            module_file = Path(tmpdir) / "env_app.py"
            module_file.write_text(
                "from celerity.decorators.module import module\n"
                "\n"
                "@module()\n"
                "class EnvModule:\n"
                "    pass\n"
            )
            monkeypatch.setenv("CELERITY_MODULE_PATH", str(module_file))
            result = discover_module()
            assert result.__name__ == "EnvModule"

    def test_raises_when_no_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """discover_module raises RuntimeError when no path is available."""
        monkeypatch.delenv("CELERITY_MODULE_PATH", raising=False)
        with pytest.raises(RuntimeError, match="No module path provided"):
            discover_module()

    def test_raises_when_no_module_class(self) -> None:
        """discover_module raises when the file has no @module class."""
        with tempfile.TemporaryDirectory() as tmpdir:
            module_file = Path(tmpdir) / "no_module.py"
            module_file.write_text("class PlainClass:\n    pass\n")
            with pytest.raises(RuntimeError, match="No @module class found"):
                discover_module(str(module_file))
