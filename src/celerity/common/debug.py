"""SDK-internal debug logging configured via ``CELERITY_DEBUG``.

Provides scoped debug logging for SDK internals. Developers enable
debug output by setting the ``CELERITY_DEBUG`` environment variable
to a comma-separated list of logger name patterns:

.. code-block:: bash

    # Enable all SDK debug logs
    CELERITY_DEBUG=celerity.*

    # Enable only DI and bootstrap logs
    CELERITY_DEBUG=celerity.di,celerity.bootstrap

    # Enable all scanner logs
    CELERITY_DEBUG=celerity.scanner.*

    # Enable everything except layer pipeline
    CELERITY_DEBUG=celerity.*,-celerity.layers

Logger names follow a dotted hierarchy::

    celerity.di           - Dependency injection
    celerity.bootstrap    - Module graph and bootstrap
    celerity.registry     - Handler registry
    celerity.scanner.http - HTTP handler scanning
    celerity.scanner.ws   - WebSocket handler scanning
    celerity.scanner.consumer - Consumer handler scanning
    celerity.scanner.schedule - Schedule handler scanning
    celerity.scanner.custom   - Custom handler scanning
    celerity.pipeline     - HTTP pipeline
    celerity.pipeline.ws  - WebSocket pipeline
    celerity.pipeline.consumer - Consumer pipeline
    celerity.pipeline.schedule - Schedule pipeline
    celerity.pipeline.custom   - Custom pipeline
    celerity.pipeline.guard    - Guard chain
    celerity.layers       - Layer pipeline composition
    celerity.factory      - Application factory
"""

from __future__ import annotations

import fnmatch
import logging
import os

_configured = False


def configure_debug_logging() -> None:
    """Configure SDK debug logging from the ``CELERITY_DEBUG`` env var.

    Called once at import time. Parses comma-separated patterns and
    enables DEBUG level on matching loggers. Patterns support ``*``
    wildcards and ``-`` prefix for exclusion.

    If ``CELERITY_DEBUG`` is not set, SDK loggers remain at their
    default level (WARNING), producing no output.
    """
    global _configured
    if _configured:
        return
    _configured = True

    debug_env = os.environ.get("CELERITY_DEBUG", "").strip()
    if not debug_env:
        return

    patterns = [p.strip() for p in debug_env.split(",") if p.strip()]
    if not patterns:
        return

    include: list[str] = []
    exclude: list[str] = []
    for pattern in patterns:
        if pattern.startswith("-"):
            exclude.append(pattern[1:])
        else:
            include.append(pattern)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(name)s %(message)s"))

    root_celerity = logging.getLogger("celerity")
    if not root_celerity.handlers:
        root_celerity.addHandler(handler)

    _apply_patterns(include, exclude)


def _apply_patterns(include: list[str], exclude: list[str]) -> None:
    """Enable DEBUG on loggers matching include patterns, skip excluded."""
    all_names = _get_sdk_logger_names()

    for name in all_names:
        included = any(fnmatch.fnmatch(name, pat) for pat in include)
        excluded = any(fnmatch.fnmatch(name, pat) for pat in exclude)
        if included and not excluded:
            logging.getLogger(name).setLevel(logging.DEBUG)


def _get_sdk_logger_names() -> list[str]:
    """Return all known SDK logger names."""
    return [
        # Phase 3-4: DI, bootstrap, handlers
        "celerity.di",
        "celerity.bootstrap",
        "celerity.registry",
        "celerity.scanner.http",
        "celerity.scanner.ws",
        "celerity.scanner.consumer",
        "celerity.scanner.schedule",
        "celerity.scanner.custom",
        "celerity.pipeline",
        "celerity.pipeline.ws",
        "celerity.pipeline.consumer",
        "celerity.pipeline.schedule",
        "celerity.pipeline.custom",
        "celerity.pipeline.guard",
        "celerity.layers",
        "celerity.factory",
        # Phase 5: CLI extraction
        "celerity.cli",
        # Phase 6: Runtime and serverless
        "celerity.runtime",
        "celerity.runtime.mapper",
        "celerity.serverless.aws",
        "celerity.serverless.gcp",
        "celerity.serverless.azure",
        # Phase 7: Testing
        "celerity.testing",
        # Phase 8: Resources
        "celerity.resource.bucket",
        "celerity.resource.queue",
        "celerity.resource.topic",
        "celerity.resource.datastore",
        "celerity.resource.cache",
        "celerity.resource.sql",
        "celerity.resource.layer",
        # Phase 9: Telemetry
        "celerity.telemetry",
    ]


# Auto-configure on import
configure_debug_logging()
