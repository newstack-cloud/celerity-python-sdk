"""Telemetry DI tokens and container helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celerity.types.container import ServiceContainer
    from celerity.types.telemetry import CelerityLogger, CelerityTracer

LOGGER_TOKEN = "CelerityLogger"
TRACER_TOKEN = "CelerityTracer"


async def get_logger(container: ServiceContainer) -> CelerityLogger:
    """Resolve the CelerityLogger from the DI container."""
    return await container.resolve(LOGGER_TOKEN)  # type: ignore[no-any-return]


async def get_tracer(container: ServiceContainer) -> CelerityTracer:
    """Resolve the CelerityTracer from the DI container."""
    return await container.resolve(TRACER_TOKEN)  # type: ignore[no-any-return]
