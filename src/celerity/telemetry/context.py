"""Trace context extraction from request/event carriers."""

from __future__ import annotations

from typing import Any


def extract_trace_context(trace_context: dict[str, str] | None) -> Any:
    """Extract OTel context from a trace context carrier map.

    The carrier map comes from:

    - HTTP: ``request.trace_context``
    - Consumer: ``event.trace_context``
    - Schedule: ``event.trace_context``

    Uses the globally configured textmap propagator. When
    ``init_telemetry()`` has run, this is a composite propagator
    (W3C TraceContext + AWS X-Ray). Otherwise it uses the default
    W3C-only propagator.

    Returns an OTel Context object or ``None`` if no trace context present.
    """
    if not trace_context:
        return None

    from opentelemetry.propagate import extract

    return extract(carrier=trace_context)
