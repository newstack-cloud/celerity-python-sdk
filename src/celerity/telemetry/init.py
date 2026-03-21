"""OpenTelemetry SDK initialization."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celerity.telemetry.env import TelemetryConfig

logger = logging.getLogger("celerity.telemetry")

_initialized = False
_tracer_provider: object | None = None


async def init_telemetry(config: TelemetryConfig) -> None:
    """Initialise the OpenTelemetry SDK.

    Configures:

    - OTLP trace exporter (gRPC to ``config.otlp_endpoint``)
    - Composite propagator: W3C TraceContext + AWS X-Ray
    - AWS X-Ray ID generator (if ``CELERITY_PLATFORM`` starts with ``"aws"``)
    - Auto-instrumentations (HTTP, database clients, Redis, etc.)
    - Service resource attributes (name, version)
    """
    global _initialized, _tracer_provider

    if _initialized:
        return

    import os

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.propagate import set_global_textmap
    from opentelemetry.propagators.composite import CompositePropagator
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    if TYPE_CHECKING:
        from opentelemetry.propagators.textmap import TextMapPropagator

    # Build resource attributes
    resource = Resource.create(
        {
            "service.name": config.service_name,
            "service.version": config.service_version,
        }
    )

    # Configure ID generator for AWS X-Ray compatibility
    platform = os.environ.get("CELERITY_PLATFORM", "")
    id_generator = None
    if platform.startswith("aws"):
        try:
            from opentelemetry.sdk.extension.aws.trace import AwsXRayIdGenerator

            id_generator = AwsXRayIdGenerator()
            logger.debug("Using AWS X-Ray ID generator")
        except ImportError:
            logger.debug("AWS X-Ray ID generator not available")

    # Set up TracerProvider
    if id_generator is not None:
        provider = TracerProvider(resource=resource, id_generator=id_generator)
    else:
        provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=config.otlp_endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _tracer_provider = provider

    # Set up composite propagator (W3C + AWS X-Ray)
    propagators: list[TextMapPropagator] = [TraceContextTextMapPropagator()]
    try:
        from opentelemetry.propagators.aws import AwsXRayPropagator

        propagators.append(AwsXRayPropagator())
    except ImportError:
        pass
    set_global_textmap(CompositePropagator(propagators))

    # Load auto-instrumentations
    from celerity.telemetry.instrumentations import load_instrumentations

    loaded = load_instrumentations()
    logger.debug("Loaded %d instrumentation(s): %s", len(loaded), ", ".join(loaded))

    _initialized = True
    logger.debug(
        "OTel initialised: service=%s endpoint=%s",
        config.service_name,
        config.otlp_endpoint,
    )


def is_initialized() -> bool:
    """Check if OTel SDK has been initialised."""
    return _initialized


async def shutdown_telemetry() -> None:
    """Gracefully shut down the OTel SDK, flushing pending spans."""
    global _initialized, _tracer_provider

    if not _initialized or _tracer_provider is None:
        return

    from opentelemetry.sdk.trace import TracerProvider

    if isinstance(_tracer_provider, TracerProvider):
        _tracer_provider.force_flush()
        _tracer_provider.shutdown()

    _tracer_provider = None
    _initialized = False
    logger.debug("OTel shut down")
