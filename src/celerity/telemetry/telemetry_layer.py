"""TelemetryLayer system layer.

Loaded as the first system layer in the handler pipeline. Provides
request-scoped structured logging and optional distributed tracing.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

from celerity.telemetry.env import read_telemetry_env
from celerity.telemetry.helpers import LOGGER_TOKEN, TRACER_TOKEN
from celerity.telemetry.logger import CelerityLoggerImpl, create_logger
from celerity.telemetry.noop import NoopTracer
from celerity.telemetry.request_context import (
    ContextAwareLogger,
    clear_request_logger,
    set_request_logger,
)
from celerity.types.layer import CelerityLayer

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from celerity.types.context import BaseHandlerContext
    from celerity.types.telemetry import CelerityLogger, CelerityTracer

logger = logging.getLogger("celerity.telemetry")


class TelemetryLayer(CelerityLayer):
    """System layer that initialises logging and tracing.

    Lifecycle:

    1. Constructor: reads config, starts async OTel init if tracing enabled
    2. First request: awaits OTel init, creates logger/tracer, registers in DI
    3. Per-request: builds request-scoped logger, extracts trace context
    4. Dispose: graceful OTel SDK shutdown
    """

    def __init__(self) -> None:
        self._config = read_telemetry_env()
        self._init_task: asyncio.Task[None] | None = None
        self._root_logger: CelerityLoggerImpl | None = None
        self._tracer: CelerityTracer | None = None
        self._initialized = False

        if self._config.tracing_enabled:
            try:
                loop = asyncio.get_running_loop()
                from celerity.telemetry.init import init_telemetry

                self._init_task = loop.create_task(init_telemetry(self._config))
            except RuntimeError:
                pass

    async def handle(
        self,
        context: BaseHandlerContext,
        next_handler: Callable[..., Awaitable[Any]],
    ) -> Any:
        if not self._initialized:
            await self._first_request_init(context.container)

        await self._refresh_log_level(context.container)

        request_logger = self._build_request_logger(context)
        otel_context = self._extract_trace_context(context)

        if request_logger is not None:
            set_request_logger(request_logger)
        context.logger = request_logger
        try:
            if otel_context:
                from opentelemetry import context as otel_ctx

                otel_token = otel_ctx.attach(otel_context)
                try:
                    return await next_handler()
                finally:
                    otel_ctx.detach(otel_token)
            else:
                return await next_handler()
        finally:
            clear_request_logger()

    async def _first_request_init(self, container: Any) -> None:
        """Complete initialisation on first request."""
        if self._init_task is not None:
            await self._init_task
            self._init_task = None

        self._root_logger = create_logger(self._config)

        if self._config.tracing_enabled:
            from celerity.telemetry.tracer import OTelTracer

            self._tracer = OTelTracer()
        else:
            self._tracer = NoopTracer()

        context_logger = ContextAwareLogger(self._root_logger)
        container.register_value(LOGGER_TOKEN, context_logger)
        container.register_value(TRACER_TOKEN, self._tracer)

        self._initialized = True
        logger.debug("TelemetryLayer initialised (tracing=%s)", self._config.tracing_enabled)

    async def _refresh_log_level(self, container: Any) -> None:
        """Check ConfigService for dynamic log level changes."""
        if self._root_logger is None:
            return

        try:
            from celerity.config.service import CONFIG_SERVICE_TOKEN

            config_service = await container.resolve(CONFIG_SERVICE_TOKEN)
            ns = config_service.namespace("app")
            level = await ns.get("CELERITY_LOG_LEVEL")
            if level:
                self._root_logger.set_level(level)
        except Exception:
            pass

    def _build_request_logger(self, context: BaseHandlerContext) -> CelerityLogger | None:
        """Build a request-scoped logger enriched with handler context."""
        if self._root_logger is None:
            return None

        from celerity.types.context import (
            ConsumerHandlerContext,
            HttpHandlerContext,
            ScheduleHandlerContext,
        )

        if isinstance(context, HttpHandlerContext) and context.request is not None:
            req = context.request
            return self._root_logger.with_context(
                request_id=req.request_id,
                method=req.method,
                path=req.path,
                matched_route=req.matched_route or "",
                client_ip=req.client_ip or "",
                user_agent=req.user_agent or "",
            )

        if isinstance(context, ConsumerHandlerContext) and context.event is not None:
            event = context.event
            return self._root_logger.with_context(
                source=event.handler_tag,
                message_count=len(event.messages),
            )

        if isinstance(context, ScheduleHandlerContext):
            return self._root_logger.with_context(
                handler_type="schedule",
            )

        return self._root_logger

    def _extract_trace_context(self, context: BaseHandlerContext) -> Any:
        """Extract trace context from request/event if available."""
        if not self._config.tracing_enabled:
            return None

        from celerity.telemetry.context import extract_trace_context
        from celerity.types.context import (
            ConsumerHandlerContext,
            HttpHandlerContext,
            ScheduleHandlerContext,
        )

        trace_ctx: dict[str, str] | None = None

        if isinstance(context, HttpHandlerContext) and context.request is not None:
            trace_ctx = context.request.trace_context
        elif (
            isinstance(context, (ConsumerHandlerContext, ScheduleHandlerContext))
            and context.event is not None
        ):
            trace_ctx = context.event.trace_context

        return extract_trace_context(trace_ctx)

    async def dispose(self) -> None:
        """Shut down OTel SDK if it was initialised."""
        if self._config.tracing_enabled:
            with contextlib.suppress(Exception):
                from celerity.telemetry.init import shutdown_telemetry

                await shutdown_telemetry()
