"""Bootstrap for runtime mode and callback factory creation.

Provides ``bootstrap_for_runtime()`` which bootstraps the application
and returns a ``RuntimeBootstrapResult`` with per-handler-type callback
factories. Each callback bridges between PyO3 runtime types and the
SDK handler pipeline.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from celerity.bootstrap.bootstrap import bootstrap
from celerity.bootstrap.discovery import discover_module
from celerity.bootstrap.runtime_mapper import (
    map_runtime_consumer_event,
    map_runtime_guard_input,
    map_runtime_request,
    map_runtime_schedule_event,
    map_runtime_websocket_message,
    map_to_runtime_event_result,
    map_to_runtime_guard_result,
    map_to_runtime_response,
)
from celerity.handlers.consumer_pipeline import execute_consumer_pipeline
from celerity.handlers.custom_pipeline import execute_custom_pipeline
from celerity.handlers.guard_pipeline import (
    GuardPipelineOptions,
    execute_guard_pipeline,
)
from celerity.handlers.http_pipeline import execute_http_pipeline
from celerity.handlers.schedule_pipeline import execute_schedule_pipeline
from celerity.handlers.websocket_pipeline import execute_websocket_pipeline
from celerity.layers.system import create_default_system_layers

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from celerity_runtime_sdk import (
        ConsumerEventInput as RuntimeConsumerEvent,
    )
    from celerity_runtime_sdk import (
        EventResult as RuntimeEventResult,
    )
    from celerity_runtime_sdk import (
        GuardInput as RuntimeGuardInput,
    )
    from celerity_runtime_sdk import (
        GuardResult as RuntimeGuardResult,
    )
    from celerity_runtime_sdk import (
        Request as RuntimeRequest,
    )
    from celerity_runtime_sdk import (
        RequestContext as RuntimeRequestContext,
    )
    from celerity_runtime_sdk import (
        Response as RuntimeResponse,
    )
    from celerity_runtime_sdk import (
        ScheduleEventInput as RuntimeScheduleEvent,
    )
    from celerity_runtime_sdk import (
        WebSocketMessageInfo as RuntimeWebSocketMsg,
    )

    from celerity.di.container import Container
    from celerity.handlers.registry import HandlerRegistry

logger = logging.getLogger("celerity.runtime")


class RuntimeBootstrapResult:
    """Result of bootstrapping for runtime mode.

    Provides callback factories that create async closures matching
    the PyO3 handler signature expectations. Each factory looks up
    the handler in the registry and returns a callback that bridges
    PyO3 types through the SDK pipeline.
    """

    def __init__(
        self,
        container: Container,
        registry: HandlerRegistry,
        system_layers: list[Any],
    ) -> None:
        self.container = container
        self.registry = registry
        self.system_layers = system_layers

    def create_route_callback(
        self,
        method: str,
        path: str,
        handler_name: str | None = None,
    ) -> Callable[..., Awaitable[RuntimeResponse]] | None:
        """Create an async callback for an HTTP route."""
        handler = self.registry.get_handler("http", f"{method} {path}")
        if handler is None:
            return None
        return self._build_http_callback(handler, handler_name)

    def create_route_callback_by_id(
        self,
        handler_id: str,
        handler_name: str | None = None,
    ) -> Callable[..., Awaitable[RuntimeResponse]] | None:
        """Create an async callback for an HTTP route by handler ID."""
        handler = self.registry.get_handler_by_id("http", handler_id)
        if handler is None:
            return None
        return self._build_http_callback(handler, handler_name)

    def create_guard_callback(
        self,
        guard_name: str,
    ) -> Callable[[RuntimeGuardInput], Awaitable[RuntimeGuardResult]] | None:
        """Create an async callback for a guard."""
        guard = self.registry.get_guard(guard_name)
        if guard is None:
            return None

        container = self.container
        registry = self.registry

        async def callback(py_guard_input: RuntimeGuardInput) -> RuntimeGuardResult:
            sdk_input = map_runtime_guard_input(py_guard_input)
            matched_handler = registry.get_handler(
                "http",
                f"{sdk_input.method} {sdk_input.path}",
            )
            pipeline_result = await execute_guard_pipeline(
                guard,
                sdk_input,
                GuardPipelineOptions(
                    container=container,
                    handler_metadata=(matched_handler.custom_metadata if matched_handler else None),
                ),
            )
            return map_to_runtime_guard_result(pipeline_result)

        return callback

    def create_websocket_callback(
        self,
        route: str,
        handler_name: str | None = None,
    ) -> Callable[[RuntimeWebSocketMsg], Awaitable[None]] | None:
        """Create an async callback for a WebSocket route."""
        handler = self.registry.get_handler("websocket", route)
        if handler is None:
            return None

        container = self.container
        layers = self.system_layers

        async def callback(py_ws_msg: RuntimeWebSocketMsg) -> None:
            sdk_message = map_runtime_websocket_message(py_ws_msg)
            await execute_websocket_pipeline(
                handler,
                sdk_message,
                {
                    "container": container,
                    "system_layers": layers,
                    "handler_name": handler_name,
                },
            )

        return callback

    def create_consumer_callback(
        self,
        handler_tag: str,
        handler_name: str | None = None,
    ) -> Callable[[RuntimeConsumerEvent], Awaitable[RuntimeEventResult]] | None:
        """Create an async callback for a consumer handler."""
        handler = self.registry.get_handler("consumer", handler_tag)
        if handler is None:
            return None

        container = self.container
        layers = self.system_layers

        async def callback(py_event: RuntimeConsumerEvent) -> RuntimeEventResult:
            sdk_event = map_runtime_consumer_event(py_event)
            sdk_result = await execute_consumer_pipeline(
                handler,
                sdk_event,
                {
                    "container": container,
                    "system_layers": layers,
                    "handler_name": handler_name,
                },
            )
            return map_to_runtime_event_result(sdk_result)

        return callback

    def create_schedule_callback(
        self,
        handler_tag: str,
        handler_name: str | None = None,
    ) -> Callable[[RuntimeScheduleEvent], Awaitable[RuntimeEventResult]] | None:
        """Create an async callback for a schedule handler."""
        handler = self.registry.get_handler("schedule", handler_tag)
        if handler is None:
            return None

        container = self.container
        layers = self.system_layers

        async def callback(py_event: RuntimeScheduleEvent) -> RuntimeEventResult:
            sdk_event = map_runtime_schedule_event(py_event)
            sdk_result = await execute_schedule_pipeline(
                handler,
                sdk_event,
                {
                    "container": container,
                    "system_layers": layers,
                    "handler_name": handler_name,
                },
            )
            return map_to_runtime_event_result(sdk_result)

        return callback

    def create_custom_callback(
        self,
        handler_name: str,
    ) -> Callable[..., Awaitable[Any]] | None:
        """Create an async callback for a custom/invoke handler."""
        handler = self.registry.get_handler("custom", handler_name)
        if handler is None:
            return None

        container = self.container
        layers = self.system_layers

        async def callback(payload: Any) -> Any:
            return await execute_custom_pipeline(
                handler,
                payload,
                {
                    "container": container,
                    "system_layers": layers,
                    "handler_name": handler_name,
                },
            )

        return callback

    def _build_http_callback(
        self,
        handler: Any,
        handler_name: str | None,
    ) -> Callable[..., Awaitable[RuntimeResponse]]:
        """Internal helper to build an HTTP callback from a resolved handler."""
        container = self.container
        layers = self.system_layers

        async def callback(
            py_request: RuntimeRequest,
            py_context: RuntimeRequestContext,
        ) -> RuntimeResponse:
            sdk_request = map_runtime_request(py_request, py_context)
            sdk_response = await execute_http_pipeline(
                handler,
                sdk_request,
                {
                    "container": container,
                    "system_layers": layers,
                    "handler_name": handler_name,
                },
            )
            return map_to_runtime_response(sdk_response)

        return callback


async def bootstrap_for_runtime(
    module_path: str | None = None,
    system_layers: list[Any] | None = None,
) -> RuntimeBootstrapResult:
    """Bootstrap the SDK for runtime mode.

    1. Discover the root module from ``CELERITY_MODULE_PATH`` or
       the explicit ``module_path`` argument.
    2. Create default system layers (telemetry, config, resources).
    3. Bootstrap DI + handler registry.
    4. Return ``RuntimeBootstrapResult`` with callback factories.

    Args:
        module_path: Optional explicit path to the module file.
        system_layers: Optional override for system layers.

    Returns:
        A ``RuntimeBootstrapResult`` ready for handler registration.
    """
    layers = system_layers if system_layers is not None else await create_default_system_layers()
    root_module = discover_module(module_path)
    container, registry, _graph = await bootstrap(root_module)

    logger.debug(
        "bootstrap_for_runtime: %d handlers, %d guards",
        len(registry.get_all_handlers()),
        len(registry.get_all_guards()),
    )

    return RuntimeBootstrapResult(container, registry, layers)
