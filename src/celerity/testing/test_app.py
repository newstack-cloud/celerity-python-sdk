"""Test harness for Celerity applications."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from celerity.bootstrap.bootstrap import bootstrap
from celerity.handlers.consumer_pipeline import execute_consumer_pipeline
from celerity.handlers.custom_pipeline import execute_custom_pipeline
from celerity.handlers.http_pipeline import execute_http_pipeline
from celerity.handlers.schedule_pipeline import execute_schedule_pipeline
from celerity.handlers.websocket_pipeline import execute_websocket_pipeline
from celerity.layers.system import create_default_system_layers
from celerity.types.http import HttpRequest, HttpResponse

if TYPE_CHECKING:
    from celerity.di.container import Container
    from celerity.handlers.registry import HandlerRegistry
    from celerity.types.consumer import ConsumerEventInput, EventResult
    from celerity.types.schedule import ScheduleEventInput
    from celerity.types.websocket import WebSocketMessage

logger = logging.getLogger("celerity.testing")


class TestApp:
    """Test harness for Celerity applications.

    Bootstraps the module, resolves DI, and provides injection methods
    for all handler types. Supports provider overrides for mocking.

    Usage::

        app = await TestApp.create(AppModule, overrides={
            OrderService: MockOrderService(),
        })
        response = await app.http_get("/orders/123", auth={"sub": "user1"})
        assert response.status == 200
        await app.close()
    """

    __test__ = False

    def __init__(
        self,
        container: Container,
        registry: HandlerRegistry,
        system_layers: list[Any],
    ) -> None:
        self.container = container
        self.registry = registry
        self.system_layers = system_layers

    @classmethod
    async def create(
        cls,
        root_module: type,
        *,
        overrides: dict[Any, Any] | None = None,
    ) -> TestApp:
        """Bootstrap and create a test app with optional provider overrides.

        Args:
            root_module: The root ``@module``-decorated class.
            overrides: ``{token: mock_value}`` pairs that replace
                providers in the DI container after bootstrap.

        Returns:
            A bootstrapped ``TestApp`` ready for injection.
        """
        container, registry, _graph = await bootstrap(root_module)
        system_layers = create_default_system_layers()

        if overrides:
            for token, value in overrides.items():
                container.register_value(token, value)
                logger.debug("override: %s", token)

        logger.debug(
            "created: %d handlers, %d guards",
            len(registry.get_all_handlers()),
            len(registry.get_all_guards()),
        )
        return cls(container, registry, system_layers)

    # -- HTTP injection --

    async def http_get(self, path: str, **kwargs: Any) -> HttpResponse:
        """Inject a GET request."""
        return await self._inject_http("GET", path, **kwargs)

    async def http_post(
        self,
        path: str,
        body: Any = None,
        **kwargs: Any,
    ) -> HttpResponse:
        """Inject a POST request."""
        return await self._inject_http("POST", path, body=body, **kwargs)

    async def http_put(
        self,
        path: str,
        body: Any = None,
        **kwargs: Any,
    ) -> HttpResponse:
        """Inject a PUT request."""
        return await self._inject_http("PUT", path, body=body, **kwargs)

    async def http_patch(
        self,
        path: str,
        body: Any = None,
        **kwargs: Any,
    ) -> HttpResponse:
        """Inject a PATCH request."""
        return await self._inject_http("PATCH", path, body=body, **kwargs)

    async def http_delete(self, path: str, **kwargs: Any) -> HttpResponse:
        """Inject a DELETE request."""
        return await self._inject_http("DELETE", path, **kwargs)

    # -- WebSocket injection --

    async def inject_websocket(
        self,
        route: str,
        message: WebSocketMessage,
    ) -> None:
        """Inject a WebSocket message into the handler pipeline.

        Args:
            route: The WebSocket route key (e.g. ``"chat"``, ``"$connect"``).
            message: The WebSocket message to inject.

        Raises:
            ValueError: If no handler is registered for the route.
        """
        handler = self.registry.get_handler("websocket", route)
        if handler is None:
            msg = f"No WebSocket handler for route: {route}"
            raise ValueError(msg)
        await execute_websocket_pipeline(handler, message, self._pipeline_options())

    # -- Consumer injection --

    async def inject_consumer(
        self,
        handler_tag: str,
        event: ConsumerEventInput,
    ) -> EventResult:
        """Inject a consumer event into the handler pipeline.

        Args:
            handler_tag: The consumer handler tag.
            event: The consumer event input.

        Returns:
            The ``EventResult`` from the handler.

        Raises:
            ValueError: If no handler is registered for the tag.
        """
        handler = self.registry.get_handler("consumer", handler_tag)
        if handler is None:
            msg = f"No consumer handler for tag: {handler_tag}"
            raise ValueError(msg)
        return await execute_consumer_pipeline(handler, event, self._pipeline_options())

    # -- Schedule injection --

    async def inject_schedule(
        self,
        handler_tag: str,
        event: ScheduleEventInput,
    ) -> EventResult:
        """Inject a schedule event into the handler pipeline.

        Args:
            handler_tag: The schedule handler tag.
            event: The schedule event input.

        Returns:
            The ``EventResult`` from the handler.

        Raises:
            ValueError: If no handler is registered for the tag.
        """
        handler = self.registry.get_handler("schedule", handler_tag)
        if handler is None:
            msg = f"No schedule handler for tag: {handler_tag}"
            raise ValueError(msg)
        return await execute_schedule_pipeline(handler, event, self._pipeline_options())

    # -- Custom handler injection --

    async def inject_custom(self, name: str, payload: Any = None) -> Any:
        """Invoke a custom handler by name.

        Args:
            name: The custom handler name.
            payload: The payload to pass to the handler.

        Returns:
            The handler's return value.

        Raises:
            ValueError: If no handler is registered with the name.
        """
        handler = self.registry.get_handler("custom", name)
        if handler is None:
            msg = f"No custom handler named: {name}"
            raise ValueError(msg)
        return await execute_custom_pipeline(handler, payload, self._pipeline_options())

    # -- Lifecycle --

    async def close(self) -> None:
        """Shut down the test app and release resources."""
        await self.container.close_all()

    def get_container(self) -> Container:
        """Get the DI container."""
        return self.container

    def get_registry(self) -> HandlerRegistry:
        """Get the handler registry."""
        return self.registry

    # -- Private --

    def _pipeline_options(self) -> dict[str, Any]:
        return {
            "container": self.container,
            "system_layers": self.system_layers,
        }

    async def _inject_http(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        auth: dict[str, Any] | None = None,
        headers: dict[str, str | list[str]] | None = None,
        query: dict[str, str | list[str]] | None = None,
        cookies: dict[str, str] | None = None,
        path_params: dict[str, str] | None = None,
        request_id: str = "test-request-id",
        client_ip: str = "127.0.0.1",
    ) -> HttpResponse:
        """Build an HttpRequest and dispatch through the HTTP pipeline."""
        # Auto-extract path params from the registry if not provided.
        resolved_params = path_params
        if resolved_params is None:
            resolved_params = self.registry.extract_path_params(f"{method} {path}")

        request = HttpRequest(
            method=method,
            path=path,
            path_params=resolved_params or {},
            headers=headers or {},
            query=query or {},
            cookies=cookies or {},
            auth=auth,
            text_body=json.dumps(body) if body is not None else None,
            content_type="application/json" if body is not None else None,
            request_id=request_id,
            client_ip=client_ip,
        )

        handler = self.registry.get_handler("http", f"{method} {path}")
        if handler is None:
            return HttpResponse(status=404)

        return await execute_http_pipeline(handler, request, self._pipeline_options())
