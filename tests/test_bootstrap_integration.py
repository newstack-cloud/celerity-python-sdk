"""Integration tests for the full bootstrap -> pipeline flow."""

from celerity.bootstrap.bootstrap import bootstrap
from celerity.decorators.consumer import consumer, message_handler
from celerity.decorators.controller import controller
from celerity.decorators.guards import guard, protected_by
from celerity.decorators.http import get, post
from celerity.decorators.injectable import injectable
from celerity.decorators.invoke import invoke
from celerity.decorators.module import module
from celerity.decorators.params import Body, Param
from celerity.decorators.schedule import schedule_handler
from celerity.decorators.websocket import on_connect, on_message, ws_controller
from celerity.handlers.guard_pipeline import GuardPipelineOptions, execute_guard_pipeline
from celerity.handlers.http_pipeline import execute_http_pipeline
from celerity.types.consumer import EventResult
from celerity.types.guard import GuardInput, GuardResult
from celerity.types.handler import ResolvedHttpHandler
from celerity.types.http import HttpRequest, HttpResponse


@injectable()
class OrderService:
    def get_order(self, order_id: str) -> dict[str, str]:
        return {"id": order_id, "name": "Test Order"}


@guard("admin")
class AdminGuard:
    async def validate(self, input: GuardInput) -> GuardResult:
        if input.auth.get("role") == "admin":
            return GuardResult.allow(auth={"role": "admin"})
        return GuardResult.forbidden("Admin access required")


@controller("/orders")
class OrderController:
    def __init__(self, svc: OrderService) -> None:
        self.svc = svc

    @get("/")
    async def list_orders(self) -> HttpResponse:
        return HttpResponse(status=200, body='{"orders": []}')

    @get("/{order_id}")
    async def get_order(self, order_id: Param[str]) -> HttpResponse:
        order = self.svc.get_order(str(order_id) if order_id else "")
        return HttpResponse(status=200, body=str(order))

    @post("/")
    @protected_by("admin")
    async def create_order(self, body: Body[dict[str, str]]) -> HttpResponse:
        return HttpResponse(status=201, body='{"created": true}')

    @schedule_handler("rate(1 day)")
    async def cleanup(self) -> EventResult:
        return EventResult(success=True)

    @invoke("processOrder")
    async def process(self) -> dict[str, bool]:
        return {"processed": True}


@ws_controller()
class ChatController:
    @on_connect()
    async def connect(self) -> None:
        pass

    @on_message("chat")
    async def handle_message(self) -> None:
        pass


@consumer("orders-queue")
class OrderConsumer:
    @message_handler()
    async def handle(self) -> EventResult:
        return EventResult(success=True)


@module(
    controllers=[OrderController, ChatController, OrderConsumer],
    providers=[OrderService],
    guards=[AdminGuard],
)
class AppModule:
    pass


class TestBootstrap:
    async def test_bootstrap_registers_all_handlers(self) -> None:
        container, registry, _graph = await bootstrap(AppModule)

        http_handlers = registry.get_handlers_by_type("http")
        assert len(http_handlers) >= 3

        ws_handlers = registry.get_handlers_by_type("websocket")
        assert len(ws_handlers) >= 2

        consumer_handlers = registry.get_handlers_by_type("consumer")
        assert len(consumer_handlers) >= 1

        schedule_handlers = registry.get_handlers_by_type("schedule")
        assert len(schedule_handlers) >= 1

        custom_handlers = registry.get_handlers_by_type("custom")
        assert len(custom_handlers) >= 1

        guards = registry.get_all_guards()
        assert len(guards) >= 1
        assert guards[0].name == "admin"

        await container.close_all()

    async def test_di_resolution_through_bootstrap(self) -> None:
        container, _registry, _graph = await bootstrap(AppModule)

        ctrl = await container.resolve(OrderController)
        assert isinstance(ctrl.svc, OrderService)

        await container.close_all()

    async def test_http_pipeline_end_to_end(self) -> None:
        container, registry, _graph = await bootstrap(AppModule)

        handler = registry.get_handler("http", "GET /orders")
        assert handler is not None
        assert isinstance(handler, ResolvedHttpHandler)

        request = HttpRequest(method="GET", path="/orders")
        response = await execute_http_pipeline(
            handler,
            request,
            {"container": container},
        )
        assert response.status == 200
        assert response.body is not None
        assert "orders" in response.body

        await container.close_all()

    async def test_http_pipeline_with_path_params(self) -> None:
        container, registry, _graph = await bootstrap(AppModule)

        handler = registry.get_handler("http", "GET /orders/abc-123")
        assert handler is not None
        assert isinstance(handler, ResolvedHttpHandler)

        request = HttpRequest(
            method="GET",
            path="/orders/abc-123",
            path_params={"order_id": "abc-123"},
        )
        response = await execute_http_pipeline(
            handler,
            request,
            {"container": container},
        )
        assert response.status == 200

        await container.close_all()

    async def test_guard_rejects_unauthenticated(self) -> None:
        """Guard pipeline returns denied result for missing auth."""
        container, registry, _graph = await bootstrap(AppModule)

        guard_handler = registry.get_guard("admin")
        assert guard_handler is not None

        guard_input = GuardInput(
            token="",
            method="POST",
            path="/orders",
            headers={},
            query={},
            cookies={},
            body=None,
            request_id="test-req-1",
            client_ip="127.0.0.1",
            auth={},
        )

        result = await execute_guard_pipeline(
            guard_handler,
            guard_input,
            GuardPipelineOptions(container=container),
        )
        assert not result.allowed
        assert result.status_code == 403

        await container.close_all()

    async def test_guard_allows_admin(self) -> None:
        """Guard pipeline returns allowed result for admin role."""
        container, registry, _graph = await bootstrap(AppModule)

        guard_handler = registry.get_guard("admin")
        assert guard_handler is not None

        guard_input = GuardInput(
            token="",
            method="POST",
            path="/orders",
            headers={},
            query={},
            cookies={},
            body=None,
            request_id="test-req-2",
            client_ip="127.0.0.1",
            auth={"role": "admin"},
        )

        result = await execute_guard_pipeline(
            guard_handler,
            guard_input,
            GuardPipelineOptions(container=container),
        )
        assert result.allowed
        assert result.auth is not None
        assert result.auth.get("role") == "admin"

        await container.close_all()
