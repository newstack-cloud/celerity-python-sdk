"""Tests for the TestApp testing harness."""

from __future__ import annotations

import pytest

from celerity.decorators.consumer import consumer, message_handler
from celerity.decorators.controller import controller
from celerity.decorators.http import get, post
from celerity.decorators.injectable import injectable
from celerity.decorators.invoke import invoke
from celerity.decorators.module import module
from celerity.decorators.schedule import schedule_handler
from celerity.testing import TestApp, mock_consumer_event, mock_schedule_event
from celerity.types.consumer import EventResult
from celerity.types.http import HttpResponse

# ---------------------------------------------------------------------------
# Sample app fixtures
# ---------------------------------------------------------------------------


@injectable()
class OrderService:
    def get_order(self, order_id: str) -> dict[str, str]:
        return {"id": order_id, "name": "Real Order"}


class MockOrderService:
    def get_order(self, order_id: str) -> dict[str, str]:
        return {"id": order_id, "name": "Mock Order"}


@controller("/orders")
class OrderController:
    def __init__(self, svc: OrderService) -> None:
        self.svc = svc

    @get("/")
    async def list_orders(self) -> HttpResponse:
        return HttpResponse(status=200, body='{"orders": []}')

    @get("/{order_id}")
    async def get_order(self) -> HttpResponse:
        return HttpResponse(status=200, body='{"id": "123"}')

    @post("/")
    async def create_order(self) -> HttpResponse:
        return HttpResponse(status=201, body='{"created": true}')

    @schedule_handler("rate(1 day)")
    async def cleanup(self) -> EventResult:
        return EventResult(success=True)

    @invoke("processOrder")
    async def process(self) -> dict[str, bool]:
        return {"processed": True}


@consumer("orders-queue")
class OrderConsumer:
    @message_handler()
    async def handle(self) -> EventResult:
        return EventResult(success=True)


@module(
    controllers=[OrderController, OrderConsumer],
    providers=[OrderService],
)
class AppModule:
    pass


# ---------------------------------------------------------------------------
# TestApp tests
# ---------------------------------------------------------------------------


class TestTestAppCreate:
    async def test_create_bootstraps(self) -> None:
        app = await TestApp.create(AppModule)
        assert app.registry is not None
        assert app.container is not None
        assert len(app.registry.get_all_handlers()) > 0
        await app.close()

    async def test_create_with_overrides(self) -> None:
        mock_svc = MockOrderService()
        app = await TestApp.create(AppModule, overrides={OrderService: mock_svc})
        resolved = await app.container.resolve(OrderService)
        assert resolved is mock_svc
        await app.close()


class TestTestAppHttp:
    async def test_http_get(self) -> None:
        app = await TestApp.create(AppModule)
        response = await app.http_get("/orders")
        assert response.status == 200
        assert response.body is not None
        assert "orders" in response.body
        await app.close()

    async def test_http_get_with_path_param(self) -> None:
        app = await TestApp.create(AppModule)
        response = await app.http_get("/orders/abc-123")
        assert response.status == 200
        await app.close()

    async def test_http_post(self) -> None:
        app = await TestApp.create(AppModule)
        response = await app.http_post("/orders", body={"name": "Widget"})
        assert response.status == 201
        await app.close()

    async def test_http_not_found(self) -> None:
        app = await TestApp.create(AppModule)
        response = await app.http_get("/nonexistent")
        assert response.status == 404
        await app.close()

    async def test_http_with_auth(self) -> None:
        app = await TestApp.create(AppModule)
        response = await app.http_get("/orders", auth={"sub": "user-1"})
        assert response.status == 200
        await app.close()


class TestTestAppConsumer:
    async def test_inject_consumer(self) -> None:
        app = await TestApp.create(AppModule)
        event = mock_consumer_event(
            "orders-queue",
            [
                {"message_id": "1", "body": '{"orderId": "123"}', "source": "q"},
            ],
        )
        result = await app.inject_consumer("orders-queue", event)
        assert result.success is True
        await app.close()

    async def test_inject_consumer_not_found(self) -> None:
        app = await TestApp.create(AppModule)
        event = mock_consumer_event("unknown::tag", [])
        with pytest.raises(ValueError, match="No consumer handler"):
            await app.inject_consumer("unknown::tag", event)
        await app.close()


class TestTestAppSchedule:
    async def test_inject_schedule(self) -> None:
        app = await TestApp.create(AppModule)
        event = mock_schedule_event("rate(1 day)")
        result = await app.inject_schedule("rate(1 day)", event)
        assert result.success is True
        await app.close()

    async def test_inject_schedule_not_found(self) -> None:
        app = await TestApp.create(AppModule)
        event = mock_schedule_event("unknown")
        with pytest.raises(ValueError, match="No schedule handler"):
            await app.inject_schedule("unknown", event)
        await app.close()


class TestTestAppCustom:
    async def test_inject_custom(self) -> None:
        app = await TestApp.create(AppModule)
        result = await app.inject_custom("processOrder")
        assert result == {"processed": True}
        await app.close()

    async def test_inject_custom_not_found(self) -> None:
        app = await TestApp.create(AppModule)
        with pytest.raises(ValueError, match="No custom handler"):
            await app.inject_custom("nonexistent")
        await app.close()


class TestTestAppLifecycle:
    async def test_close(self) -> None:
        app = await TestApp.create(AppModule)
        await app.close()

    async def test_get_container(self) -> None:
        app = await TestApp.create(AppModule)
        assert app.get_container() is app.container
        await app.close()

    async def test_get_registry(self) -> None:
        app = await TestApp.create(AppModule)
        assert app.get_registry() is app.registry
        await app.close()
