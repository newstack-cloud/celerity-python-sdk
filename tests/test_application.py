"""Tests for CelerityApplication and CelerityFactory."""

from __future__ import annotations

from celerity.application import CelerityApplication, CelerityFactory
from celerity.decorators.controller import controller
from celerity.decorators.http import get
from celerity.decorators.injectable import injectable
from celerity.decorators.module import module
from celerity.types.http import HttpResponse


@injectable()
class GreetingService:
    def greet(self) -> str:
        return "hello"


@controller("/")
class HomeController:
    def __init__(self, svc: GreetingService) -> None:
        self.svc = svc

    @get("/")
    async def index(self) -> HttpResponse:
        return HttpResponse(status=200, body=self.svc.greet())


@module(controllers=[HomeController], providers=[GreetingService])
class TestModule:
    pass


class TestCelerityFactory:
    async def test_create_returns_application(self) -> None:
        app = await CelerityFactory.create(TestModule)
        assert isinstance(app, CelerityApplication)
        assert len(app.registry.get_all_handlers()) > 0
        await app.close()

    async def test_create_with_custom_layers(self) -> None:
        """Custom layers are appended after system layers."""

        class StubLayer:
            async def handle(self, ctx: object, next_handler: object) -> object:
                return await next_handler()  # type: ignore[operator]

        app = await CelerityFactory.create(TestModule, layers=[StubLayer()])
        assert any(isinstance(layer, StubLayer) for layer in app.system_layers)
        await app.close()


class TestCelerityApplication:
    async def test_get_container(self) -> None:
        app = await CelerityFactory.create(TestModule)
        assert app.get_container() is app.container
        await app.close()

    async def test_get_registry(self) -> None:
        app = await CelerityFactory.create(TestModule)
        assert app.get_registry() is app.registry
        await app.close()

    async def test_close_completes_without_error(self) -> None:
        """close() disposes layers and closes the container without raising."""
        app = await CelerityFactory.create(TestModule)
        # Resolve a service to ensure something is in the container.
        await app.container.resolve(GreetingService)
        await app.close()
        # close_all should complete without errors.

    async def test_di_resolves_through_factory(self) -> None:
        """DI container resolves dependencies correctly after factory bootstrap."""
        app = await CelerityFactory.create(TestModule)
        ctrl = await app.container.resolve(HomeController)
        assert isinstance(ctrl.svc, GreetingService)
        assert ctrl.svc.greet() == "hello"
        await app.close()
