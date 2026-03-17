"""Tests for celerity.di.container."""

import pytest

from celerity.decorators.injectable import inject, injectable
from celerity.di.container import Container
from celerity.types.container import ClassProvider, FactoryProvider

# Module-level classes for circular dependency testing.
# Forward references only resolve at module scope.


@injectable()
class _CircularA:
    def __init__(self, b: "_CircularB") -> None:
        self.b = b


@injectable()
class _CircularB:
    def __init__(self, a: _CircularA) -> None:
        self.a = a


@injectable()
class _CycleX:
    def __init__(self, y: "_CycleY") -> None:
        pass


@injectable()
class _CycleY:
    def __init__(self, z: "_CycleZ") -> None:
        pass


@injectable()
class _CycleZ:
    def __init__(self, x: _CycleX) -> None:
        pass


class TestBasicResolution:
    async def test_resolve_class_provider(self) -> None:
        @injectable()
        class MyService:
            pass

        container = Container()
        container.register(MyService, ClassProvider(use_class=MyService))
        result = await container.resolve(MyService)
        assert isinstance(result, MyService)

    async def test_resolve_value_provider(self) -> None:
        container = Container()
        container.register_value("API_KEY", "sk-123")
        result = await container.resolve("API_KEY")
        assert result == "sk-123"

    async def test_resolve_factory_provider(self) -> None:
        container = Container()
        container.register(
            "greeting",
            FactoryProvider(use_factory=lambda: "hello"),
        )
        result = await container.resolve("greeting")
        assert result == "hello"

    async def test_resolve_async_factory(self) -> None:
        async def make_value() -> str:
            return "async-value"

        container = Container()
        container.register(
            "value",
            FactoryProvider(use_factory=make_value),
        )
        result = await container.resolve("value")
        assert result == "async-value"

    async def test_singleton_caching(self) -> None:
        @injectable()
        class MyService:
            pass

        container = Container()
        container.register_class(MyService)
        first = await container.resolve(MyService)
        second = await container.resolve(MyService)
        assert first is second

    async def test_has(self) -> None:
        container = Container()
        assert container.has("missing") is False
        container.register_value("key", "value")
        assert container.has("key") is True

    async def test_has_with_provider(self) -> None:
        @injectable()
        class Svc:
            pass

        container = Container()
        container.register_class(Svc)
        assert container.has(Svc) is True


class TestConstructorInjection:
    async def test_resolves_type_hint_deps(self) -> None:
        @injectable()
        class Database:
            pass

        @injectable()
        class UserService:
            def __init__(self, db: Database) -> None:
                self.db = db

        container = Container()
        container.register_class(Database)
        container.register_class(UserService)
        svc = await container.resolve(UserService)
        assert isinstance(svc.db, Database)

    async def test_no_params_without_injectable(self) -> None:
        class SimpleService:
            pass

        container = Container()
        result = await container.resolve(SimpleService)
        assert isinstance(result, SimpleService)

    async def test_params_without_injectable_raises(self) -> None:
        class BadService:
            def __init__(self, dep: str) -> None:
                self.dep = dep

        container = Container()
        with pytest.raises(RuntimeError, match="@injectable"):
            await container.resolve(BadService)

    async def test_factory_with_injected_deps(self) -> None:
        @injectable()
        class Config:
            pass

        container = Container()
        container.register_class(Config)
        container.register(
            "service",
            FactoryProvider(
                use_factory=lambda cfg: f"built-with-{type(cfg).__name__}",
                inject=[Config],
            ),
        )
        result = await container.resolve("service")
        assert result == "built-with-Config"


class TestCircularDetection:
    async def test_two_step_class_cycle(self) -> None:
        container = Container()
        container.register_class(_CircularA)
        container.register_class(_CircularB)
        with pytest.raises(RuntimeError, match="Circular dependency"):
            await container.resolve(_CircularA)

    async def test_three_step_class_cycle(self) -> None:
        container = Container()
        container.register_class(_CycleX)
        container.register_class(_CycleY)
        container.register_class(_CycleZ)
        with pytest.raises(RuntimeError, match="Circular dependency"):
            await container.resolve(_CycleX)

    async def test_two_step_factory_cycle(self) -> None:
        container = Container()
        container.register(
            "A",
            FactoryProvider(use_factory=lambda b: f"A({b})", inject=["B"]),
        )
        container.register(
            "B",
            FactoryProvider(use_factory=lambda a: f"B({a})", inject=["A"]),
        )
        with pytest.raises(RuntimeError, match="Circular dependency"):
            await container.resolve("A")


class TestLifecycle:
    async def test_auto_tracks_close(self) -> None:
        closed = False

        @injectable()
        class Resource:
            def close(self) -> None:
                nonlocal closed
                closed = True

        container = Container()
        container.register_class(Resource)
        await container.resolve(Resource)
        await container.close_all()
        assert closed is True

    async def test_auto_tracks_aclose(self) -> None:
        closed = False

        @injectable()
        class AsyncResource:
            async def aclose(self) -> None:
                nonlocal closed
                closed = True

        container = Container()
        container.register_class(AsyncResource)
        await container.resolve(AsyncResource)
        await container.close_all()
        assert closed is True

    async def test_close_reverse_order(self) -> None:
        order: list[str] = []

        class First:
            def close(self) -> None:
                order.append("first")

        class Second:
            def close(self) -> None:
                order.append("second")

        container = Container()
        container.register_value("first", First())
        container.register_value("second", Second())
        await container.close_all()
        assert order == ["second", "first"]

    async def test_close_all_survives_exception(self) -> None:
        closed_after = False

        class BadResource:
            def close(self) -> None:
                raise RuntimeError("close failed")

        class GoodResource:
            def close(self) -> None:
                nonlocal closed_after
                closed_after = True

        container = Container()
        container.register_value("good", GoodResource())
        container.register_value("bad", BadResource())
        await container.close_all()
        assert closed_after is True

    async def test_on_close_callback(self) -> None:
        closed_value = None

        def on_close(val: str) -> None:
            nonlocal closed_value
            closed_value = val

        container = Container()
        container.register(
            "conn",
            FactoryProvider(
                use_factory=lambda: "my-connection",
                on_close=on_close,
            ),
        )
        await container.resolve("conn")
        await container.close_all()
        assert closed_value == "my-connection"


class TestValidateDependencies:
    def test_valid_graph(self) -> None:
        @injectable()
        class A:
            pass

        @injectable()
        class B:
            def __init__(self, a: A) -> None:
                pass

        container = Container()
        container.register_class(A)
        container.register_class(B)
        container.validate_dependencies()

    def test_missing_provider_batch_report(self) -> None:
        container = Container()
        container.register(
            "svc",
            FactoryProvider(use_factory=lambda x: x, inject=["missing_token"]),
        )
        with pytest.raises(RuntimeError, match="Unresolvable dependencies") as exc_info:
            container.validate_dependencies()
        msg = str(exc_info.value)
        assert "svc requires missing_token" in msg
        assert "no provider registered" in msg

    def test_missing_provider_reports_all(self) -> None:
        container = Container()
        container.register(
            "a",
            FactoryProvider(use_factory=lambda x: x, inject=["dep1"]),
        )
        container.register(
            "b",
            FactoryProvider(use_factory=lambda x: x, inject=["dep2"]),
        )
        with pytest.raises(RuntimeError, match="Unresolvable dependencies") as exc_info:
            container.validate_dependencies()
        msg = str(exc_info.value)
        assert "a requires dep1" in msg
        assert "b requires dep2" in msg


class TestNoProvider:
    async def test_unregistered_string_token_raises(self) -> None:
        container = Container()
        with pytest.raises(RuntimeError, match="No provider registered"):
            await container.resolve("nonexistent")

    async def test_missing_dep_shows_slot_context(self) -> None:
        from typing import Annotated

        @injectable()
        class NeedsDb:
            def __init__(self, db: Annotated[object, inject("DB_CONNECTION")]) -> None:
                self.db = db

        container = Container()
        container.register_class(NeedsDb)
        with pytest.raises(RuntimeError, match="parameter 'db'") as exc_info:
            await container.resolve(NeedsDb)
        msg = str(exc_info.value)
        assert "NeedsDb" in msg
        assert "position 0" in msg
        assert "DB_CONNECTION" in msg
