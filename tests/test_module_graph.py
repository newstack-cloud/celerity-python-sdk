"""Tests for celerity.bootstrap.module_graph."""

import pytest

from celerity.bootstrap.module_graph import (
    build_module_graph,
    register_module_graph,
    walk_module_graph,
)
from celerity.decorators.controller import controller
from celerity.decorators.guards import guard
from celerity.decorators.injectable import injectable
from celerity.decorators.module import module
from celerity.di.container import Container


@controller("/orders")
class OrderController:
    pass


@injectable()
class OrderService:
    pass


@guard("admin")
class AdminGuard:
    pass


class TestBuildModuleGraph:
    def test_single_module(self) -> None:
        @module(controllers=[OrderController], providers=[OrderService])
        class AppModule:
            pass

        graph = build_module_graph(AppModule)
        assert AppModule in graph
        node = graph[AppModule]
        assert OrderController in node.controllers
        assert OrderService in node.providers

    def test_nested_imports(self) -> None:
        @module(providers=[OrderService])
        class InfraModule:
            pass

        @module(controllers=[OrderController], imports=[InfraModule])
        class FeatureModule:
            pass

        @module(imports=[FeatureModule])
        class RootModule:
            pass

        graph = build_module_graph(RootModule)
        assert RootModule in graph
        assert FeatureModule in graph
        assert InfraModule in graph

    def test_module_without_decorator(self) -> None:
        class PlainModule:
            pass

        graph = build_module_graph(PlainModule)
        assert PlainModule in graph
        node = graph[PlainModule]
        assert node.controllers == []
        assert node.providers == []

    def test_circular_import_raises(self) -> None:
        @module(imports=[])
        class ModuleA:
            pass

        @module(imports=[ModuleA])
        class ModuleB:
            pass

        # Manually patch A to import B (can't do at decoration time)
        from celerity.metadata.keys import MODULE, get_metadata

        meta_a = get_metadata(ModuleA, MODULE)
        meta_a.imports = [ModuleB]

        with pytest.raises(RuntimeError, match="Circular module import"):
            build_module_graph(ModuleA)

    def test_guards_collected(self) -> None:
        @module(guards=[AdminGuard])
        class AppModule:
            pass

        graph = build_module_graph(AppModule)
        assert AdminGuard in graph[AppModule].guards

    def test_layers_collected(self) -> None:
        class LoggingLayer:
            pass

        class MetricsLayer:
            pass

        @module(
            controllers=[OrderController],
            layers=[LoggingLayer, MetricsLayer],
        )
        class AppModule:
            pass

        graph = build_module_graph(AppModule)
        assert graph[AppModule].layers == [LoggingLayer, MetricsLayer]

    def test_layers_default_empty(self) -> None:
        @module(controllers=[OrderController])
        class AppModule:
            pass

        graph = build_module_graph(AppModule)
        assert graph[AppModule].layers == []

    def test_shared_import_visited_once(self) -> None:
        @module(providers=[OrderService])
        class SharedModule:
            pass

        @module(imports=[SharedModule])
        class FeatureA:
            pass

        @module(imports=[SharedModule])
        class FeatureB:
            pass

        @module(imports=[FeatureA, FeatureB])
        class Root:
            pass

        graph = build_module_graph(Root)
        assert len([k for k in graph if k is SharedModule]) == 1


class TestRegisterModuleGraph:
    def test_registers_class_providers(self) -> None:
        @module(providers=[OrderService])
        class AppModule:
            pass

        graph = build_module_graph(AppModule)
        container = Container()
        register_module_graph(graph, container)
        assert container.has(OrderService)

    def test_registers_controllers(self) -> None:
        @module(controllers=[OrderController])
        class AppModule:
            pass

        graph = build_module_graph(AppModule)
        container = Container()
        register_module_graph(graph, container)
        assert container.has(OrderController)

    def test_registers_guards(self) -> None:
        @module(guards=[AdminGuard])
        class AppModule:
            pass

        graph = build_module_graph(AppModule)
        container = Container()
        register_module_graph(graph, container)
        assert container.has(AdminGuard)

    def test_does_not_duplicate_registration(self) -> None:
        @module(controllers=[OrderController])
        class AppModule:
            pass

        graph = build_module_graph(AppModule)
        container = Container()
        register_module_graph(graph, container)
        register_module_graph(graph, container)
        assert container.has(OrderController)


class TestWalkModuleGraph:
    def test_builds_and_registers(self) -> None:
        @module(
            controllers=[OrderController],
            providers=[OrderService],
            guards=[AdminGuard],
        )
        class AppModule:
            pass

        container = Container()
        graph = walk_module_graph(AppModule, container)
        assert AppModule in graph
        assert container.has(OrderController)
        assert container.has(OrderService)
        assert container.has(AdminGuard)

    async def test_end_to_end_resolution(self) -> None:
        @injectable()
        class Database:
            pass

        @injectable()
        class UserService:
            def __init__(self, db: Database) -> None:
                self.db = db

        @controller("/users")
        class UserController:
            def __init__(self, svc: UserService) -> None:
                self.svc = svc

        @module(
            controllers=[UserController],
            providers=[Database, UserService],
        )
        class AppModule:
            pass

        container = Container()
        walk_module_graph(AppModule, container)

        ctrl = await container.resolve(UserController)
        assert isinstance(ctrl.svc, UserService)
        assert isinstance(ctrl.svc.db, Database)
