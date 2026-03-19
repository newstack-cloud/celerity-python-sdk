"""Tests for the handler manifest extraction CLI."""

from __future__ import annotations

from typing import Any

from celerity.cli.identity import (
    derive_class_handler_function,
    derive_class_handler_name,
    derive_class_resource_name,
    derive_code_location,
    derive_function_handler_function,
    derive_function_resource_name,
)
from celerity.cli.metadata_app import build_scanned_module
from celerity.cli.serializer import serialize_manifest
from celerity.decorators.consumer import consumer, message_handler
from celerity.decorators.controller import controller
from celerity.decorators.guards import guard, protected_by, public
from celerity.decorators.http import get, post
from celerity.decorators.injectable import injectable
from celerity.decorators.invoke import invoke
from celerity.decorators.metadata import set_handler_metadata
from celerity.decorators.module import module
from celerity.decorators.schedule import schedule_handler
from celerity.decorators.websocket import on_connect, on_message, ws_controller
from celerity.types.consumer import EventResult
from celerity.types.guard import GuardInput, GuardResult
from celerity.types.http import HttpResponse

# ---------------------------------------------------------------------------
# Sample app fixtures
# ---------------------------------------------------------------------------


@injectable()
class OrderService:
    pass


@guard("admin")
class AdminGuard:
    async def validate(self, input: GuardInput) -> GuardResult:
        return GuardResult.allow()


@controller("/orders")
class OrderController:
    def __init__(self, svc: OrderService) -> None:
        self.svc = svc

    @get("/")
    async def list_orders(self) -> HttpResponse:
        return HttpResponse(status=200)

    @get("/{order_id}")
    async def get_order(self) -> HttpResponse:
        return HttpResponse(status=200)

    @post("/")
    @protected_by("admin")
    async def create_order(self) -> HttpResponse:
        return HttpResponse(status=201)

    @post("/public")
    @public()
    async def public_endpoint(self) -> HttpResponse:
        return HttpResponse(status=200)

    @schedule_handler("rate(1 day)")
    async def cleanup(self) -> EventResult:
        return EventResult(success=True)

    @invoke("processOrder")
    async def process(self) -> dict[str, bool]:
        return {"processed": True}

    @get("/meta")
    @set_handler_metadata("version", "v2")
    async def with_meta(self) -> HttpResponse:
        return HttpResponse(status=200)


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

    @message_handler("order.created")
    async def on_created(self) -> EventResult:
        return EventResult(success=True)


@module(
    controllers=[OrderController, ChatController, OrderConsumer],
    providers=[OrderService],
    guards=[AdminGuard],
)
class AppModule:
    pass


# ---------------------------------------------------------------------------
# Identity derivation tests
# ---------------------------------------------------------------------------


class TestIdentity:
    def test_class_resource_name(self) -> None:
        result = derive_class_resource_name("OrdersController", "get_order")
        assert result == "ordersController_get_order"

    def test_class_handler_name(self) -> None:
        result = derive_class_handler_name("OrdersController", "get_order")
        assert result == "OrdersController-get_order"

    def test_class_handler_function(self) -> None:
        result = derive_class_handler_function(
            "src/handlers/orders.py",
            "OrdersController",
            "get_order",
        )
        assert result == "orders.OrdersController.get_order"

    def test_function_resource_name(self) -> None:
        assert derive_function_resource_name("getOrder") == "getOrder"

    def test_function_handler_function(self) -> None:
        result = derive_function_handler_function("src/handlers/orders.py", "getOrder")
        assert result == "orders.getOrder"

    def test_code_location_relative(self) -> None:
        result = derive_code_location("src/handlers/orders.py", "")
        assert result == "./src/handlers"

    def test_code_location_same_dir(self) -> None:
        result = derive_code_location("app.py", "")
        assert result == "./"


# ---------------------------------------------------------------------------
# Metadata scanning tests
# ---------------------------------------------------------------------------


class TestMetadataScanning:
    def test_scans_http_handlers(self) -> None:
        scanned = build_scanned_module(AppModule)
        http_methods = []
        for h in scanned.class_handlers:
            for m in h.methods:
                if m.handler_type == "http":
                    http_methods.append(m.method_name)
        assert "list_orders" in http_methods
        assert "get_order" in http_methods
        assert "create_order" in http_methods

    def test_scans_websocket_handlers(self) -> None:
        scanned = build_scanned_module(AppModule)
        ws_methods = []
        for h in scanned.class_handlers:
            for m in h.methods:
                if m.handler_type == "websocket":
                    ws_methods.append(m.method_name)
        assert "connect" in ws_methods
        assert "handle_message" in ws_methods

    def test_scans_consumer_handlers(self) -> None:
        scanned = build_scanned_module(AppModule)
        consumer_methods = []
        for h in scanned.class_handlers:
            for m in h.methods:
                if m.handler_type == "consumer":
                    consumer_methods.append(m.method_name)
        assert "handle" in consumer_methods
        assert "on_created" in consumer_methods

    def test_scans_schedule_handlers(self) -> None:
        scanned = build_scanned_module(AppModule)
        schedule_methods = []
        for h in scanned.class_handlers:
            for m in h.methods:
                if m.handler_type == "schedule":
                    schedule_methods.append(m.method_name)
        assert "cleanup" in schedule_methods

    def test_scans_custom_handlers(self) -> None:
        scanned = build_scanned_module(AppModule)
        custom_methods = []
        for h in scanned.class_handlers:
            for m in h.methods:
                if m.handler_type == "custom":
                    custom_methods.append(m.method_name)
        assert "process" in custom_methods

    def test_scans_guards(self) -> None:
        scanned = build_scanned_module(AppModule)
        guard_names = [g.guard_name for g in scanned.guard_handlers]
        assert "admin" in guard_names

    def test_no_instantiation_required(self) -> None:
        """Scanning works without DI -- no instances are constructed."""
        scanned = build_scanned_module(AppModule)
        assert len(scanned.class_handlers) > 0


# ---------------------------------------------------------------------------
# Serialization tests
# ---------------------------------------------------------------------------


class TestSerialization:
    def _manifest_dict(self) -> dict[str, Any]:
        scanned = build_scanned_module(AppModule)
        manifest = serialize_manifest(scanned, "src/app.py", project_root="/project")
        return manifest.to_dict()

    def test_manifest_version(self) -> None:
        d = self._manifest_dict()
        assert d["version"] == "1.0.0"

    def test_http_annotations(self) -> None:
        d = self._manifest_dict()
        list_handler = _find_handler(d["handlers"], "list_orders")
        assert list_handler is not None
        ann = list_handler["annotations"]
        assert ann["celerity.handler.http"] is True
        assert ann["celerity.handler.http.method"] == "GET"
        assert ann["celerity.handler.http.path"] == "/orders"

    def test_http_path_with_param(self) -> None:
        d = self._manifest_dict()
        get_handler = _find_handler(d["handlers"], "get_order")
        assert get_handler is not None
        assert get_handler["annotations"]["celerity.handler.http.path"] == "/orders/{order_id}"

    def test_guard_protected_by(self) -> None:
        d = self._manifest_dict()
        create_handler = _find_handler(d["handlers"], "create_order")
        assert create_handler is not None
        ann = create_handler["annotations"]
        assert "celerity.handler.guard.protectedBy" in ann
        assert "admin" in ann["celerity.handler.guard.protectedBy"]

    def test_public_annotation(self) -> None:
        d = self._manifest_dict()
        public_handler = _find_handler(d["handlers"], "public_endpoint")
        assert public_handler is not None
        ann = public_handler["annotations"]
        assert ann.get("celerity.handler.public") is True

    def test_custom_metadata_annotation(self) -> None:
        d = self._manifest_dict()
        meta_handler = _find_handler(d["handlers"], "with_meta")
        assert meta_handler is not None
        ann = meta_handler["annotations"]
        assert ann.get("celerity.handler.metadata.version") == "v2"

    def test_schedule_annotations(self) -> None:
        d = self._manifest_dict()
        sched = _find_handler(d["handlers"], "cleanup", handler_type="schedule")
        assert sched is not None
        ann = sched["annotations"]
        assert ann["celerity.handler.schedule"] is True
        assert ann["celerity.handler.schedule.expression"] == "rate(1 day)"

    def test_custom_handler_annotations(self) -> None:
        d = self._manifest_dict()
        custom = _find_handler(d["handlers"], "process", handler_type="custom")
        assert custom is not None
        ann = custom["annotations"]
        assert ann["celerity.handler.custom"] is True
        assert ann["celerity.handler.custom.name"] == "processOrder"

    def test_websocket_annotations(self) -> None:
        d = self._manifest_dict()
        ws = _find_handler(d["handlers"], "handle_message")
        assert ws is not None
        ann = ws["annotations"]
        assert ann["celerity.handler.websocket"] is True
        assert ann["celerity.handler.websocket.route"] == "chat"

    def test_consumer_annotations(self) -> None:
        d = self._manifest_dict()
        cons = _find_handler(d["handlers"], "handle", handler_type="consumer")
        assert cons is not None
        ann = cons["annotations"]
        assert ann["celerity.handler.consumer"] is True
        assert ann["celerity.handler.consumer.source"] == "orders-queue"

    def test_consumer_with_route(self) -> None:
        d = self._manifest_dict()
        cons = _find_handler(d["handlers"], "on_created")
        assert cons is not None
        ann = cons["annotations"]
        assert ann.get("celerity.handler.consumer.route") == "order.created"

    def test_guard_handler_entry(self) -> None:
        d = self._manifest_dict()
        assert len(d["guardHandlers"]) >= 1
        guard_entry = d["guardHandlers"][0]
        assert guard_entry["guardName"] == "admin"
        assert guard_entry["guardType"] == "class"
        assert guard_entry["annotations"]["celerity.handler.guard.custom"] == "admin"

    def test_handler_spec(self) -> None:
        d = self._manifest_dict()
        handler = _find_handler(d["handlers"], "list_orders")
        assert handler is not None
        spec = handler["spec"]
        assert spec["handlerName"] == "OrderController-list_orders"
        assert spec["codeLocation"] == "./src"
        assert spec["handler"] == "app.OrderController.list_orders"

    def test_handler_type_field(self) -> None:
        d = self._manifest_dict()
        http = _find_handler(d["handlers"], "list_orders")
        assert http is not None
        assert http["handlerType"] == "http"

        ws = _find_handler(d["handlers"], "connect")
        assert ws is not None
        assert ws["handlerType"] == "websocket"


# ---------------------------------------------------------------------------
# Nested module tests
# ---------------------------------------------------------------------------


@controller("/nested")
class NestedController:
    @get("/")
    async def index(self) -> HttpResponse:
        return HttpResponse(status=200)


@module(controllers=[NestedController])
class ChildModule:
    pass


@module(imports=[ChildModule])
class ParentModule:
    pass


class TestDependencyGraph:
    def test_dependency_graph_present(self) -> None:
        d = self._manifest_dict()
        assert "dependencyGraph" in d
        assert "nodes" in d["dependencyGraph"]

    def test_providers_scanned(self) -> None:
        d = self._manifest_dict()
        nodes = d["dependencyGraph"]["nodes"]
        token_names = [n["token"] for n in nodes]
        assert "OrderService" in token_names
        assert "OrderController" in token_names
        assert "AdminGuard" in token_names

    def test_node_structure(self) -> None:
        d = self._manifest_dict()
        nodes = d["dependencyGraph"]["nodes"]
        order_svc = next(n for n in nodes if n["token"] == "OrderService")
        assert order_svc["tokenType"] == "class"
        assert order_svc["providerType"] == "class"
        assert isinstance(order_svc["dependencies"], list)

    def test_dependencies_resolved(self) -> None:
        d = self._manifest_dict()
        nodes = d["dependencyGraph"]["nodes"]
        order_ctrl = next(n for n in nodes if n["token"] == "OrderController")
        assert "OrderService" in order_ctrl["dependencies"]

    def _manifest_dict(self) -> dict[str, Any]:
        scanned = build_scanned_module(AppModule)
        manifest = serialize_manifest(scanned, "src/app.py", project_root="/project")
        return manifest.to_dict()


class TestNestedModules:
    def test_nested_module_handlers_discovered(self) -> None:
        scanned = build_scanned_module(ParentModule)
        methods = []
        for h in scanned.class_handlers:
            for m in h.methods:
                methods.append(m.method_name)
        assert "index" in methods


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_handler(
    handlers: list[dict[str, Any]],
    method_name: str,
    handler_type: str | None = None,
) -> dict[str, Any] | None:
    for handler in handlers:
        if handler.get("methodName") == method_name and (
            handler_type is None or handler.get("handlerType") == handler_type
        ):
            return handler
    return None
