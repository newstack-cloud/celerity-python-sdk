"""Tests for celerity.decorators."""

from celerity.decorators.consumer import consumer, message_handler
from celerity.decorators.consumer_params import ConsumerEvent, Messages
from celerity.decorators.controller import controller
from celerity.decorators.guards import guard, protected_by, public
from celerity.decorators.http import delete, get, head, options, patch, post, put
from celerity.decorators.injectable import _InjectMarker, inject, injectable
from celerity.decorators.invoke import invoke
from celerity.decorators.invoke_params import InvokeContext, Payload
from celerity.decorators.layer import use_layer
from celerity.decorators.metadata import action, set_handler_metadata
from celerity.decorators.module import module
from celerity.decorators.params import (
    Auth,
    Body,
    Cookies,
    Headers,
    Param,
    Query,
    Req,
    RequestId,
    Token,
)
from celerity.decorators.resource import use_resource
from celerity.decorators.schedule import schedule_handler
from celerity.decorators.schedule_params import ScheduleExpression, ScheduleId, ScheduleInput
from celerity.decorators.websocket import on_connect, on_disconnect, on_message, ws_controller
from celerity.decorators.websocket_params import ConnectionId, EventType, MessageBody, MessageId
from celerity.metadata.keys import (
    CONSUMER,
    CONSUMER_HANDLER,
    CONTROLLER,
    CUSTOM_METADATA,
    GUARD_CUSTOM,
    GUARD_PROTECTEDBY,
    HTTP_METHOD,
    INJECT,
    INJECTABLE,
    INVOKE,
    LAYER,
    MODULE,
    PUBLIC,
    ROUTE_PATH,
    SCHEDULE_HANDLER,
    USE_RESOURCE,
    WEBSOCKET_CONTROLLER,
    WEBSOCKET_EVENT,
    get_metadata,
)
from celerity.types.module import ModuleMetadata


class TestController:
    def test_with_prefix(self) -> None:
        @controller("/orders")
        class Ctrl:
            pass

        assert get_metadata(Ctrl, CONTROLLER) == {"prefix": "/orders"}
        assert get_metadata(Ctrl, INJECTABLE) is True

    def test_without_prefix(self) -> None:
        @controller()
        class Ctrl:
            pass

        assert get_metadata(Ctrl, CONTROLLER) == {}

    def test_sets_injectable(self) -> None:
        @controller("/api")
        class Ctrl:
            pass

        assert get_metadata(Ctrl, INJECTABLE) is True


class TestHttpMethods:
    def test_get(self) -> None:
        @get("/{id}")
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[HTTP_METHOD] == "GET"
        assert meta[ROUTE_PATH] == "/{id}"

    def test_post_default_path(self) -> None:
        @post()
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[HTTP_METHOD] == "POST"
        assert meta[ROUTE_PATH] == "/"

    def test_all_methods(self) -> None:
        methods = [
            (get, "GET"),
            (post, "POST"),
            (put, "PUT"),
            (patch, "PATCH"),
            (delete, "DELETE"),
            (head, "HEAD"),
            (options, "OPTIONS"),
        ]
        for decorator, expected_method in methods:

            @decorator("/test")
            def handler() -> None:
                pass

            meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
            assert meta[HTTP_METHOD] == expected_method

    def test_preserves_function_identity(self) -> None:
        @get("/")
        def my_handler() -> None:
            """My docstring."""

        assert my_handler.__name__ == "my_handler"
        assert my_handler.__doc__ == "My docstring."


class TestGuards:
    def test_guard_decorator(self) -> None:
        @guard("admin")
        class AdminGuard:
            pass

        assert get_metadata(AdminGuard, GUARD_CUSTOM) == "admin"
        assert get_metadata(AdminGuard, INJECTABLE) is True

    def test_protected_by_on_class(self) -> None:
        @protected_by("admin")
        class Ctrl:
            pass

        assert get_metadata(Ctrl, GUARD_PROTECTEDBY) == ["admin"]

    def test_protected_by_stacking(self) -> None:
        @protected_by("admin")
        @protected_by("rate_limit")
        class Ctrl:
            pass

        assert get_metadata(Ctrl, GUARD_PROTECTEDBY) == ["admin", "rate_limit"]

    def test_protected_by_on_method(self) -> None:
        @protected_by("auth")
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__
        assert meta[GUARD_PROTECTEDBY] == ["auth"]

    def test_public(self) -> None:
        @public()
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[PUBLIC] is True


class TestModule:
    def test_with_controllers_and_providers(self) -> None:
        class OrderService:
            pass

        @controller("/orders")
        class OrderCtrl:
            pass

        @module(controllers=[OrderCtrl], providers=[OrderService])
        class AppModule:
            pass

        metadata = get_metadata(AppModule, MODULE)
        assert isinstance(metadata, ModuleMetadata)
        assert metadata.controllers == [OrderCtrl]
        assert metadata.providers == [OrderService]

    def test_empty_module(self) -> None:
        @module()
        class AppModule:
            pass

        metadata = get_metadata(AppModule, MODULE)
        assert isinstance(metadata, ModuleMetadata)
        assert metadata.controllers is None
        assert metadata.providers is None


class TestInjectable:
    def test_injectable(self) -> None:
        @injectable()
        class Service:
            pass

        assert get_metadata(Service, INJECTABLE) is True

    def test_inject_class_decorator(self) -> None:
        @inject({0: "DB_TOKEN", 2: "CACHE_TOKEN"})
        class Service:
            pass

        overrides = get_metadata(Service, INJECT)
        assert overrides == {0: "DB_TOKEN", 2: "CACHE_TOKEN"}

    def test_inject_marker(self) -> None:
        result = inject("DB_TOKEN")
        assert isinstance(result, _InjectMarker)
        assert result.token == "DB_TOKEN"


class TestWebSocket:
    def test_ws_controller(self) -> None:
        @ws_controller()
        class ChatHandler:
            pass

        assert get_metadata(ChatHandler, WEBSOCKET_CONTROLLER) is True
        assert get_metadata(ChatHandler, INJECTABLE) is True

    def test_on_connect(self) -> None:
        @on_connect()
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[WEBSOCKET_EVENT] == {
            "route": "$connect",
            "event_type": "connect",
        }

    def test_on_message(self) -> None:
        @on_message("chat")
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[WEBSOCKET_EVENT] == {
            "route": "chat",
            "event_type": "message",
        }

    def test_on_disconnect(self) -> None:
        @on_disconnect()
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[WEBSOCKET_EVENT] == {
            "route": "$disconnect",
            "event_type": "disconnect",
        }


class TestConsumer:
    def test_consumer_decorator(self) -> None:
        @consumer("orders-queue")
        class OrderConsumer:
            pass

        assert get_metadata(OrderConsumer, CONSUMER) == {"source": "orders-queue"}
        assert get_metadata(OrderConsumer, INJECTABLE) is True

    def test_message_handler(self) -> None:
        @message_handler()
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[CONSUMER_HANDLER] == {}

    def test_message_handler_with_route(self) -> None:
        @message_handler(route="priority")
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[CONSUMER_HANDLER] == {"route": "priority"}


class TestSchedule:
    def test_no_args(self) -> None:
        @schedule_handler()
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[SCHEDULE_HANDLER] == {}

    def test_rate_expression(self) -> None:
        @schedule_handler("rate(1 day)")
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[SCHEDULE_HANDLER] == {"schedule": "rate(1 day)"}

    def test_cron_expression(self) -> None:
        @schedule_handler("cron(0 9 ? * MON *)")
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[SCHEDULE_HANDLER] == {"schedule": "cron(0 9 ? * MON *)"}

    def test_source_name(self) -> None:
        @schedule_handler("daily-cleanup")
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[SCHEDULE_HANDLER] == {"source": "daily-cleanup"}

    def test_explicit_kwargs(self) -> None:
        @schedule_handler(source="weekly-report", schedule="cron(0 9 ? * MON *)")
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[SCHEDULE_HANDLER] == {
            "source": "weekly-report",
            "schedule": "cron(0 9 ? * MON *)",
        }


class TestInvoke:
    def test_invoke_decorator(self) -> None:
        @invoke("processOrder")
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__  # type: ignore[attr-defined]
        assert meta[INVOKE] == {"name": "processOrder"}


class TestLayer:
    def test_use_layer_on_class(self) -> None:
        class LoggingLayer:
            pass

        @use_layer(LoggingLayer)
        class Ctrl:
            pass

        assert get_metadata(Ctrl, LAYER) == [LoggingLayer]

    def test_use_layer_multiple(self) -> None:
        class A:
            pass

        class B:
            pass

        @use_layer(A, B)
        class Ctrl:
            pass

        assert get_metadata(Ctrl, LAYER) == [A, B]

    def test_use_layer_stacking(self) -> None:
        class A:
            pass

        class B:
            pass

        @use_layer(B)
        @use_layer(A)
        class Ctrl:
            pass

        assert get_metadata(Ctrl, LAYER) == [A, B]

    def test_use_layer_on_method(self) -> None:
        class RateLimitLayer:
            pass

        @use_layer(RateLimitLayer)
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__
        assert meta[LAYER] == [RateLimitLayer]


class TestMetadata:
    def test_set_handler_metadata_on_class(self) -> None:
        @set_handler_metadata("version", "v2")
        class Ctrl:
            pass

        assert get_metadata(Ctrl, CUSTOM_METADATA) == {"version": "v2"}

    def test_set_handler_metadata_on_method(self) -> None:
        @set_handler_metadata("version", "v2")
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__
        assert meta[CUSTOM_METADATA] == {"version": "v2"}

    def test_action(self) -> None:
        @action("create_order")
        class Ctrl:
            pass

        assert get_metadata(Ctrl, CUSTOM_METADATA) == {"action": "create_order"}


class TestResource:
    def test_use_resource_on_class(self) -> None:
        @use_resource("orders-db", "orders-cache")
        class Ctrl:
            pass

        assert get_metadata(Ctrl, USE_RESOURCE) == ["orders-db", "orders-cache"]

    def test_use_resource_on_method(self) -> None:
        @use_resource("analytics-db")
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__
        assert meta[USE_RESOURCE] == ["analytics-db"]


class TestParams:
    def test_body(self) -> None:
        t = Body[dict[str, str]]
        assert t.__celerity_param__.type == "body"
        assert t.__args__ == (dict[str, str],)  # type: ignore[attr-defined]

    def test_query(self) -> None:
        t = Query[str]
        assert t.__celerity_param__.type == "query"

    def test_param(self) -> None:
        t = Param[str]
        assert t.__celerity_param__.type == "param"

    def test_headers(self) -> None:
        t = Headers[dict[str, str]]
        assert t.__celerity_param__.type == "headers"

    def test_cookies(self) -> None:
        t = Cookies[dict[str, str]]
        assert t.__celerity_param__.type == "cookies"

    def test_auth(self) -> None:
        assert Auth.__celerity_param__.type == "auth"

    def test_token(self) -> None:
        assert Token.__celerity_param__.type == "token"

    def test_req(self) -> None:
        assert Req.__celerity_param__.type == "request"

    def test_request_id(self) -> None:
        assert RequestId.__celerity_param__.type == "requestId"

    def test_connection_id(self) -> None:
        assert ConnectionId.__celerity_param__.type == "connectionId"

    def test_message_body(self) -> None:
        t = MessageBody[dict[str, str]]
        assert t.__celerity_param__.type == "messageBody"

    def test_message_id(self) -> None:
        assert MessageId.__celerity_param__.type == "messageId"

    def test_event_type(self) -> None:
        assert EventType.__celerity_param__.type == "eventType"

    def test_messages(self) -> None:
        t = Messages[dict[str, str]]
        assert t.__celerity_param__.type == "messages"

    def test_consumer_event(self) -> None:
        assert ConsumerEvent.__celerity_param__.type == "consumerEvent"

    def test_schedule_input(self) -> None:
        t = ScheduleInput[dict[str, str]]
        assert t.__celerity_param__.type == "scheduleInput"

    def test_schedule_id(self) -> None:
        assert ScheduleId.__celerity_param__.type == "scheduleId"

    def test_schedule_expression(self) -> None:
        assert ScheduleExpression.__celerity_param__.type == "scheduleExpression"

    def test_payload(self) -> None:
        t = Payload[dict[str, str]]
        assert t.__celerity_param__.type == "payload"

    def test_invoke_context(self) -> None:
        assert InvokeContext.__celerity_param__.type == "invokeContext"


class TestDecoratorComposition:
    def test_multiple_decorators_on_method(self) -> None:
        class LoggingLayer:
            pass

        @get("/")
        @protected_by("admin")
        @use_layer(LoggingLayer)
        def handler() -> None:
            pass

        meta = handler.__celerity_metadata__
        assert meta[HTTP_METHOD] == "GET"
        assert meta[GUARD_PROTECTEDBY] == ["admin"]
        assert meta[LAYER] == [LoggingLayer]

    def test_preserves_function_identity(self) -> None:
        @get("/test")
        @protected_by("auth")
        def my_handler() -> None:
            """Handler docstring."""

        assert my_handler.__name__ == "my_handler"
        assert my_handler.__doc__ == "Handler docstring."


class TestTopLevelImports:
    def test_decorators_importable_from_celerity(self) -> None:
        import celerity

        assert hasattr(celerity, "controller")
        assert hasattr(celerity, "get")
        assert hasattr(celerity, "post")
        assert hasattr(celerity, "module")
        assert hasattr(celerity, "injectable")
        assert hasattr(celerity, "ws_controller")
        assert hasattr(celerity, "consumer")
        assert hasattr(celerity, "guard")
        assert hasattr(celerity, "protected_by")

    def test_param_types_importable_from_celerity(self) -> None:
        import celerity

        assert hasattr(celerity, "Body")
        assert hasattr(celerity, "Query")
        assert hasattr(celerity, "Param")
        assert hasattr(celerity, "Auth")
        assert hasattr(celerity, "Messages")
        assert hasattr(celerity, "Payload")
