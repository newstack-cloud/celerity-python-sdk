"""Module decorator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from celerity.metadata.keys import MODULE, set_metadata
from celerity.types.module import ModuleMetadata

if TYPE_CHECKING:
    from collections.abc import Callable

    from celerity.types.common import InjectionToken
    from celerity.types.container import Provider
    from celerity.types.module import FunctionHandlerDefinition, GuardDefinition


def module(
    *,
    controllers: list[type] | None = None,
    function_handlers: list[FunctionHandlerDefinition] | None = None,
    guards: list[type | GuardDefinition] | None = None,
    providers: list[type | Provider] | None = None,
    imports: list[type] | None = None,
    exports: list[InjectionToken] | None = None,
) -> Callable[[type], type]:
    """Declare a module -- the organisational unit for grouping
    controllers, providers, and sub-modules.

    Modules form a hierarchy: each application has a root module that
    imports feature modules. Providers are scoped to their declaring
    module unless explicitly exported.

    Args:
        controllers: Controller classes in this module.
        function_handlers: Function-based handler definitions.
        guards: Guard classes or guard definitions.
        providers: Provider classes or provider dicts.
        imports: Other modules to import.
        exports: Tokens to export for importing modules.

    Returns:
        A class decorator that attaches module metadata.

    Example::

        @module(
            controllers=[OrderController],
            providers=[OrderService, PaymentService],
            imports=[DatabaseModule],
            exports=[OrderService],
        )
        class OrderModule:
            pass

        @module(
            function_handlers=[
                {"handler": process_event, "trigger": "consumer"},
            ],
            guards=[JwtGuard],
        )
        class EventModule:
            pass
    """

    def decorator(cls: type) -> type:
        metadata = ModuleMetadata(
            controllers=controllers,
            function_handlers=function_handlers,
            guards=guards,
            providers=providers,
            imports=imports,
            exports=exports,
        )
        set_metadata(cls, MODULE, metadata)
        return cls

    return decorator
