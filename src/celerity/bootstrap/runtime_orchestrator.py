"""Full runtime lifecycle orchestrator.

Dynamically imports ``celerity_runtime_sdk``, loads config from
``CELERITY_*`` environment variables, bootstraps the user's module,
registers handler callbacks, and starts the server.

Event loop model:

``app.setup()`` creates its own asyncio event loop that
``app.run(block=True)`` later drives with ``run_forever()``.
The bootstrap coroutine must run on **this** event loop so that
DI container state, system layers, and handler callbacks all
share the same loop context. ``start_runtime()`` is therefore
a regular function: it runs the async bootstrap on the
Rust-created loop, then hands control to ``app.run()``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from celerity.bootstrap.runtime_entry import RuntimeBootstrapResult, bootstrap_for_runtime
from celerity.types.container import ValueProvider

if TYPE_CHECKING:
    from celerity_runtime_sdk import (
        CoreRuntimeAppConfig,
        CoreRuntimeApplication,
    )

logger = logging.getLogger("celerity.runtime")


def start_runtime(*, block: bool = True) -> None:
    """Start the Celerity runtime in FFI mode.

    Lifecycle:

    1. Import ``celerity_runtime_sdk`` (PyO3 module)
    2. Create ``CoreRuntimeApplication`` from env config
    3. Call ``setup()`` — creates a fresh asyncio event loop internally
    4. Run ``bootstrap_for_runtime()`` on the Rust-created event loop
       so that DI, layers, and handler callbacks share one loop
    5. Register handler callbacks with the runtime
    6. Call ``app.run(block)`` — spawns Tokio in a thread, then calls
       ``event_loop.run_forever()`` to process Python handler callbacks

    Args:
        block: If ``True`` (default), block on
            ``event_loop.run_forever()``. Set to ``False`` for testing.

    Raises:
        ImportError: If ``celerity_runtime_sdk`` is not installed.
    """
    from celerity_runtime_sdk import (
        CoreRuntimeApplication as RuntimeApp,
    )
    from celerity_runtime_sdk import (
        CoreRuntimeConfig,
    )

    config = CoreRuntimeConfig.from_env()
    app = RuntimeApp(config)
    app_config = app.setup()

    # app.setup() creates a new asyncio event loop that is
    # set as the current event loop, we reuse the same event loop
    # for bootstrapping, especially important for contextvars usage.
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(bootstrap_for_runtime())

    _register_http_handlers(app, app_config, result)
    _register_guard_handlers(app, app_config, result)
    _register_websocket_handlers(app, app_config, result)
    _register_consumer_handlers(app, app_config, result)
    _register_event_handlers(app, app_config, result)
    _register_schedule_handlers(app, app_config, result)
    _register_custom_handlers(app, app_config, result)

    if app_config.api and app_config.api.websocket:
        _register_websocket_sender(app, result)

    logger.debug("start_runtime: all handlers registered, starting runtime")
    app.run(block)


# ---------------------------------------------------------------------------
# Handler registration helpers
# ---------------------------------------------------------------------------


def _register_http_handlers(
    app: CoreRuntimeApplication,
    app_config: CoreRuntimeAppConfig,
    result: RuntimeBootstrapResult,
) -> None:
    if not app_config.api or not app_config.api.http:
        return

    for defn in app_config.api.http.handlers:
        callback = result.create_route_callback(defn.method, defn.path, defn.name)
        if callback is None:
            callback = result.create_route_callback_by_id(defn.handler, defn.name)
        if callback:
            app.register_http_handler(defn.path, defn.method, callback, defn.timeout)
            logger.debug("registered http: %s %s", defn.method, defn.path)


def _register_guard_handlers(
    app: CoreRuntimeApplication,
    app_config: CoreRuntimeAppConfig,
    result: RuntimeBootstrapResult,
) -> None:
    if not app_config.api or not app_config.api.guards:
        return

    for defn in app_config.api.guards.handlers:
        callback = result.create_guard_callback(defn.name)
        if callback:
            app.register_guard_handler(defn.name, callback)
            logger.debug("registered guard: %s", defn.name)


def _register_websocket_handlers(
    app: CoreRuntimeApplication,
    app_config: CoreRuntimeAppConfig,
    result: RuntimeBootstrapResult,
) -> None:
    if not app_config.api or not app_config.api.websocket:
        return

    for defn in app_config.api.websocket.handlers:
        callback = result.create_websocket_callback(defn.route, defn.name)
        if callback:
            app.register_websocket_handler(defn.route, callback)
            logger.debug("registered websocket: %s", defn.route)


def _register_consumer_handlers(
    app: CoreRuntimeApplication,
    app_config: CoreRuntimeAppConfig,
    result: RuntimeBootstrapResult,
) -> None:
    if not app_config.consumers:
        return

    for consumer in app_config.consumers.consumers:
        for defn in consumer.handlers:
            method_name = defn.handler.rsplit(".", 1)[-1] if "." in defn.handler else defn.name
            lookup_key = f"{consumer.consumer_name}::{method_name}"
            callback = result.create_consumer_callback(lookup_key, defn.name)
            if callback:
                app.register_consumer_handler(defn.name, callback, defn.timeout)
                logger.debug("registered consumer: %s", defn.name)


def _register_event_handlers(
    app: CoreRuntimeApplication,
    app_config: CoreRuntimeAppConfig,
    result: RuntimeBootstrapResult,
) -> None:
    if not app_config.events:
        return

    for event in app_config.events.events:
        for defn in event.handlers:
            method_name = defn.handler.rsplit(".", 1)[-1] if "." in defn.handler else defn.name
            lookup_key = f"{event.consumer_name}::{method_name}"
            callback = result.create_consumer_callback(lookup_key, defn.name)
            if callback:
                app.register_consumer_handler(defn.name, callback, defn.timeout)
                logger.debug("registered event consumer: %s", defn.name)


def _register_schedule_handlers(
    app: CoreRuntimeApplication,
    app_config: CoreRuntimeAppConfig,
    result: RuntimeBootstrapResult,
) -> None:
    if not app_config.schedules:
        return

    for schedule in app_config.schedules.schedules:
        for defn in schedule.handlers:
            method_name = defn.handler.rsplit(".", 1)[-1] if "." in defn.handler else defn.name
            lookup_key = f"{schedule.schedule_id}::{method_name}"
            callback = result.create_schedule_callback(lookup_key, defn.name)
            if callback:
                app.register_schedule_handler(defn.name, callback, defn.timeout)
                logger.debug("registered schedule: %s", defn.name)


def _register_custom_handlers(
    app: CoreRuntimeApplication,
    app_config: CoreRuntimeAppConfig,
    result: RuntimeBootstrapResult,
) -> None:
    if not app_config.custom_handlers:
        return

    for defn in app_config.custom_handlers.handlers:
        callback = result.create_custom_callback(defn.name)
        if callback:
            app.register_custom_handler(defn.name, callback, defn.timeout)
            logger.debug("registered custom: %s", defn.name)


def _register_websocket_sender(
    app: CoreRuntimeApplication,
    result: RuntimeBootstrapResult,
) -> None:
    """Register the runtime WebSocket sender in the DI container."""
    ws_registry = app.websocket_registry()
    result.container.register("WebSocketRegistry", ValueProvider(use_value=ws_registry))
    logger.debug("registered WebSocketRegistry in DI container")
