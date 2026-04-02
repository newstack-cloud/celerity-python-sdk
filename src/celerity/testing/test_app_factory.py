"""Unified createTestApp factory for unit and integration testing."""

from __future__ import annotations

from typing import Any

from celerity.testing.blueprint import load_blueprint_resources
from celerity.testing.clients import create_real_clients
from celerity.testing.discovery import discover_resource_tokens
from celerity.testing.resource_mocks import create_mocks_for_tokens
from celerity.testing.test_app import TestApp


async def create_test_app(
    module: type,
    *,
    integration: bool = False,
    overrides: dict[str, Any] | None = None,
    blueprint_path: str | None = None,
) -> TestApp:
    """Create a test application for unit or integration testing.

    - ``integration=False`` (default): Auto-discovers resource dependencies
      from the module graph and creates ``AsyncMock`` objects for each.
      Access mocks via ``app.get_datastore_mock()``, etc.

    - ``integration=True``: Creates real resource clients from env vars
      provided by ``celerity dev test``. Physical resource names are resolved
      from the blueprint.

    Explicit overrides take precedence over auto-discovered resources.

    Args:
        module: The root module under test.
        integration: If True, use real infrastructure clients.
        overrides: Token → value overrides (applied last).
        blueprint_path: Path to the blueprint file (auto-detected if omitted).

    Returns:
        A ``TestApp`` instance with mock accessors and lifecycle management.

    Example (unit)::

        app = await create_test_app(module=UsersModule)
        ds = app.get_datastore_mock("usersDatastore")
        ds.get_item.return_value = None

    Example (integration)::

        app = await create_test_app(module=UsersModule, integration=True)
        service = await app.get_container().resolve(UsersService)
        user = await service.create(...)
    """
    # 1. Discover resource tokens from module metadata.
    resource_infos = discover_resource_tokens(module)

    # 2. Create mocks or real clients.
    mocks: dict[str, Any] = {}
    closeables: list[Any] = []
    resource_handles: dict[str, Any] = {}

    if integration:
        bp_resources = load_blueprint_resources(blueprint_path)
        resource_handles, closeables = await create_real_clients(resource_infos, bp_resources)
    else:
        mocks = create_mocks_for_tokens(resource_infos)
        resource_handles = dict(mocks)

    # 3. Build the overrides dict: resource handles + explicit overrides.
    all_overrides: dict[Any, Any] = {}
    for token, handle in resource_handles.items():
        all_overrides[token] = handle

    if overrides:
        all_overrides.update(overrides)

    # 4. Bootstrap via TestApp.create with empty system layers
    # (resources registered manually via overrides, not via layer auto-discovery).
    app = await TestApp.create(module, overrides=all_overrides)

    # 5. Attach mock accessors and closeables.
    app._test_mocks = mocks  # type: ignore[attr-defined]
    app._test_closeables = closeables  # type: ignore[attr-defined]
    app._test_original_close = app.close  # type: ignore[attr-defined]

    # Patch close to also clean up real clients.
    original_close = app.close

    async def enhanced_close() -> None:
        for client in closeables:
            try:
                close_fn = getattr(client, "close", None) or getattr(client, "aclose", None)
                if close_fn:
                    result = close_fn()
                    if hasattr(result, "__await__"):
                        await result
            except Exception:
                pass
        await original_close()

    app.close = enhanced_close  # type: ignore[method-assign]

    # Add mock accessor methods.
    app.get_mock = lambda token: mocks.get(token)  # type: ignore[attr-defined]
    app.get_datastore_mock = lambda name: mocks.get(f"celerity:datastore:{name}")  # type: ignore[attr-defined]
    app.get_topic_mock = lambda name: mocks.get(f"celerity:topic:{name}")  # type: ignore[attr-defined]
    app.get_queue_mock = lambda name: mocks.get(f"celerity:queue:{name}")  # type: ignore[attr-defined]
    app.get_cache_mock = lambda name: mocks.get(f"celerity:cache:{name}")  # type: ignore[attr-defined]
    app.get_bucket_mock = lambda name: mocks.get(f"celerity:bucket:{name}")  # type: ignore[attr-defined]
    app.get_config_mock = lambda name: mocks.get(f"celerity:config:{name}")  # type: ignore[attr-defined]

    return app
