"""Cache connection configuration presets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celerity.resources._common import RuntimeMode


@dataclass(frozen=True)
class ConnectionConfig:
    """Redis connection configuration."""

    connect_timeout_ms: int
    command_timeout_ms: int
    keep_alive_ms: int
    max_retries: int
    retry_delay_ms: int
    lazy_connect: bool


FUNCTIONS_CONNECTION = ConnectionConfig(
    connect_timeout_ms=5_000,
    command_timeout_ms=5_000,
    keep_alive_ms=0,
    max_retries=2,
    retry_delay_ms=100,
    lazy_connect=True,
)

RUNTIME_CONNECTION = ConnectionConfig(
    connect_timeout_ms=10_000,
    command_timeout_ms=0,
    keep_alive_ms=30_000,
    max_retries=10,
    retry_delay_ms=500,
    lazy_connect=False,
)

_CONFIG_KEY_MAP: dict[str, str] = {
    "connectTimeoutMs": "connect_timeout_ms",
    "commandTimeoutMs": "command_timeout_ms",
    "keepAliveMs": "keep_alive_ms",
    "maxRetries": "max_retries",
    "retryDelayMs": "retry_delay_ms",
    "lazyConnect": "lazy_connect",
}


def resolve_connection_config(
    runtime_mode: RuntimeMode,
    overrides: dict[str, str] | None = None,
) -> ConnectionConfig:
    """Select a preset and apply per-resource overrides from config keys.

    Args:
        runtime_mode: ``"functions"`` or ``"runtime"``.
        overrides: Optional config key overrides (camelCase keys).

    Returns:
        A ``ConnectionConfig`` with overrides applied.
    """
    base = FUNCTIONS_CONNECTION if runtime_mode == "functions" else RUNTIME_CONNECTION

    if not overrides:
        return base

    fields: dict[str, int | bool] = {}
    for camel_key, field_name in _CONFIG_KEY_MAP.items():
        value = overrides.get(camel_key)
        if value is not None:
            if field_name == "lazy_connect":
                fields[field_name] = value.lower() == "true"
            else:
                fields[field_name] = int(value)

    if not fields:
        return base

    from dataclasses import asdict

    merged = {**asdict(base), **fields}
    return ConnectionConfig(**merged)
