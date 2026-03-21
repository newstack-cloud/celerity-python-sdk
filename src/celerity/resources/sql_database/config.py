"""SQL database pool configuration presets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import quote_plus

if TYPE_CHECKING:
    from celerity.resources._common import RuntimeMode


@dataclass(frozen=True)
class PoolConfig:
    """Connection pool configuration for SQL database engines."""

    pool_min_size: int
    pool_max_size: int
    idle_timeout_seconds: float
    acquire_timeout_seconds: float


FUNCTIONS_POOL = PoolConfig(
    pool_min_size=0,
    pool_max_size=2,
    idle_timeout_seconds=1,
    acquire_timeout_seconds=10,
)

RUNTIME_POOL = PoolConfig(
    pool_min_size=2,
    pool_max_size=10,
    idle_timeout_seconds=30,
    acquire_timeout_seconds=10,
)

_POOL_KEY_MAP: dict[str, str] = {
    "poolMinSize": "pool_min_size",
    "poolMaxSize": "pool_max_size",
    "poolIdleTimeout": "idle_timeout_seconds",
    "poolAcquireTimeout": "acquire_timeout_seconds",
}


def resolve_pool_overrides(
    config_key: str,
    overrides: dict[str, str],
) -> dict[str, int | float]:
    """Extract pool overrides from config keys.

    Config keys::

        {configKey}_poolMinSize
        {configKey}_poolMaxSize
        {configKey}_poolIdleTimeout
        {configKey}_poolAcquireTimeout

    Returns a dict of override fields (only those present in config).
    """
    result: dict[str, int | float] = {}
    for camel_key, field_name in _POOL_KEY_MAP.items():
        value = overrides.get(f"{config_key}_{camel_key}")
        if value is not None:
            if field_name == "idle_timeout_seconds" or field_name == "acquire_timeout_seconds":
                result[field_name] = float(value)
            else:
                result[field_name] = int(value)
    return result


def resolve_pool_config(
    runtime_mode: RuntimeMode,
    config_key: str | None = None,
    overrides: dict[str, str] | None = None,
    explicit: PoolConfig | None = None,
) -> PoolConfig:
    """Resolve pool config with 3-level precedence.

    1. Explicit PoolConfig (highest priority)
    2. Per-resource config key overrides
    3. Runtime-mode preset (FUNCTIONS_POOL or RUNTIME_POOL)
    """
    if explicit is not None:
        return explicit

    base = FUNCTIONS_POOL if runtime_mode == "functions" else RUNTIME_POOL

    if not config_key or not overrides:
        return base

    pool_overrides = resolve_pool_overrides(config_key, overrides)
    if not pool_overrides:
        return base

    from dataclasses import asdict

    merged = {**asdict(base), **pool_overrides}
    return PoolConfig(**merged)


def build_connection_url(
    engine: str,
    user: str,
    password: str,
    host: str,
    port: int,
    database: str,
    ssl: bool,
) -> str:
    """Build a SQLAlchemy async connection URL.

    PostgreSQL: ``postgresql+asyncpg://user:pass@host:port/db?ssl=require``
    MySQL:      ``mysql+aiomysql://user:pass@host:port/db?ssl=true``

    Password is URL-encoded to handle special characters.
    """
    encoded_password = quote_plus(password)

    if engine == "postgres":
        dialect = "postgresql+asyncpg"
        ssl_param = "ssl=require" if ssl else ""
    elif engine == "mysql":
        dialect = "mysql+aiomysql"
        ssl_param = "ssl=true" if ssl else ""
    else:
        msg = f"Unsupported engine: {engine!r}. Must be 'postgres' or 'mysql'"
        raise ValueError(msg)

    url = f"{dialect}://{user}:{encoded_password}@{host}:{port}/{database}"
    if ssl_param:
        url = f"{url}?{ssl_param}"
    return url
