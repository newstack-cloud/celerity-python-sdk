"""Auto-instrumentation for common Python libraries.

Core (always loaded if installed):
- opentelemetry-instrumentation-aiohttp-client -- async HTTP calls
- opentelemetry-instrumentation-urllib3 -- sync HTTP calls (boto3 uses this)

Optional (loaded if installed):
- opentelemetry-instrumentation-aiobotocore -- AWS SDK (S3, DynamoDB, SQS, SNS)
- opentelemetry-instrumentation-redis -- Redis/Valkey
- opentelemetry-instrumentation-asyncpg -- PostgreSQL
- opentelemetry-instrumentation-sqlalchemy -- SQLAlchemy Core/ORM
- opentelemetry-instrumentation-logging -- stdlib logging bridge
"""

from __future__ import annotations

import logging

logger = logging.getLogger("celerity.telemetry")

_INSTRUMENTATIONS: list[tuple[str, str, str]] = [
    # (module_path, class_name, display_name)
    (
        "opentelemetry.instrumentation.aiohttp_client",
        "AioHttpClientInstrumentor",
        "aiohttp-client",
    ),
    (
        "opentelemetry.instrumentation.urllib3",
        "URLLib3Instrumentor",
        "urllib3",
    ),
    (
        "opentelemetry.instrumentation.aiobotocore",
        "AioBotocoreInstrumentor",
        "aiobotocore",
    ),
    (
        "opentelemetry.instrumentation.redis",
        "RedisInstrumentor",
        "redis",
    ),
    (
        "opentelemetry.instrumentation.asyncpg",
        "AsyncPGInstrumentor",
        "asyncpg",
    ),
    (
        "opentelemetry.instrumentation.sqlalchemy",
        "SQLAlchemyInstrumentor",
        "sqlalchemy",
    ),
    (
        "opentelemetry.instrumentation.logging",
        "LoggingInstrumentor",
        "logging",
    ),
]


def load_instrumentations() -> list[str]:
    """Discover and load available OTel instrumentations.

    Optional instrumentations are silently skipped if not installed.
    Returns a list of successfully loaded instrumentation names.
    """
    import importlib

    loaded: list[str] = []
    for module_path, class_name, display_name in _INSTRUMENTATIONS:
        try:
            mod = importlib.import_module(module_path)
            instrumentor_cls = getattr(mod, class_name)
            instrumentor_cls().instrument()
            loaded.append(display_name)
            logger.debug("Loaded instrumentation: %s", display_name)
        except (ImportError, AttributeError, Exception):
            logger.debug("Skipped instrumentation: %s (not installed)", display_name)
    return loaded
