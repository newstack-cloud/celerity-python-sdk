"""Function-based handler and guard factories."""

from celerity.functions.consumer import create_consumer_handler
from celerity.functions.custom import create_custom_handler
from celerity.functions.guard import create_guard
from celerity.functions.http import (
    create_http_handler,
    http_delete,
    http_get,
    http_patch,
    http_post,
    http_put,
)
from celerity.functions.schedule import create_schedule_handler
from celerity.functions.websocket import create_websocket_handler

__all__ = [
    "create_consumer_handler",
    "create_custom_handler",
    "create_guard",
    "create_http_handler",
    "create_schedule_handler",
    "create_websocket_handler",
    "http_delete",
    "http_get",
    "http_patch",
    "http_post",
    "http_put",
]
