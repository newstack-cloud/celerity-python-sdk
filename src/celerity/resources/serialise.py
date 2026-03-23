"""Message body serialisation for queue and topic producers."""

from __future__ import annotations

import dataclasses
import json
from typing import Any

type MessageBody = str | dict[str, Any] | Any
"""Accepted message body types.

- ``str``: passed through as-is.
- ``dict``: serialised with ``json.dumps()``.
- Pydantic ``BaseModel``: serialised via ``model_dump_json()``.
- ``dataclass``: serialised via ``dataclasses.asdict()`` + ``json.dumps()``.
- Other: serialised with ``json.dumps()`` as a fallback.
"""


def serialise_body(body: MessageBody) -> str:
    """Serialise a message body to a JSON string.

    Args:
        body: A string, dict, dataclass, Pydantic model, or any
            JSON-serialisable value.

    Returns:
        A JSON string suitable for sending to a queue or topic.

    Raises:
        TypeError: If *body* is not JSON-serialisable.
    """
    if isinstance(body, str):
        return body

    if isinstance(body, dict):
        return json.dumps(body)

    # Pydantic v2 BaseModel — avoid importing pydantic at module level.
    if hasattr(body, "model_dump_json"):
        result = body.model_dump_json()
        return result if isinstance(result, str) else str(result)

    if dataclasses.is_dataclass(body) and not isinstance(body, type):
        return json.dumps(dataclasses.asdict(body))

    return json.dumps(body)
