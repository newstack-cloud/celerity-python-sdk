"""Schedule event types."""

from dataclasses import dataclass
from typing import Any


@dataclass
class ScheduleEventInput:
    """Input delivered to a schedule handler."""

    handler_tag: str
    schedule_id: str
    message_id: str
    schedule: str
    input: Any = None
    vendor: Any = None
    trace_context: dict[str, str] | None = None
