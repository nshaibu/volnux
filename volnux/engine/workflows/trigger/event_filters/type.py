from typing import List

from .base import EventFilterBase
from ..triggers.base import Event


class TypeFilter(EventFilterBase):
    """Filter events by event type."""

    def __init__(self, event_types: List[str]):
        self.event_types = set(event_types)

    def matches(self, event: Event) -> bool:
        """Check if an event type is in the allowed list."""
        return event.event_type in self.event_types
