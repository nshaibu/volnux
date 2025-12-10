from typing import Dict, Any

from .base import EventFilterBase
from ..triggers.base import Event


class PatternFilter(EventFilterBase):
    """Filter events by pattern matching on event data."""

    def __init__(self, pattern: Dict[str, Any]):
        self.pattern = pattern

    def matches(self, event: Event) -> bool:
        """Check if event data matches the pattern."""
        for key, value in self.pattern.items():
            if key not in event.data or event.data[key] != value:
                return False
        return True
