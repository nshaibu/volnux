from typing import List

from .base import EventFilterBase
from ..triggers.base import Event


class CompositeFilter(EventFilterBase):
    """Combine multiple filters with AND/OR logic."""

    def __init__(self, filters: List[EventFilterBase], operator: str = "AND"):
        self.filters = filters
        self.operator = operator.upper()

    def matches(self, event: Event) -> bool:
        """Evaluate composite filter."""
        if self.operator == "AND":
            return all(f.matches(event) for f in self.filters)
        elif self.operator == "OR":
            return any(f.matches(event) for f in self.filters)
        else:
            raise ValueError(f"Unknown operator: {self.operator}")
