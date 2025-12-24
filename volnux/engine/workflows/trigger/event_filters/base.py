from abc import ABC, abstractmethod

from ..triggers.base import Event


class EventFilterBase(ABC):
    """Base class for event filtering."""

    @abstractmethod
    def matches(self, event: Event) -> bool:
        """Check if event matches this filter."""
        pass
