from abc import ABC, abstractmethod

from ..event import Event


class EventFilterBase(ABC):
    """Base class for event filtering."""

    @abstractmethod
    def matches(self, event: Event) -> bool:
        """Check if event matches this filter."""
        pass
