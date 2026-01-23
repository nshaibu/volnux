from abc import ABC, abstractmethod
from typing import Optional, Callable

from ..context import Event


class EventBusAdapterBase(ABC):
    """
    Abstract adapter for different event bus implementations.

    This allows plugging in different message queue systems:
    - RabbitMQ
    - Kafka
    - Redis Pub/Sub
    - AWS SQS/SNS
    - In-memory (for testing)
    """

    @abstractmethod
    async def publish(self, event: Event, topic: Optional[str] = None):
        """Publish an event to the bus."""
        pass

    @abstractmethod
    async def subscribe(self, topic: str, callback: Callable[[Event], None]):
        """Subscribe to events on a topic."""
        pass

    @abstractmethod
    async def unsubscribe(self, topic: str, callback: Callable[[Event], None]):
        """Unsubscribe from a topic."""
        pass

    @abstractmethod
    async def connect(self):
        """Connect to the event bus."""
        pass

    @abstractmethod
    async def disconnect(self):
        """Disconnect from the event bus."""
        pass
