import logging
import asyncio
from typing import Dict, List, Optional, Callable

from .base import EventBusAdapterBase
from ..context import Event

logger = logging.getLogger(__name__)


class InMemoryEventBus(EventBusAdapterBase):
    """
    In-memory event bus for testing and simple deployments.

    Uses asyncio for event handling.
    """

    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = {}
        self._wildcard_subscribers: List[Callable] = []
        self._connected = False

    async def connect(self):
        """Connect to the event bus."""
        self._connected = True
        logger.info("In-memory event bus connected")

    async def disconnect(self):
        """Disconnect from the event bus."""
        self._connected = False
        self._subscribers.clear()
        self._wildcard_subscribers.clear()
        logger.info("In-memory event bus disconnected")

    async def publish(self, event: Event, topic: Optional[str] = None):
        """Publish an event to subscribers."""
        if not self._connected:
            raise RuntimeError("Event bus not connected")

        topic = topic or event.event_type
        logger.debug(f"Publishing event {event.event_id} to topic {topic}")

        # Notify topic-specific subscribers
        for callback in self._subscribers.get(topic, []):
            try:
                await self._invoke_callback(callback, event)
            except Exception as e:
                logger.error(f"Error in subscriber callback: {e}")

        # Notify wildcard subscribers
        for callback in self._wildcard_subscribers:
            try:
                await self._invoke_callback(callback, event)
            except Exception as e:
                logger.error(f"Error in wildcard subscriber: {e}")

    async def _invoke_callback(self, callback: Callable, event: Event):
        """Invoke callback, handling both sync and async."""
        if asyncio.iscoroutinefunction(callback):
            await callback(event)
        else:
            callback(event)

    async def subscribe(self, topic: str, callback: Callable[[Event], None]):
        """Subscribe to a topic."""
        if topic == "*":
            self._wildcard_subscribers.append(callback)
        else:
            if topic not in self._subscribers:
                self._subscribers[topic] = []
            self._subscribers[topic].append(callback)
        logger.info(f"Subscribed to topic: {topic}")

    async def unsubscribe(self, topic: str, callback: Callable[[Event], None]):
        """Unsubscribe from a topic."""
        if topic == "*":
            self._wildcard_subscribers.remove(callback)
        else:
            if topic in self._subscribers:
                self._subscribers[topic].remove(callback)
        logger.info(f"Unsubscribed from topic: {topic}")
