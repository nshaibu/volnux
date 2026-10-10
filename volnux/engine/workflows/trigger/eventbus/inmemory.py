import logging
import asyncio
from typing import Dict, List, Optional, Callable

from .base import EventBusAdapterBase
from ..event import Event

logger = logging.getLogger(__name__)


class InMemoryEventBus(EventBusAdapterBase):
    """
    In-memory event bus for testing and simple deployments.

    Supports the ``"*"`` wildcard topic for catch-all subscribers.
    """

    def __init__(self):
        super().__init__()
        self._subscribers: Dict[str, List[Callable]] = {}
        self._wildcard_subscribers: List[Callable] = []

    async def _connect(self):
        pass

    async def _disconnect(self):
        self._subscribers.clear()
        self._wildcard_subscribers.clear()

    async def publish(self, event: Event, topic: Optional[str] = None):
        self._require_connected()
        topic = self._resolve_topic(event, topic)
        logger.debug(f"Publishing event {event.event_id} to topic {topic}")

        for callback in list(self._subscribers.get(topic, [])):
            await self._invoke_callback(callback, event)

        for callback in list(self._wildcard_subscribers):
            await self._invoke_callback(callback, event)

    async def subscribe(self, topic: str, callback: Callable[[Event], None]):
        if topic == "*":
            self._wildcard_subscribers.append(callback)
        else:
            self._subscribers.setdefault(topic, []).append(callback)
        logger.info(f"Subscribed to topic: {topic}")

    async def unsubscribe(self, topic: str, callback: Callable[[Event], None]):
        try:
            if topic == "*":
                self._wildcard_subscribers.remove(callback)
            elif topic in self._subscribers:
                self._subscribers[topic].remove(callback)
        except ValueError:
            logger.warning("Unsubscribe: callback not registered for %r", topic)
        logger.info(f"Unsubscribed from topic: {topic}")
