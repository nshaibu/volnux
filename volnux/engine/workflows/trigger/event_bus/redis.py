import logging
import json
import asyncio
import warnings
from typing import Dict, List, Optional, Callable

try:
    import redis.asyncio as redis
except ImportError:
    redis = None
    warnings.warn(
        "Dependency 'redis' is not installed. RabbitMQEventBus requires redis to operate. "
        "Install with: pip install redis",
        ImportWarning,
    )

from .base import EventBusAdapterBase
from ..context import Event

logger = logging.getLogger(__name__)


class RedisEventBus(EventBusAdapterBase):
    """
    Redis Pub/Sub adapter for the event bus.

    Requires: pip install redis
    """

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self._redis = None
        self._pubsub = None
        self._subscribers: Dict[str, List[Callable]] = {}
        self._listener_task = None

    @staticmethod
    def is_redis_installed():
        if redis is None:
            raise RuntimeError("aio-pika not installed. Run: pip install aio-pika")

    async def connect(self):
        """Connect to Redis."""
        self.is_redis_installed()

        try:
            self._redis = redis.from_url(self.redis_url)
            self._pubsub = self._redis.pubsub()

            # Start listener task
            self._listener_task = asyncio.create_task(self._listen())

            logger.info("Connected to Redis Pub/Sub")
        except ImportError:
            raise RuntimeError("redis not installed. Run: pip install redis")

    async def disconnect(self):
        """Disconnect from Redis."""
        self.is_redis_installed()

        if self._listener_task:
            self._listener_task.cancel()
        if self._pubsub:
            await self._pubsub.close()
        if self._redis:
            await self._redis.close()
        logger.info("Disconnected from Redis")

    async def publish(self, event: Event, topic: Optional[str] = None):
        """Publish event to Redis."""
        self.is_redis_installed()

        topic = topic or event.event_type
        channel = f"workflow:events:{topic}"

        message = json.dumps(event.to_dict())
        await self._redis.publish(channel, message)

        logger.debug(f"Published event {event.event_id} to Redis channel {channel}")

    async def subscribe(self, topic: str, callback: Callable[[Event], None]):
        """Subscribe to Redis channel."""
        channel = f"workflow:events:{topic}"

        if channel not in self._subscribers:
            self._subscribers[channel] = []
            await self._pubsub.subscribe(channel)

        self._subscribers[channel].append(callback)
        logger.info(f"Subscribed to Redis channel: {channel}")

    async def unsubscribe(self, topic: str, callback: Callable[[Event], None]):
        """Unsubscribe from a Redis channel."""
        channel = f"workflow:events:{topic}"

        if channel in self._subscribers:
            self._subscribers[channel].remove(callback)

            if not self._subscribers[channel]:
                await self._pubsub.unsubscribe(channel)
                del self._subscribers[channel]

        logger.info(f"Unsubscribed from Redis channel: {channel}")

    async def _listen(self):
        """Listen for messages from Redis."""
        try:
            async for message in self._pubsub.listen():
                if message["type"] == "message":
                    channel = message["channel"].decode()
                    data = json.loads(message["data"].decode())
                    event = Event.from_dict(data)

                    # Notify subscribers
                    for callback in self._subscribers.get(channel, []):
                        try:
                            await self._invoke_callback(callback, event)
                        except Exception as e:
                            logger.error(f"Error in Redis subscriber: {e}")
        except asyncio.CancelledError:
            pass

    async def _invoke_callback(self, callback: Callable, event: Event):
        """Invoke callback."""
        if asyncio.iscoroutinefunction(callback):
            await callback(event)
        else:
            callback(event)
