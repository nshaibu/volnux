import logging
import asyncio
import json
import warnings
from typing import Dict, Any, Optional, Callable

try:
    import aio_pika
except ImportError:
    aio_pika = None
    warnings.warn(
        "Dependency 'aio-pika' is not installed. RabbitMQEventBus requires aio-pika to operate. "
        "Install with: pip install aio-pika",
        ImportWarning,
    )

from .base import EventBusAdapterBase
from ..context import Event

logger = logging.getLogger(__name__)


class RabbitMQEventBus(EventBusAdapterBase):
    """
    RabbitMQ adapter for the event bus.
    """

    def __init__(self, connection_url: str, exchange_name: str = "workflow_events"):
        self.connection_url = connection_url
        self.exchange_name = exchange_name
        self._connection = None
        self._channel = None
        self._exchange = None
        self._subscriptions: Dict[str, Any] = {}

    @staticmethod
    def is_aio_pika_installed():
        if aio_pika is None:
            raise RuntimeError("aio-pika not installed. Run: pip install aio-pika")

    async def connect(self):
        """Connect to RabbitMQ."""
        self.is_aio_pika_installed()

        try:
            self._connection = await aio_pika.connect_robust(self.connection_url)
            self._channel = await self._connection.channel()
            self._exchange = await self._channel.declare_exchange(
                self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
            )
            logger.info(f"Connected to RabbitMQ: {self.exchange_name}")
        except Exception as e:
            raise RuntimeError("Connection to RabbitMQ failed") from e

    async def disconnect(self):
        """Disconnect from RabbitMQ."""
        if self._connection:
            await self._connection.close()
        logger.info("Disconnected from RabbitMQ")

    async def publish(self, event: Event, topic: Optional[str] = None):
        """Publish event to RabbitMQ."""
        self.is_aio_pika_installed()

        topic = topic or event.event_type
        message = aio_pika.Message(
            body=json.dumps(event.to_dict()).encode(),
            content_type="application/json",
            message_id=event.event_id,
            timestamp=event.timestamp,
        )

        await self._exchange.publish(message, routing_key=topic)
        logger.debug(f"Published event {event.event_id} to RabbitMQ topic {topic}")

    async def subscribe(self, topic: str, callback: Callable[[Event], None]):
        """Subscribe to RabbitMQ topic."""
        self.is_aio_pika_installed()

        # Create queue for this subscription
        queue = await self._channel.declare_queue(
            f"workflow_trigger_{topic}_{id(callback)}", auto_delete=True
        )

        # Bind queue to topic
        await queue.bind(self._exchange, routing_key=topic)

        async def on_message(message: aio_pika.IncomingMessage):
            async with message.process():
                event_data = json.loads(message.body.decode())
                event = Event.from_dict(event_data)
                await self._invoke_callback(callback, event)

        # Start consuming
        consumer_tag = await queue.consume(on_message)
        self._subscriptions[f"{topic}:{id(callback)}"] = (queue, consumer_tag)

        logger.info(f"Subscribed to RabbitMQ topic: {topic}")

    async def unsubscribe(self, topic: str, callback: Callable[[Event], None]):
        """Unsubscribe from RabbitMQ topic."""
        key = f"{topic}:{id(callback)}"
        if key in self._subscriptions:
            queue, consumer_tag = self._subscriptions[key]
            await queue.cancel(consumer_tag)
            del self._subscriptions[key]
            logger.info(f"Unsubscribed from RabbitMQ topic: {topic}")

    async def _invoke_callback(self, callback: Callable, event: Event):
        """Invoke callback."""
        if asyncio.iscoroutinefunction(callback):
            await callback(event)
        else:
            callback(event)
