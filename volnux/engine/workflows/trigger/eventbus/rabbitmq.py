import json
import logging
import warnings
from typing import Any, Dict, Optional

try:
    import aio_pika
except ImportError:  # pragma: no cover
    aio_pika = None
    warnings.warn(
        "Dependency 'aio-pika' is not installed. RabbitMQEventBus requires aio-pika to operate. "
        "Install with: pip install aio-pika",
        ImportWarning,
    )

from .base import EventBusAdapterBase, EventCallback
from ..event import Event

logger = logging.getLogger(__name__)


class RabbitMQEventBus(EventBusAdapterBase):
    """
    RabbitMQ adapter for the event bus.

    Uses a single topic exchange (``exchange_name``) and declares one
    auto-delete queue per subscriber callback, bound by routing key.
    """

    def __init__(
        self,
        connection_url: str,
        exchange_name: str = "workflow_events",
    ):
        super().__init__()
        self.connection_url = connection_url
        self.exchange_name = exchange_name
        self._connection = None
        self._channel = None
        self._exchange = None
        # key: f"{topic}:{id(callback)}" -> (queue, consumer_tag)
        self._subscriptions: Dict[str, Any] = {}

    @staticmethod
    def _require_aio_pika() -> None:
        if aio_pika is None:
            raise RuntimeError("aio-pika not installed. Run: pip install aio-pika")

    async def _connect(self) -> None:
        self._require_aio_pika()
        try:
            self._connection = await aio_pika.connect_robust(self.connection_url)
            self._channel = await self._connection.channel()
            self._exchange = await self._channel.declare_exchange(
                self.exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True,
            )
            logger.info("Connected to RabbitMQ exchange %r", self.exchange_name)
        except Exception as e:
            await self._safe_close()
            raise RuntimeError("Connection to RabbitMQ failed") from e

    async def _disconnect(self) -> None:
        await self._safe_close()

    async def _safe_close(self) -> None:
        # Cancel all consumers first so no messages are in-flight.
        for key, (queue, consumer_tag) in list(self._subscriptions.items()):
            try:
                await queue.cancel(consumer_tag)
            except Exception:  # noqa: BLE001
                logger.exception("Error cancelling consumer %s", key)
        self._subscriptions.clear()

        if self._connection is not None:
            try:
                await self._connection.close()
            except Exception:  # noqa: BLE001
                logger.exception("Error closing RabbitMQ connection")

        self._connection = None
        self._channel = None
        self._exchange = None

    async def publish(self, event: Event, topic: Optional[str] = None) -> None:
        self._require_connected()
        topic = self._resolve_topic(event, topic)

        message = aio_pika.Message(
            body=json.dumps(event.to_dict()).encode(),
            content_type="application/json",
            message_id=event.event_id,
            timestamp=event.timestamp,
        )
        await self._exchange.publish(message, routing_key=topic)
        logger.debug("Published event %s to RabbitMQ topic %s", event.event_id, topic)

    async def subscribe(self, topic: str, callback: EventCallback) -> None:
        self._require_connected()

        key = self._subscription_key(topic, callback)
        if key in self._subscriptions:
            logger.debug("Already subscribed: %s", key)
            return

        queue = await self._channel.declare_queue(
            f"workflow_trigger_{topic}_{id(callback)}",
            auto_delete=True,
        )
        await queue.bind(self._exchange, routing_key=topic)

        async def on_message(message: "aio_pika.IncomingMessage") -> None:
            async with message.process():
                try:
                    event = Event.from_dict(json.loads(message.body.decode()))
                except (TypeError, ValueError, KeyError):
                    logger.exception(
                        "Malformed event on RabbitMQ topic %s (id=%s)",
                        topic,
                        message.message_id,
                    )
                    return
                await self._invoke_callback(callback, event)

        consumer_tag = await queue.consume(on_message)
        self._subscriptions[key] = (queue, consumer_tag)
        logger.info("Subscribed to RabbitMQ topic: %s", topic)

    async def unsubscribe(self, topic: str, callback: EventCallback) -> None:
        key = self._subscription_key(topic, callback)
        entry = self._subscriptions.pop(key, None)
        if entry is None:
            logger.warning(
                "Unsubscribe: no subscription for topic=%s callback=%s",
                topic,
                getattr(callback, "__qualname__", callback),
            )
            return

        queue, consumer_tag = entry
        try:
            await queue.cancel(consumer_tag)
        except Exception:  # noqa: BLE001
            logger.exception("Error cancelling RabbitMQ consumer for %s", topic)
        logger.info("Unsubscribed from RabbitMQ topic: %s", topic)

    @staticmethod
    def _subscription_key(topic: str, callback: EventCallback) -> str:
        return f"{topic}:{id(callback)}"
