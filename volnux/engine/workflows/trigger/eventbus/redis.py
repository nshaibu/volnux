import asyncio
import json
import logging
import warnings
from typing import Callable, Dict, List, Optional

try:
    import redis.asyncio as redis
except ImportError:  # pragma: no cover
    redis = None
    warnings.warn(
        "Dependency 'redis' is not installed. RedisEventBus requires redis to operate. "
        "Install with: pip install redis",
        ImportWarning,
    )

from .base import EventBusAdapterBase, EventCallback
from ..event import Event

logger = logging.getLogger(__name__)

_CHANNEL_PREFIX = "workflow:events:"


class RedisEventBus(EventBusAdapterBase):
    """
    Redis Pub/Sub adapter for the event bus.

    Channels are namespaced as ``workflow:events:{topic}``.

    Requires:
        pip install redis
    """

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        super().__init__()
        self.redis_url = redis_url
        self._redis = None
        self._pubsub = None
        self._subscribers: Dict[str, List[EventCallback]] = {}
        self._listener_task: Optional[asyncio.Task] = None

    @staticmethod
    def _require_redis() -> None:
        if redis is None:
            raise RuntimeError("redis not installed. Run: pip install redis")

    async def _connect(self) -> None:
        self._require_redis()
        try:
            self._redis = redis.from_url(self.redis_url)
            # Surface connectivity errors early.
            await self._redis.ping()
            self._pubsub = self._redis.pubsub()
            self._listener_task = asyncio.create_task(
                self._listen(), name="RedisEventBus._listen"
            )
            logger.info("Connected to Redis Pub/Sub at %s", self.redis_url)
        except Exception as e:
            # Best-effort cleanup if connect failed partway.
            await self._safe_close()
            raise RuntimeError("Connection to Redis failed") from e

    async def _disconnect(self) -> None:
        await self._safe_close()

    async def _safe_close(self) -> None:
        """Idempotent teardown of listener + pubsub + client."""
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                logger.exception("Redis listener raised during shutdown")
        self._listener_task = None

        if self._pubsub is not None:
            try:
                await self._pubsub.close()
            except Exception:  # noqa: BLE001
                logger.exception("Error closing Redis pubsub")
            self._pubsub = None

        if self._redis is not None:
            try:
                await self._redis.close()
            except Exception:  # noqa: BLE001
                logger.exception("Error closing Redis client")
            self._redis = None

        self._subscribers.clear()

    async def publish(self, event: Event, topic: Optional[str] = None) -> None:
        self._require_connected()
        topic = self._resolve_topic(event, topic)
        channel = f"{_CHANNEL_PREFIX}{topic}"

        payload = json.dumps(event.to_dict())
        await self._redis.publish(channel, payload)
        logger.debug("Published event %s to Redis channel %s", event.event_id, channel)

    async def subscribe(self, topic: str, callback: EventCallback) -> None:
        self._require_connected()
        channel = f"{_CHANNEL_PREFIX}{topic}"

        is_new_channel = channel not in self._subscribers
        self._subscribers.setdefault(channel, []).append(callback)

        if is_new_channel:
            await self._pubsub.subscribe(channel)

        logger.info("Subscribed to Redis channel: %s", channel)

    async def unsubscribe(self, topic: str, callback: EventCallback) -> None:
        channel = f"{_CHANNEL_PREFIX}{topic}"
        subscribers = self._subscribers.get(channel)
        if not subscribers:
            logger.warning("Unsubscribe: no subscribers for %s", channel)
            return
        try:
            subscribers.remove(callback)
        except ValueError:
            logger.warning("Unsubscribe: callback not registered for %s", channel)
            return

        if not subscribers:
            del self._subscribers[channel]
            if self._pubsub is not None:
                await self._pubsub.unsubscribe(channel)

        logger.info("Unsubscribed from Redis channel: %s", channel)

    async def _listen(self) -> None:
        """Receive pub/sub messages and dispatch to local subscribers."""
        assert self._pubsub is not None
        try:
            async for message in self._pubsub.listen():
                if message.get("type") != "message":
                    continue

                channel = _as_str(message["channel"])
                data = _as_str(message["data"])

                try:
                    event = Event.from_dict(json.loads(data))
                except (TypeError, ValueError, KeyError):
                    logger.exception("Malformed event on channel %s", channel)
                    continue

                for cb in list(self._subscribers.get(channel, [])):
                    await self._invoke_callback(cb, event)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("Redis listener terminated unexpectedly")


def _as_str(value) -> str:
    """Coerce bytes → str; pass-through for str."""
    return value.decode() if isinstance(value, (bytes, bytearray)) else value
