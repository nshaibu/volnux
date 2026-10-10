"""
Redis-backed command channel using pub/sub for task-level command routing.

Why Redis pub/sub (not polling):
    The command channel must deliver PAUSE/CANCEL/CHECKPOINT to a running
    task within milliseconds. Polling introduces latency proportional to
    the poll interval. Redis pub/sub delivers messages in <1ms on a local
    network — appropriate for cooperative pause semantics.

Key space conventions:
    volnux:task:loc:{task_id} → node_id (string, TTL=LOCATION_TTL)
                                  Registers which node owns this task.
                                  Refreshed every LOCATION_REFRESH interval.
                                  Expires automatically on node failure.

    volnux:task:cmd:{task_id}   → pub/sub channel for TaskCommand messages
                                  Published by any node (coordinator or operator).
                                  Subscribed by the node that owns the task.

    volnux:task:msg:{task_id}   → pub/sub channel for TaskMessage replies
                                  Published by the node that owns the task.
                                  Subscribed by the initiating coordinator.

    volnux:node:alive:{node_id} → heartbeat key (TTL=NODE_HEARTBEAT_TTL)
                                  Presence = node is alive.
                                  Absence = node failed, tasks need recovery.

Routing model:
    Publish to volnux:task:cmd:{task_id} — Redis delivers to exactly the
    subscriber holding that channel. No explicit node lookup required for
    delivery. Node location (volnux:task:loc) is for administrative
    inspection and recovery, not for routing.

    This means any node can send a command to any task without knowing
    which node runs it. Redis handles the fan — in practice there is
    exactly one subscriber per task_id (the owning node).

Dependencies:
    pip install redis[asyncio]>=5.0.0

Architecture note:
    RedisChannel replaces MeshChannel in the go-to-market deployment.
    The CommandChannelBase interface is identical. When the P2P mesh
    implementation is complete, MeshChannel can be swapped in with
    zero changes to EventBase or AgentEventBase code.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Optional, Any

from volnux.distribution.channels.base import CommandChannelBase
from volnux.distribution.commands import TaskCommand, TaskMessage, CommandType

logger = logging.getLogger(__name__)

LOCATION_TTL = int(os.environ.get("VOLNUX_TASK_LOCATION_TTL", "300"))
LOCATION_REFRESH = int(os.environ.get("VOLNUX_TASK_LOCATION_REFRESH", "60"))
NODE_HEARTBEAT_TTL = int(os.environ.get("VOLNUX_NODE_HEARTBEAT_TTL", "30"))
SUBSCRIBE_TIMEOUT = float(os.environ.get("VOLNUX_SUBSCRIBE_TIMEOUT", "0.05"))
QUEUE_MAX_SIZE = int(os.environ.get("VOLNUX_CHANNEL_QUEUE_SIZE", "1000"))


def _cmd_channel(task_id: str) -> str:
    return f"volnux:task:cmd:{task_id}"


def _msg_channel(task_id: str) -> str:
    return f"volnux:task:msg:{task_id}"


def _loc_key(task_id: str) -> str:
    return f"volnux:task:loc:{task_id}"


def _heartbeat_key(node_id: str) -> str:
    return f"volnux:node:alive:{node_id}"


class RedisConnectionPool:
    """
    Shared asyncio Redis connection pool for all RedisChannel instances
    on a node. Creating one pool per channel would exhaust Redis connections
    at scale. One pool shared across all channels is the correct pattern.

    Usage:
        pool = await RedisConnectionPool.get()
        client = pool.client        # for regular commands (SET, GET, PUBLISH)
        # For subscriptions, always create a new client per channel —
        # a subscribed client cannot issue regular commands.
    """

    _instance: Optional[RedisConnectionPool] = None
    _lock = asyncio.Lock()

    def __init__(self, client: Any, url: str):
        self.client = client
        self._url = url

    @classmethod
    async def get(cls) -> RedisConnectionPool:
        async with cls._lock:
            if cls._instance is None:
                cls._instance = await cls._create()
        return cls._instance

    @classmethod
    async def _create(cls) -> RedisConnectionPool:
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise RuntimeError(
                "redis package not installed. "
                "Run: pip install 'redis[asyncio]>=5.0.0'"
            )

        url = os.environ.get("VOLNUX_REDIS_URL", "redis://localhost:6379/0")

        pool = aioredis.ConnectionPool.from_url(
            url,
            max_connections=50,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
            retry_on_timeout=True,
            health_check_interval=30,
        )
        client = aioredis.Redis(connection_pool=pool)
        logger.info("RedisConnectionPool: connected to %s", url)
        return cls(client=client, url=url)

    @classmethod
    async def new_subscriber(cls) -> Any:
        """
        Create a dedicated Redis client for pub/sub subscription.
        A subscribed client is in subscription mode and cannot issue
        regular commands. Always use a fresh client for subscriptions.
        """
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise RuntimeError("redis package not installed.")

        pool = await cls.get()
        return aioredis.Redis.from_url(pool._url, socket_timeout=None)


class RedisChannel(CommandChannelBase):
    """
    Redis pub/sub implementation of CommandChannelBase.

    Each channel instance is scoped to a single task_id. Commands published
    to volnux:task:cmd:{task_id} are received by the subscriber for that
    task. Messages published to volnux:task:msg:{task_id} are received by
    the coordinator that initiated the command.

    Lifecycle:
        channel = RedisChannel(task_id, node_id=node_id)
        await channel.start() # subscribe, register location
        ...
        await channel.stop() # unsubscribe, deregister location

    Child channels (for tools in AgentEventBase):
        child = type(channel)(tool_task_id, node_id=channel._node_id)
        await child.start()
        # tool uses child for its lifecycle
        await child.stop()          # stops only child, not parent

    Thread safety:
        All methods are async and safe to call from the event loop.
        The _listener_task runs as a background asyncio Task.
    """

    def __init__(
        self,
        task_id: str,
        node_id: Optional[str] = None,
    ):
        super().__init__(task_id)
        self._node_id = node_id or os.environ.get(
            "VOLNUX_NODE_ID", str(uuid.uuid4())[:8]
        )
        self._cmd_queue: asyncio.Queue[TaskCommand] = asyncio.Queue(
            maxsize=QUEUE_MAX_SIZE
        )
        self._msg_queue: asyncio.Queue[TaskMessage] = asyncio.Queue(
            maxsize=QUEUE_MAX_SIZE
        )
        self._listener_task: Optional[asyncio.Task] = None
        self._location_task: Optional[asyncio.Task] = None
        self._subscriber: Optional[Any] = None
        self._pubsub: Optional[Any] = None
        self._running: bool = False
        self._started: bool = False

    async def start(self) -> None:
        """
        Subscribe to this task's command channel and register task location.
        Must be called before receive_command() or receive_message() will work.
        """
        if self._started:
            return

        self._running = True
        self._started = True

        # Subscribe to command channel
        self._subscriber = await RedisConnectionPool.new_subscriber()
        self._pubsub = self._subscriber.pubsub(ignore_subscribe_messages=True)
        await self._pubsub.subscribe(_cmd_channel(self.task_id))

        # Background listener — drains pub/sub messages into local queue
        self._listener_task = asyncio.create_task(
            self._listener_loop(),
            name=f"redis-cmd-listener:{self.task_id}",
        )

        # Register task location in Redis (with TTL)
        await self._register_location()

        # Background refresh — keeps location TTL alive
        self._location_task = asyncio.create_task(
            self._location_refresh_loop(),
            name=f"redis-loc-refresh:{self.task_id}",
        )

        logger.debug("RedisChannel.start: task=%s node=%s", self.task_id, self._node_id)

    async def stop(self) -> None:
        """
        Unsubscribe from the command channel and deregister task location.
        Safe to call multiple times.
        """
        if not self._started:
            return

        self._running = False

        # Cancel background tasks
        for task in (self._listener_task, self._location_task):
            if task and not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass

        # Unsubscribe and close pub/sub connection
        if self._pubsub:
            try:
                await self._pubsub.unsubscribe(_cmd_channel(self.task_id))
                await self._pubsub.aclose()
            except Exception:
                pass

        if self._subscriber:
            try:
                await self._subscriber.aclose()
            except Exception:
                pass

        # Deregister task location
        try:
            pool = await RedisConnectionPool.get()
            await pool.client.delete(_loc_key(self.task_id))
        except Exception as exc:
            logger.warning("RedisChannel.stop: failed to deregister location: %s", exc)

        logger.debug("RedisChannel.stop: task=%s", self.task_id)

    async def send_command(self, command: TaskCommand) -> None:
        """
        Publish a TaskCommand to the target task's command channel.
        The target task_id is in command.task_id — it may differ from
        self.task_id when an agent is forwarding commands to a tool.
        """
        try:
            pool = await RedisConnectionPool.get()
            payload = self._serialise_command(command)
            receivers = await pool.client.publish(
                _cmd_channel(command.task_id), payload
            )
            if receivers == 0:
                logger.warning(
                    "RedisChannel.send_command: no subscribers for task=%s "
                    "(task may have completed or not started yet)",
                    command.task_id,
                )
        except Exception as exc:
            logger.error(
                "RedisChannel.send_command: failed to publish command "
                "type=%s task=%s: %s",
                command.command_type,
                command.task_id,
                exc,
            )
            raise

    async def receive_command(
        self, timeout: Optional[float] = None
    ) -> Optional[TaskCommand]:
        """
        Receive the next TaskCommand from the local queue.
        Returns None on timeout. Blocks indefinitely when timeout is None.
        """
        try:
            if timeout is not None:
                return await asyncio.wait_for(self._cmd_queue.get(), timeout=timeout)
            return await self._cmd_queue.get()
        except asyncio.TimeoutError:
            return None

    async def send_message(self, message: TaskMessage) -> None:
        """
        Publish a TaskMessage to the message channel for this task.
        The coordinator (or initiating node) subscribes to this channel
        to receive progress updates and status reports.
        """
        try:
            pool = await RedisConnectionPool.get()
            payload = self._serialise_message(message)
            await pool.client.publish(_msg_channel(message.task_id), payload)
        except Exception as exc:
            logger.error("RedisChannel.send_message: failed: %s", exc)
            raise

    async def receive_message(
        self, timeout: Optional[float] = None
    ) -> Optional[TaskMessage]:
        """
        Receive the next TaskMessage from the local queue.
        Primarily used by coordinators monitoring task progress.
        """
        try:
            if timeout is not None:
                return await asyncio.wait_for(self._msg_queue.get(), timeout=timeout)
            return await self._msg_queue.get()
        except asyncio.TimeoutError:
            return None

    async def _register_location(self) -> None:
        """
        Write task_id → node_id into Redis with TTL.
        Allows operators and administrative tooling to know which node
        owns which task. Not used in the command delivery hot path.
        """
        try:
            pool = await RedisConnectionPool.get()
            await pool.client.setex(
                _loc_key(self.task_id),
                LOCATION_TTL,
                self._node_id,
            )
        except Exception as exc:
            # Non-fatal — command delivery does not depend on location
            logger.warning(
                "RedisChannel: failed to register location " "task=%s node=%s: %s",
                self.task_id,
                self._node_id,
                exc,
            )

    async def _location_refresh_loop(self) -> None:
        """
        Periodically refresh the task location TTL to prevent expiry
        while the task is still running.
        """
        while self._running:
            await asyncio.sleep(LOCATION_REFRESH)
            if not self._running:
                break
            await self._register_location()

    async def _listener_loop(self) -> None:
        """
        Background task that reads from the Redis pub/sub subscription
        and puts received messages into the local asyncio.Queue.

        The queue decouples Redis I/O from the event's command polling.
        EventCommandMixin calls receive_command(timeout=0.01) rapidly;
        this loop handles the Redis I/O at its own pace.
        """
        logger.debug("RedisChannel._listener_loop: started task=%s", self.task_id)

        while self._running:
            try:
                message = await asyncio.wait_for(
                    self._pubsub.get_message(ignore_subscribe_messages=True),
                    timeout=SUBSCRIBE_TIMEOUT,
                )

                if message is None:
                    continue

                if message.get("type") != "message":
                    continue

                raw = message.get("data", b"")
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")

                command = self._deserialise_command(raw)
                if command is not None:
                    try:
                        self._cmd_queue.put_nowait(command)
                    except asyncio.QueueFull:
                        logger.error(
                            "RedisChannel: command queue full task=%s — "
                            "dropping command type=%s. "
                            "Increase VOLNUX_CHANNEL_QUEUE_SIZE.",
                            self.task_id,
                            command.command_type,
                        )

            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                # Normal — no messages in this interval
                continue
            except Exception as exc:
                if self._running:
                    logger.warning(
                        "RedisChannel._listener_loop: error task=%s: %s "
                        "— reconnecting in 1s",
                        self.task_id,
                        exc,
                    )
                    await asyncio.sleep(1.0)
                    # Attempt to re-subscribe after transient Redis error
                    try:
                        await self._pubsub.subscribe(_cmd_channel(self.task_id))
                    except Exception:
                        pass

        logger.debug("RedisChannel._listener_loop: stopped task=%s", self.task_id)

    @staticmethod
    def _serialise_command(command: TaskCommand) -> str:
        return json.dumps(
            {
                "task_id": command.task_id,
                "command_type": command.command_type.value,
                "payload": command.payload or {},
                "timestamp": command.timestamp,
            }
        )

    @staticmethod
    def _deserialise_command(raw: str) -> Optional[TaskCommand]:
        try:
            data = json.loads(raw)
            return TaskCommand(
                task_id=data["task_id"],
                command_type=CommandType(data["command_type"]),
                payload=data.get("payload", {}),
                timestamp=data.get("timestamp", time.time()),
            )
        except Exception as exc:
            logger.warning(
                "RedisChannel: failed to deserialise command: %s raw=%r",
                exc,
                raw[:200],
            )
            return None

    @staticmethod
    def _serialise_message(message: TaskMessage) -> str:
        return json.dumps(
            {
                "task_id": message.task_id,
                "message_type": message.message_type.value,
                "payload": message.payload or {},
                "timestamp": message.timestamp,
            }
        )


class CoordinatorMessageSubscriber:
    """
    Coordinator-side subscriber for TaskMessage replies from tasks.

    The coordinator (Manager) uses this to receive progress updates,
    status reports, and errors from all tasks it manages. Unlike
    RedisChannel (which is scoped to one task), this subscriber
    maintains a pattern subscription across multiple task message
    channels.

    Usage:
        subscriber = CoordinatorMessageSubscriber(node_id="node-1")
        await subscriber.start()

        # Subscribe to messages from a specific task
        await subscriber.watch(task_id)

        # Receive next message from any watched task
        msg = await subscriber.receive(timeout=1.0)

        await subscriber.stop()
    """

    def __init__(self, node_id: str):
        self._node_id = node_id
        self._pubsub: Optional[Any] = None
        self._subscriber: Optional[Any] = None
        self._msg_queue: asyncio.Queue[TaskMessage] = asyncio.Queue(
            maxsize=QUEUE_MAX_SIZE
        )
        self._listener_task: Optional[asyncio.Task] = None
        self._running: bool = False
        self._watched: set[str] = set()

    async def start(self) -> None:
        self._running = True
        self._subscriber = await RedisConnectionPool.new_subscriber()
        self._pubsub = self._subscriber.pubsub(ignore_subscribe_messages=True)
        self._listener_task = asyncio.create_task(
            self._listener_loop(),
            name=f"coord-msg-listener:{self._node_id}",
        )

    async def stop(self) -> None:
        self._running = False
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await asyncio.wait_for(self._listener_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        if self._pubsub:
            try:
                await self._pubsub.aclose()
            except Exception:
                pass
        if self._subscriber:
            try:
                await self._subscriber.aclose()
            except Exception:
                pass

    async def watch(self, task_id: str) -> None:
        """Subscribe to messages from a specific task."""
        if task_id not in self._watched:
            await self._pubsub.subscribe(_msg_channel(task_id))
            self._watched.add(task_id)

    async def unwatch(self, task_id: str) -> None:
        """Unsubscribe from messages from a specific task."""
        if task_id in self._watched:
            await self._pubsub.unsubscribe(_msg_channel(task_id))
            self._watched.discard(task_id)

    async def receive(self, timeout: Optional[float] = None) -> Optional[TaskMessage]:
        try:
            if timeout is not None:
                return await asyncio.wait_for(self._msg_queue.get(), timeout=timeout)
            return await self._msg_queue.get()
        except asyncio.TimeoutError:
            return None

    async def _listener_loop(self) -> None:
        while self._running:
            try:
                message = await asyncio.wait_for(
                    self._pubsub.get_message(ignore_subscribe_messages=True),
                    timeout=SUBSCRIBE_TIMEOUT,
                )
                if message is None or message.get("type") != "message":
                    continue
                raw = message.get("data", b"")
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                msg = self._deserialise_message(raw)
                if msg:
                    try:
                        self._msg_queue.put_nowait(msg)
                    except asyncio.QueueFull:
                        logger.warning(
                            "CoordinatorMessageSubscriber: message queue full"
                        )
            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                if self._running:
                    logger.warning("CoordinatorMessageSubscriber: error: %s", exc)
                    await asyncio.sleep(1.0)

    @staticmethod
    def _deserialise_message(raw: str) -> Optional[TaskMessage]:
        try:
            from volnux.distribution.commands import MessageType

            data = json.loads(raw)
            return TaskMessage(
                task_id=data["task_id"],
                message_type=MessageType(data["message_type"]),
                payload=data.get("payload", {}),
                timestamp=data.get("timestamp", time.time()),
            )
        except Exception as exc:
            logger.warning(
                "CoordinatorMessageSubscriber: failed to deserialise: %s",
                exc,
            )
            return None


class NodeHeartbeat:
    """
    Publishes a periodic heartbeat key in Redis so other nodes and
    administrative tooling can detect node liveness.

    Key: volnux:node:alive:{node_id}  TTL = NODE_HEARTBEAT_TTL

    When a node fails, its heartbeat key expires within NODE_HEARTBEAT_TTL
    seconds. The RehydrationManager uses this to detect that tasks on the
    failed node need recovery — their volnux:task:loc:{task_id} still
    points to a dead node, which triggers checkpoint-based task migration.
    """

    def __init__(self, node_id: str, metadata: Optional[dict] = None):
        self._node_id = node_id
        self._metadata = metadata or {}
        self._task: Optional[asyncio.Task] = None
        self._running: bool = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(
            self._heartbeat_loop(),
            name=f"node-heartbeat:{self._node_id}",
        )
        logger.info(
            "NodeHeartbeat: started node=%s ttl=%ds",
            self._node_id,
            NODE_HEARTBEAT_TTL,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await asyncio.wait_for(self._task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        try:
            pool = await RedisConnectionPool.get()
            await pool.client.delete(_heartbeat_key(self._node_id))
        except Exception:
            pass

    async def _heartbeat_loop(self) -> None:
        refresh_interval = NODE_HEARTBEAT_TTL // 2  # refresh at half the TTL
        while self._running:
            try:
                pool = await RedisConnectionPool.get()
                value = json.dumps(
                    {
                        "node_id": self._node_id,
                        "timestamp": time.time(),
                        **self._metadata,
                    }
                )
                await pool.client.setex(
                    _heartbeat_key(self._node_id),
                    NODE_HEARTBEAT_TTL,
                    value,
                )
            except Exception as exc:
                logger.warning("NodeHeartbeat: failed to publish: %s", exc)
            await asyncio.sleep(refresh_interval)
