"""A command channel carried by the messaging backend (Redis, Kafka...).

Used for tasks that run outside this process (process pool, Celery): both sides
only need the same backend configuration, not a shared address space. Commands
and messages are plain queue entries, the capability every messaging backend
shares; pub/sub would lose a command sent before the task subscribed.

    engine -- push --> [command queue, one per task] -- pop --> task
    engine <-- pop --- [inbox queue, one per flow]   <-- push -- task
"""

import logging
import time
from typing import Any, Dict, Optional, Tuple, TYPE_CHECKING

from formax import BaseModel, InitStrategy

from volnux.backends.messaging.base import QueueSide
from volnux.backends.storage_route import StorageRoute
from volnux.mixins.messaging import MessagingBackendIntegrationMixin

from ..base import CommandType, MessageType, TaskCommand, TaskMessage
from .base import CommandChannelBase

if TYPE_CHECKING:
    from volnux.backends.store import KeyValueStoreBackendBase

logger = logging.getLogger(__name__)

_COMMAND = "command"
_MESSAGE = "message"


class ChannelEnvelope(MessagingBackendIntegrationMixin, BaseModel):
    """One command or message on the wire."""

    class Config:
        # Records need an id when they are built: __post_init__ assigns it, and
        # deserialising a queue entry requires it.
        init_strategy = InitStrategy.DATACLASS

    kind: str
    task_id: str
    name: str
    payload: Dict[str, Any]
    timestamp: float

    @classmethod
    def from_command(cls, command: TaskCommand) -> "ChannelEnvelope":
        return cls(
            kind=_COMMAND,
            task_id=command.task_id,
            name=command.command_type.value,
            payload=command.payload or {},
            timestamp=command.timestamp,
        )

    @classmethod
    def from_message(cls, message: TaskMessage) -> "ChannelEnvelope":
        return cls(
            kind=_MESSAGE,
            task_id=message.task_id,
            name=message.message_type.value,
            payload=message.payload or {},
            timestamp=message.timestamp,
        )

    def to_command(self) -> TaskCommand:
        return TaskCommand(
            task_id=self.task_id,
            command_type=CommandType(self.name),
            payload=dict(self.payload) or None,
            timestamp=self.timestamp,
        )

    def to_message(self) -> TaskMessage:
        return TaskMessage(
            task_id=self.task_id,
            message_type=MessageType(self.name),
            payload=dict(self.payload),
            timestamp=self.timestamp,
        )


def _backend_timeout(timeout: Optional[float]) -> Optional[float]:
    """Channel timeout -> backend timeout.

    Channel: ``None`` waits forever, ``0`` only checks, ``> 0`` waits that long.
    Backend queues: ``None`` is non-blocking, ``0`` waits forever.
    """
    if timeout is None:
        return 0
    if timeout <= 0:
        return None
    return timeout


class MessagingCommandChannel(CommandChannelBase):
    """Channel whose queues live in the configured messaging backend.

    The channel carries only names, so it pickles to a few hundred bytes and
    can be shipped to a pool worker or a Celery worker; the backend is resolved
    from ``KEY_VALUE_STORE_CONFIG`` in whichever process uses the channel.

    :param task_id: Identifies the task; its command queue is derived from it.
    :param inbox: Names the shared queue tasks report on. All tasks of one flow
        use one inbox, so the engine needs a single consumer, not one per task.
    """

    #: A pop blocks inside the backend and returns the moment a command arrives,
    #: so a long poll costs nothing in latency and saves a round trip per 10 ms.
    poll_interval = 1.0

    def __init__(self, task_id: str, inbox: Optional[str] = None):
        super().__init__(task_id)
        self.inbox = inbox or task_id

    def __getstate__(self) -> Tuple[str, str]:
        return self.task_id, self.inbox

    def __setstate__(self, state: Tuple[str, str]) -> None:
        self.task_id, self.inbox = state

    def create_child(self, task_id: str) -> "MessagingCommandChannel":
        return type(self)(task_id, inbox=self.inbox)

    # transport

    @staticmethod
    async def _backend() -> "KeyValueStoreBackendBase":
        return await ChannelEnvelope.get_backend()

    @staticmethod
    def _queue(backend: "KeyValueStoreBackendBase", *parts: str) -> str:
        return StorageRoute(components=["volnux", "task-channel", *parts]).resolve(
            backend
        )

    def _command_queue(self, backend: "KeyValueStoreBackendBase") -> str:
        return self._queue(backend, "cmd", self.task_id)

    def _inbox_queue(self, backend: "KeyValueStoreBackendBase") -> str:
        return self._queue(backend, "inbox", self.inbox)

    async def _push(self, queue: str, envelope: ChannelEnvelope) -> None:
        backend = await self._backend()
        await backend.push(queue, envelope, side=QueueSide.RIGHT)  # type: ignore[attr-defined]

    async def _pop(
        self, queue: str, timeout: Optional[float]
    ) -> Optional[ChannelEnvelope]:
        backend = await self._backend()
        try:
            return await backend.pop(  # type: ignore[attr-defined,no-any-return]
                queue,
                ChannelEnvelope,
                timeout=_backend_timeout(timeout),
                side=QueueSide.LEFT,
            )
        except (
            ValueError,
            KeyError,
        ) as exc:  # a malformed entry must not kill a listener
            logger.warning("Dropped an unreadable channel entry on %s: %s", queue, exc)
            return None

    # CommandChannelBase

    async def send_command(self, command: TaskCommand) -> None:
        backend = await self._backend()
        await self._push(
            self._command_queue(backend), ChannelEnvelope.from_command(command)
        )

    async def receive_command(
        self, timeout: Optional[float] = None
    ) -> Optional[TaskCommand]:
        backend = await self._backend()
        envelope = await self._pop(self._command_queue(backend), timeout)
        return envelope.to_command() if envelope is not None else None

    async def send_message(self, message: TaskMessage) -> None:
        backend = await self._backend()
        await self._push(
            self._inbox_queue(backend), ChannelEnvelope.from_message(message)
        )

    async def receive_message(
        self, timeout: Optional[float] = None
    ) -> Optional[TaskMessage]:
        backend = await self._backend()
        envelope = await self._pop(self._inbox_queue(backend), timeout)
        return envelope.to_message() if envelope is not None else None

    async def close(self) -> None:
        """Delete this task's command queue (best effort)."""
        await self._delete(lambda backend: self._command_queue(backend))

    async def delete_inbox(self) -> None:
        await self._delete(lambda backend: self._inbox_queue(backend))

    async def _delete(self, queue_of: Any) -> None:
        try:
            backend = await self._backend()
            await backend.queue_delete(queue_of(backend))  # type: ignore[attr-defined]
        except Exception as exc:
            logger.debug("Could not delete channel queue for %s: %s", self.task_id, exc)
