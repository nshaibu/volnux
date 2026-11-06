"""
Task communication protocol for coordinating with running tasks across different executors.

This module provides a unified interface for the coordinator to communicate with tasks
regardless of the execution backend (local threads, processes, remote workers, Celery).
"""

import enum
import logging
import asyncio
import threading
import time
from typing import Type, List, Dict, Optional, Union, Generic, TYPE_CHECKING, Tuple, Any
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from concurrent.futures import Future

if TYPE_CHECKING:
    from .channels.base import CommandChannelBase
    from volnux.event import EventBase
    from volnux.execution.context import ExecutionContext

logger = logging.getLogger(__name__)


LockType = Union[asyncio.Lock, threading.Lock]
EventType = Union[asyncio.Event, threading.Event]


class CommandType(enum.Enum):
    """Commands that can be sent to running tasks"""

    PAUSE = "pause"
    RESUME = "resume"
    CANCEL = "cancel"
    QUERY_STATUS = "query_status"
    UPDATE_PRIORITY = "update_priority"
    REQUEST_PROGRESS = "request_progress"
    CHECKPOINT = "checkpoint"


class MessageType(enum.Enum):
    """Types of messages exchanged between coordinator and tasks"""

    COMMAND = "command"
    STATUS_UPDATE = "status_update"
    PROGRESS_UPDATE = "progress_update"
    HEARTBEAT = "heartbeat"
    ERROR = "error"
    RESULT = "result"
    LOG = "log"


class TaskState(enum.Enum):
    """Execution state of a task"""

    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    #: Waiting for something outside the task (a human, an external event or
    #: condition). Distinct from PAUSED, which is a coordinator command.
    SUSPENDED = "suspended"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class TaskCommand:
    """Command sent from coordinator to task"""

    task_id: str
    command_type: CommandType
    payload: Optional[Dict[str, Any]] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class TaskMessage:
    """Message sent from task to coordinator"""

    task_id: str
    message_type: MessageType
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class TaskStatus:
    """Current status of a task"""

    task_id: str
    state: TaskState
    progress: float = 0.0  # 0.0 to 1.0
    message: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_heartbeat: float = field(default_factory=time.time)


@dataclass
class TaskExecutionHandle:
    """
    Represents a specific execution instance of a task.

    Combines task definition ID with runtime execution ID to uniquely identify
    a running task instance across different backends.
    """

    # Task definition ID (from TaskBase)
    task_def_id: str

    # Runtime execution ID (from EventBase._task_id or Future)
    execution_id: str

    # Event class name (for routing and identification)
    event_name: str

    # Backend-specific identifier (Future, AsyncResult, etc.)
    backend_ref: Optional[Any] = None

    # Execution context state ID
    context_state_id: Optional[str] = None

    @property
    def full_id(self) -> str:
        """Composite ID that uniquely identifies this execution instance"""
        return f"{self.task_def_id}::{self.execution_id}"

    @classmethod
    def from_event(
        cls,
        event: "EventBase",
        task_def_id: str,
        backend_ref: Optional[Any] = None,
        context_state_id: Optional[str] = None,
    ) -> "TaskExecutionHandle":
        """Create a handle from an event instance"""
        return cls(
            task_def_id=task_def_id,
            execution_id=event._task_id,
            event_name=event.__class__.__name__,
            backend_ref=backend_ref,
            context_state_id=context_state_id,
        )

    @classmethod
    def from_future(
        cls,
        future: Future,
        task_def_id: str,
        event_name: str,
        context_state_id: Optional[str] = None,
    ) -> "TaskExecutionHandle":
        """Create a handle from Future with generated execution ID"""
        execution_id = f"exec-{id(future)}"
        return cls(
            task_def_id=task_def_id,
            execution_id=execution_id,
            event_name=event_name,
            backend_ref=future,
            context_state_id=context_state_id,
        )


class ChannelUnavailable(RuntimeError):
    """A control channel cannot be offered for this task (no cross-process
    messaging backend configured, say). The task still runs, without one."""


#: States after which a task sends nothing more.
TERMINAL_STATES = frozenset(
    {TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED}
)


def parse_task_state(raw: Any) -> Optional[TaskState]:
    """The ``TaskState`` for a value sent by an event.

    Events send ``state.value`` (``"running"``); the member NAME (``"RUNNING"``)
    is accepted too. Unknown values return ``None``.
    """
    if isinstance(raw, TaskState):
        return raw
    for lookup in (lambda: TaskState(raw), lambda: TaskState[str(raw).upper()]):
        try:
            return lookup()
        except (ValueError, KeyError):
            continue
    return None


class TaskCommunicationBridge(ABC):
    """
    The coordinator's side of the task control plane.

    A bridge registers each running task, hands the task a channel to receive
    commands on, and consumes the status messages the task sends back. How the
    bytes travel depends on where the task runs:

    * ``LocalTaskCommunicationBridge``: in this process (async and thread executors)
    * ``MessagingTaskCommunicationBridge``: anywhere else (process pool, Celery),
      over a Redis/Kafka-backed messaging backend
    """

    channel_class: Type["CommandChannelBase"]

    #: Seconds a pump waits for a message before checking whether to stop.
    pump_timeout: float = 1.0

    def __init__(self, context: "ExecutionContext"):
        self.context = context

        # Channel registry: full_id -> CommandChannelBase
        self._channels: Dict[str, "CommandChannelBase"] = {}

        # Execution handle tracking: full_id -> handle
        self._execution_handles: Dict[str, TaskExecutionHandle] = {}

        # Status cache: full_id -> TaskStatus
        self._statuses: Dict[str, TaskStatus] = {}

        # asyncio primitives are created lazily inside the running loop: built
        # here they would bind to whatever loop (if any) exists at construction
        self._lock_obj: Optional[asyncio.Lock] = None
        self._pumps: Dict[str, "asyncio.Task[None]"] = {}
        self._monitoring = False
        self._shutdown_done = False

    @property
    def _lock(self) -> asyncio.Lock:
        if self._lock_obj is None:
            self._lock_obj = asyncio.Lock()
        return self._lock_obj

    @property
    def is_shutdown(self) -> bool:
        return self._shutdown_done

    def _get_or_create_channel(self, task_id: str) -> "CommandChannelBase":
        """Get or create a communication channel for a task"""
        if task_id not in self._channels:
            self._channels[task_id] = self.channel_class(task_id)
        return self._channels[task_id]

    def _register_task(
        self, handle: TaskExecutionHandle
    ) -> Tuple["CommandChannelBase", str]:
        """Record ``handle`` and create its channel and initial (QUEUED) status.

        :return: ``(channel, full_id)``.
        """
        full_id = handle.full_id
        self._execution_handles[full_id] = handle
        channel = self._get_or_create_channel(full_id)
        self._statuses[full_id] = TaskStatus(
            task_id=full_id,
            state=TaskState.QUEUED,
            metadata={
                "task_def_id": handle.task_def_id,
                "execution_id": handle.execution_id,
                "event_name": handle.event_name,
            },
        )

        return channel, full_id

    async def register_task(self, handle: TaskExecutionHandle) -> "CommandChannelBase":
        """
        Register a task and return the channel to inject into its event.

        When monitoring has already started the task's messages are consumed
        from this moment on.
        """
        async with self._lock:
            channel, full_id = self._register_task(handle)

        logger.debug(f"Registered execution: {handle.event_name} ({full_id})")
        if self._monitoring:
            self._start_pump(full_id, channel)
        return channel

    def get_execution_handle(self, task_id: str) -> Optional[TaskExecutionHandle]:
        """Get execution handle by any ID format"""
        # Direct lookup
        if task_id in self._execution_handles:
            return self._execution_handles[task_id]

        # Search by task_def_id or execution_id
        for handle in self._execution_handles.values():
            if handle.task_def_id == task_id or handle.execution_id == task_id:
                return handle

        return None

    async def send_command(self, task_id: str, command: TaskCommand) -> bool:
        """
        Send a command to a running task (async).

        Args:
            task_id: Task identifier (full_id, task_def_id, or execution_id)
            command: Command to send

        Returns:
            True if the command was sent successfully
        """
        handle = self.get_execution_handle(task_id)
        if not handle:
            logger.warning(f"Task {task_id} not found in bridge")
            return False

        full_id = handle.full_id
        channel = self._channels.get(full_id)

        if not channel:
            logger.warning(f"No channel for task {full_id}")
            return False

        try:
            command.task_id = full_id
            await channel.send_command(command)
            logger.debug(
                f"Sent command {command.command_type} to {handle.event_name} ({full_id})"
            )
            return True
        except Exception as e:
            logger.error(f"Error sending command: {e}", exc_info=True)
            return False

    async def get_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get the current status of a task"""
        handle = self.get_execution_handle(task_id)
        if not handle:
            return None

        return self._statuses.get(handle.full_id)

    async def is_alive(self, task_id: str, timeout: float = 30.0) -> bool:
        """Check if a task is still alive based on heartbeat"""
        status = await self.get_status(task_id)
        if not status:
            return False
        return (time.time() - status.last_heartbeat) < timeout

    # Monitoring

    async def start_monitoring(self) -> None:
        """Start consuming task messages (idempotent)."""
        if self._monitoring or self._shutdown_done:
            return
        self._monitoring = True
        for full_id, channel in list(self._channels.items()):
            self._start_pump(full_id, channel)

    def _start_pump(self, full_id: str, channel: "CommandChannelBase") -> None:
        """One pump per channel: waits for that task's messages and applies them."""
        existing = self._pumps.get(full_id)
        if existing is not None and not existing.done():
            return
        self._pumps[full_id] = asyncio.create_task(
            self._pump(full_id, channel), name=f"volnux-bridge-pump-{full_id}"
        )

    async def _pump(self, full_id: str, channel: "CommandChannelBase") -> None:
        try:
            while not self._shutdown_done:
                message = await channel.receive_message(timeout=self.pump_timeout)
                if message is None:
                    continue
                await self._process_message(message, full_id)
                status = self._statuses.get(full_id)
                if status is not None and status.state in TERMINAL_STATES:
                    return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Message pump for {full_id} failed: {e}", exc_info=True)

    async def _process_message(
        self, message: TaskMessage, full_id: Optional[str] = None
    ) -> None:
        """Apply a message from a task to its cached status."""
        if full_id is None:
            handle = self.get_execution_handle(message.task_id)
            full_id = handle.full_id if handle else message.task_id
        status = self._statuses.get(full_id)
        if status is None:
            logger.debug(f"Message for unknown task {message.task_id}; ignored")
            return

        payload = message.payload or {}
        if message.message_type in (
            MessageType.STATUS_UPDATE,
            MessageType.PROGRESS_UPDATE,
            MessageType.ERROR,
        ):
            if "state" in payload:
                state = parse_task_state(payload["state"])
                if state is None:
                    logger.warning(
                        f"Ignoring unknown task state {payload['state']!r} for {full_id}"
                    )
                else:
                    status.state = state

            if "progress" in payload:
                status.progress = payload["progress"]
            elif payload.get("total"):
                status.progress = payload.get("step", 0) / payload["total"]

            detail = payload.get("message") or payload.get("error")
            if detail:
                status.message = str(detail)
            elif payload.get("step_name"):
                status.message = str(payload["step_name"])

        status.last_heartbeat = time.time()

        logger.debug(
            f"Processed message: {message.message_type} from {message.task_id}"
        )

    async def shutdown(self) -> None:
        """Stop monitoring and release every channel. Safe to call twice."""
        if self._shutdown_done:
            return
        self._shutdown_done = True

        pumps = list(self._pumps.values())
        for pump in pumps:
            pump.cancel()
        if pumps:
            await asyncio.gather(*pumps, return_exceptions=True)
        self._pumps.clear()

        for channel in list(self._channels.values()):
            try:
                await channel.close()
            except Exception as e:
                logger.debug(f"Error closing channel {channel.task_id}: {e}")
        self._channels.clear()
