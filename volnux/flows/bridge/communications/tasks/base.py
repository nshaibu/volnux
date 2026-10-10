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


class TaskCommunicationBridge(ABC):
    """
    Abstract interface for task communication.

    Implementations provide backend-specific communication for different executors
    (local threads, processes, remote workers, Celery).
    """

    channel_class: Type["CommandChannelBase"]

    def __init__(self, context: "ExecutionContext"):
        self.context = context

        # Channel registry: full_id -> CommandChannelBase
        self._channels: Dict[str, "CommandChannelBase"] = {}

        # Execution handle tracking
        self._execution_handles: Dict[str, TaskExecutionHandle] = {}

        # Status cache
        self._statuses: Dict[str, TaskStatus] = {}

        # Track execution handles by full_id
        self._execution_handles: Dict[str, TaskExecutionHandle] = {}

        self._lock: Optional[LockType] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event: Optional[EventType] = None

    def _get_or_create_channel(self, task_id: str) -> "CommandChannelBase":
        """Get or create a communication channel for a task"""
        if task_id not in self._channels:
            self._channels[task_id] = self.channel_class(task_id)
        return self._channels[task_id]

    def _register_task(
        self, handle: TaskExecutionHandle
    ) -> Tuple["CommandChannelBase", str]:
        """
        Registers a task execution handle to manage its lifecycle and maintain its
        status within the system.

        :param handle: The TaskExecutionHandle object encapsulating essential
                       information about the task execution, including identifiers
                       and task-specific metadata.
        :type handle: TaskExecutionHandle
        :return: A tuple containing the communication channel object associated
                 with the task and the full identifier of the registered task.
        :rtype: Tuple[CommandChannelBase, str]
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

    @abstractmethod
    async def register_task(self, handle: TaskExecutionHandle) -> "CommandChannelBase":
        """
        Registers a task and returns a command channel for handling task execution.

        This method serves as an interface for subclasses to implement task registration
        logic. Upon successful implementation in a subclass, it is expected to associate
        the given handle with a task and return an appropriate command channel to interact
        with the task's execution lifecycle.

        :param handle: The execution handle representing the task to be registered.
        :type handle: TaskExecutionHandle
        :return: An instance of CommandChannelBase for managing task execution.
        :rtype: CommandChannelBase
        :raises NotImplementedError: This method must be overridden in a subclass.
        """

        raise NotImplementedError("register_task must be implemented by subclasses")

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

    async def start_monitoring(self) -> None:
        """Start monitoring task messages"""
        if not self._monitor_task:
            self._monitor_task = asyncio.create_task(self._monitor_messages())

    async def _monitor_messages(self) -> None:
        """Monitor incoming messages from tasks"""
        while not self._shutdown_event.is_set():
            # Poll all channels for messages
            for full_id, channel in list(self._channels.items()):
                try:
                    message = await channel.receive_message(timeout=0.1)
                    if message:
                        await self._process_message(message)
                except Exception as e:
                    logger.error(f"Error receiving message: {e}", exc_info=True)

            # Small sleep to prevent busy loop
            await asyncio.sleep(0.05)

    async def _process_message(self, message: TaskMessage) -> None:
        """Process incoming a message from a task"""
        # Update status if it's a status update
        if message.message_type == MessageType.STATUS_UPDATE:
            status = self._statuses.get(message.task_id)
            if status:
                status.state = TaskState[message.payload.get("state", "RUNNING")]
                status.progress = message.payload.get("progress", status.progress)
                status.message = message.payload.get("message", "")
                status.last_heartbeat = time.time()

        # Update heartbeat timestamp
        elif message.message_type == MessageType.HEARTBEAT:
            status = self._statuses.get(message.task_id)
            if status:
                status.last_heartbeat = time.time()

        logger.debug(
            f"Processed message: {message.message_type} from {message.task_id}"
        )

    async def shutdown(self) -> None:
        """Clean up resources"""
        self._shutdown_event.set()

        if self._monitor_task:
            try:
                await asyncio.wait_for(self._monitor_task, timeout=2.0)
            except asyncio.TimeoutError:
                self._monitor_task.cancel()
