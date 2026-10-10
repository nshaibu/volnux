import asyncio
import logging
import time
import typing
from dataclasses import dataclass, field

from celery import Celery
from celery.result import AsyncResult
from celery.signals import (
    task_prerun,
    task_postrun,
    task_failure,
    task_success,
    task_revoked,
    task_retry,
)

from .base import (
    TaskCommand,
    TaskMessage,
    TaskStatus,
    TaskState,
    CommandType,
    MessageType,
    TaskExecutionHandle,
)
from .process import ProcessTaskCommunicationBridge

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext

logger = logging.getLogger(__name__)


@dataclass
class CeleryTaskMetadata:
    """Metadata tracked for each Celery task"""

    task_id: str
    celery_task_id: str
    async_result: AsyncResult
    start_time: typing.Optional[float] = None
    state_history: typing.List[typing.Tuple[float, str]] = field(default_factory=list)

    def add_state(self, state: str) -> None:
        """Record state transition"""
        self.state_history.append((time.time(), state))


class CelerySignalMonitor:
    """
    Monitors Celery signals and forwards them as TaskMessages.

    This runs in the coordinator process and listens to signals
    emitted by Celery workers.
    """

    def __init__(self, celery_app: Celery):
        self.celery_app = celery_app
        self._callbacks: typing.Dict[str, typing.List[typing.Callable]] = {}
        self._connected = False

    def connect_signals(self) -> None:
        """Connect to Celery signals"""
        if self._connected:
            return

        @task_prerun.connect
        def on_task_prerun(sender=None, task_id=None, task=None, **kwargs):
            """Task about to start"""
            self._notify(
                "task_started",
                task_id,
                {
                    "task_name": sender.name if sender else "unknown",
                    "timestamp": time.time(),
                },
            )

        @task_postrun.connect
        def on_task_postrun(sender=None, task_id=None, **kwargs):
            """Task completed (success or failure)"""
            self._notify(
                "task_ended",
                task_id,
                {
                    "timestamp": time.time(),
                },
            )

        @task_success.connect
        def on_task_success(sender=None, result=None, **kwargs):
            """Task succeeded"""
            task_id = kwargs.get("task_id")
            self._notify(
                "task_success",
                task_id,
                {
                    "result": result,
                    "timestamp": time.time(),
                },
            )

        @task_failure.connect
        def on_task_failure(sender=None, task_id=None, exception=None, **kwargs):
            """Task failed"""
            self._notify(
                "task_failure",
                task_id,
                {
                    "exception": str(exception),
                    "timestamp": time.time(),
                },
            )

        @task_revoked.connect
        def on_task_revoked(sender=None, request=None, terminated=None, **kwargs):
            """Task was revoked/cancelled"""
            task_id = request.id if request else None
            self._notify(
                "task_revoked",
                task_id,
                {
                    "terminated": terminated,
                    "timestamp": time.time(),
                },
            )

        @task_retry.connect
        def on_task_retry(sender=None, task_id=None, reason=None, **kwargs):
            """Task is being retried"""
            self._notify(
                "task_retry",
                task_id,
                {
                    "reason": str(reason),
                    "timestamp": time.time(),
                },
            )

        self._connected = True
        logger.info("Connected to Celery signals")

    def _notify(self, event_type: str, task_id: str, data: dict) -> None:
        """Notify registered callbacks"""
        callbacks = self._callbacks.get(event_type, [])
        for callback in callbacks:
            try:
                callback(task_id, data)
            except Exception as e:
                logger.error(f"Error in signal callback: {e}", exc_info=True)

    def on(self, event_type: str, callback: typing.Callable) -> None:
        """Register callback for a signal event"""
        if event_type not in self._callbacks:
            self._callbacks[event_type] = []
        self._callbacks[event_type].append(callback)


class CeleryTaskCommunicationBridge(ProcessTaskCommunicationBridge):

    def __init__(self, context: "ExecutionContext", celery_app: Celery):
        super().__init__(context)

        self.celery_app = celery_app

        # Track task metadata: volnux_task_id -> CeleryTaskMetadata
        self._tasks: typing.Dict[str, CeleryTaskMetadata] = {}

        # Signal monitor
        self._signal_monitor = CelerySignalMonitor(celery_app)
        self._signal_monitor.connect_signals()

        # Register signal handlers
        self._setup_signal_handlers()

        # Poll task for checking AsyncResult states
        self._poll_task: typing.Optional[asyncio.Task] = None

    def _setup_signal_handlers(self) -> None:
        """Setup handlers for Celery signals"""

        def on_started(celery_task_id: str, data: dict):
            """Handle task started signal"""
            metadata = self._find_metadata_by_celery_id(celery_task_id)
            if metadata:
                metadata.start_time = data["timestamp"]
                metadata.add_state("RUNNING")

                # Update cached status
                if metadata.task_id in self._statuses:
                    self._statuses[metadata.task_id].state = TaskState.RUNNING
                    self._statuses[metadata.task_id].last_heartbeat = time.time()

        def on_success(celery_task_id: str, data: dict):
            """Handle task success signal"""
            metadata = self._find_metadata_by_celery_id(celery_task_id)
            if metadata:
                metadata.add_state("SUCCESS")

                if metadata.task_id in self._statuses:
                    self._statuses[metadata.task_id].state = TaskState.COMPLETED
                    self._statuses[metadata.task_id].last_heartbeat = time.time()

        def on_failure(celery_task_id: str, data: dict):
            """Handle task failure signal"""
            metadata = self._find_metadata_by_celery_id(celery_task_id)
            if metadata:
                metadata.add_state("FAILURE")

                if metadata.task_id in self._statuses:
                    self._statuses[metadata.task_id].state = TaskState.FAILED
                    self._statuses[metadata.task_id].message = data.get("exception", "")
                    self._statuses[metadata.task_id].last_heartbeat = time.time()

        def on_revoked(celery_task_id: str, data: dict):
            """Handle task revoked signal"""
            metadata = self._find_metadata_by_celery_id(celery_task_id)
            if metadata:
                metadata.add_state("REVOKED")

                if metadata.task_id in self._statuses:
                    self._statuses[metadata.task_id].state = TaskState.CANCELLED
                    self._statuses[metadata.task_id].last_heartbeat = time.time()

        # Register handlers
        self._signal_monitor.on("task_started", on_started)
        self._signal_monitor.on("task_success", on_success)
        self._signal_monitor.on("task_failure", on_failure)
        self._signal_monitor.on("task_revoked", on_revoked)

    def _find_metadata_by_celery_id(
        self, celery_task_id: str
    ) -> typing.Optional[CeleryTaskMetadata]:
        """Find task metadata by Celery task ID"""
        for metadata in self._tasks.values():
            if metadata.celery_task_id == celery_task_id:
                return metadata
        return None

    async def register_task(self, handle: TaskExecutionHandle) -> None:
        """Register a Celery task for monitoring"""
        full_id = handle.full_id

        # Extract AsyncResult from handle
        async_result = handle.backend_ref
        if not isinstance(async_result, AsyncResult):
            logger.warning(f"Expected AsyncResult, got {type(async_result)}")
            return

        # Create metadata
        metadata = CeleryTaskMetadata(
            task_id=full_id,
            celery_task_id=async_result.id,
            async_result=async_result,
        )

        async with self._lock:
            self._execution_handles[full_id] = handle
            self._tasks[full_id] = metadata

            # Initialize status
            self._statuses[full_id] = TaskStatus(
                task_id=full_id,
                state=TaskState.QUEUED,
                metadata={
                    "task_def_id": handle.task_def_id,
                    "execution_id": handle.execution_id,
                    "event_name": handle.event_name,
                    "celery_task_id": async_result.id,
                },
            )

        logger.debug(
            f"Registered Celery task: {handle.event_name} "
            f"(volnux_id={full_id}, celery_id={async_result.id})"
        )

    async def send_command(self, task_id: str, command: TaskCommand) -> bool:
        """
        Send command to a Celery task.

        Only CANCEL is supported via Celery's control.revoke().
        Other commands would require custom task implementation.
        """
        handle = self.get_execution_handle(task_id)
        if not handle:
            logger.warning(f"Task {task_id} not found in Celery bridge")
            return False

        full_id = handle.full_id
        metadata = self._tasks.get(full_id)

        if not metadata:
            logger.warning(f"No metadata for task {full_id}")
            return False

        try:
            if command.command_type == CommandType.CANCEL:
                # Revoke task via Celery control
                self.celery_app.control.revoke(
                    metadata.celery_task_id, terminate=True, signal="SIGKILL"
                )
                logger.info(f"Revoked Celery task {metadata.celery_task_id}")
                return True

            else:
                logger.warning(
                    f"Command {command.command_type} not supported for Celery "
                    f"(only CANCEL via revoke)"
                )
                return False

        except Exception as e:
            logger.error(f"Error sending command to Celery task: {e}", exc_info=True)
            return False

    async def get_status(self, task_id: str) -> typing.Optional[TaskStatus]:
        """
        Get task status by polling AsyncResult.

        This is the decentralized approach - each coordinator
        polls its own tasks directly.
        """
        handle = self.get_execution_handle(task_id)
        if not handle:
            return None

        full_id = handle.full_id
        metadata = self._tasks.get(full_id)

        if not metadata:
            return self._statuses.get(full_id)

        # Poll AsyncResult for current state
        async_result = metadata.async_result
        celery_state = async_result.state

        # Map Celery state to TaskState
        state_mapping = {
            "PENDING": TaskState.QUEUED,
            "STARTED": TaskState.RUNNING,
            "SUCCESS": TaskState.COMPLETED,
            "FAILURE": TaskState.FAILED,
            "REVOKED": TaskState.CANCELLED,
            "RETRY": TaskState.RUNNING,
        }

        task_state = state_mapping.get(celery_state, TaskState.RUNNING)

        # Get metadata from result (if task updates it)
        info = async_result.info
        progress = 0.0
        message = ""

        if isinstance(info, dict):
            progress = info.get("progress", 0.0)
            message = info.get("message", "")
        elif celery_state == "FAILURE" and info:
            message = str(info)

        # Update cached status
        status = self._statuses.get(full_id)
        if status:
            status.state = task_state
            status.progress = progress
            status.message = message
            status.last_heartbeat = time.time()
        else:
            status = TaskStatus(
                task_id=full_id,
                state=task_state,
                progress=progress,
                message=message,
                last_heartbeat=time.time(),
            )
            self._statuses[full_id] = status

        return status

    async def is_alive(self, task_id: str, timeout: float = 30.0) -> bool:
        """Check if task is alive by checking AsyncResult state"""
        status = await self.get_status(task_id)
        if not status:
            return False

        # Task is alive if in non-terminal state
        return status.state in (TaskState.QUEUED, TaskState.RUNNING)

    async def start_monitoring(self) -> None:
        """Start periodic polling of AsyncResult states"""
        if not self._poll_task:
            self._poll_task = asyncio.create_task(self._poll_loop())

    async def _poll_loop(self) -> None:
        """Periodically poll all registered tasks"""
        while not self._shutdown_event.is_set():
            try:
                # Poll each registered task
                for task_id in list(self._tasks.keys()):
                    await self.get_status(task_id)

                # Poll every 1 second
                await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(f"Error in poll loop: {e}", exc_info=True)

    async def shutdown(self) -> None:
        """Shutdown monitoring"""
        self._shutdown_event.set()

        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

    def get_task_history(self, task_id: str) -> typing.List[typing.Tuple[float, str]]:
        """Get state transition history for a task"""
        handle = self.get_execution_handle(task_id)
        if not handle:
            return []

        metadata = self._tasks.get(handle.full_id)
        if metadata:
            return metadata.state_history
        return []
