import logging
import asyncio
import inspect
from typing import Any, Dict, Optional
from volnux.flows.bridge.communications.tasks import (
    CommandType,
    MessageType,
    TaskMessage,
    TaskCommand,
    TaskState,
)
from ..protocols.event import BaseEvent
from ...result import EventResult
from volnux.exceptions import SuspendTask
from volnux.concurrency.async_utils import to_thread

logger = logging.getLogger(__name__)


class EventCommandMixin:
    """
    Mixin for handling commands from the coordinator.

    This class provides functionality to process and handle commands such as
    PAUSE, RESUME, CANCEL, CHECKPOINT, QUERY_STATUS, and UPDATE_PRIORITY
    received from a coordinator. It ensures that tasks can be controlled
    dynamically during execution based on incoming commands.

    Expected attributes from the mixing class:
        _command_channel: Optional channel for command communication
        _pause_gate: asyncio.Event used to control pause/resume state
        _task_id: str - Unique identifier for the task
        _main_worker_task: asyncio.Task - The main worker task reference
        _preempted: bool - Flag indicating if task was preempted for priority change
        _command_listener_task: Optional asyncio.Task - Background command listener task

    The command listener runs as a background task, continuously polling for
    commands from the coordinator while the main worker executes steps.
    """

    # Type hints for expected attributes (will be set by mixing class)
    _command_channel: Optional[Any]
    _pause_gate: asyncio.Event
    _task_id: str
    _preempted: bool
    _main_worker_task: asyncio.Task
    _command_listener_task: Optional[asyncio.Task]

    async def _send_update(
        self,
        msg_type: MessageType,
        state: TaskState,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Send a standardized status update message to the coordinator.

        Args:
            msg_type: The type of message being sent
            state: The current state of the task
            extra: Optional additional payload data

        Note:
            Errors during message sending are logged but do not interrupt execution.
        """
        if not self._command_channel:
            logger.debug(f"No command channel available for task {self._task_id}")
            return

        payload = {"state": state.value}
        if extra:
            payload.update(extra)

        try:
            await self._command_channel.send_message(
                TaskMessage(
                    task_id=self._task_id,
                    message_type=msg_type,
                    payload=payload,
                )
            )
            logger.debug(
                f"Sent {msg_type.value} update for task {self._task_id}: {state.value}"
            )
        except Exception as e:
            logger.error(
                f"Failed to send {msg_type.value} update for task {self._task_id}: {e}",
                exc_info=True,
            )

    async def _command_listener(self: BaseEvent) -> None:
        """
        Continuously listen for and process incoming commands from the coordinator.

        This method runs as a background task, polling the command channel with
        a short timeout. It delegates command handling to _handle_command.

        The loop continues until the task is cancelled, at which point it exits
        gracefully.

        Exceptions are handled gracefully:
        - TimeoutError: Expected when no command is available
        - CancelledError: Re-raised to allow proper task cancellation
        - Other exceptions: Logged but don't stop the listener
        """
        if not self._command_channel:
            logger.debug(
                f"No command channel for task {self._task_id}, listener exiting"
            )
            return

        logger.debug(f"Command listener started for task {self._task_id}")

        try:
            while True:
                try:
                    command = await self._command_channel.receive_command(timeout=0.01)
                    if command:
                        await self._handle_command(command)
                except asyncio.TimeoutError:
                    # Expected - no command available within timeout; the
                    # timeout itself paces the loop, no extra sleep needed.
                    pass
                except asyncio.CancelledError:
                    # Don't catch cancellation - let it propagate
                    raise
                except Exception as e:
                    logger.error(
                        f"Error in command listener for task {self._task_id}: {e}",
                        exc_info=True,
                    )

        except asyncio.CancelledError:
            logger.debug(f"Command listener for task {self._task_id} cancelled")
            raise

    async def _handle_command(self: BaseEvent, command: TaskCommand) -> None:
        """
        Handle incoming commands from the coordinator.

        Supported commands:
        - PAUSE: Clears the pause gate to halt task execution at the next checkpoint
        - RESUME: Sets the pause gate to allow task execution to continue
        - CANCEL: Cancels the main worker task
        - UPDATE_PRIORITY: Marks a task as preempted and cancels for re-queuing
        - CHECKPOINT: Triggers an immediate checkpoint save
        - QUERY_STATUS: Reports current task state to coordinator

        Args:
            command: The command to process

        Note:
            The main worker task (step_runner) is responsible for checking
            _pause_gate at the beginning of each step iteration.
        """
        try:
            if command.command_type == CommandType.PAUSE:
                self._pause_gate.clear()
                logger.info(f"Task {self._task_id} received PAUSE command")
                await self._send_update(MessageType.STATUS_UPDATE, TaskState.PAUSED)

            elif command.command_type == CommandType.RESUME:
                self._pause_gate.set()
                logger.info(f"Task {self._task_id} received RESUME command")
                await self._send_update(MessageType.STATUS_UPDATE, TaskState.RUNNING)

            elif command.command_type == CommandType.CANCEL:
                logger.warning(f"Task {self._task_id} received CANCEL command")
                # The authoritative CANCELLED status update is sent by
                # steps_runner once the worker task actually stops.
                self._main_worker_task.cancel()

            elif command.command_type == CommandType.UPDATE_PRIORITY:
                new_priority = (
                    command.payload.get("priority") if command.payload else None
                )
                logger.info(
                    f"Task {self._task_id} received UPDATE_PRIORITY command "
                    f"(new priority: {new_priority})"
                )
                self._preempted = True
                await self._send_update(
                    MessageType.STATUS_UPDATE,
                    TaskState.RUNNING,
                    extra={"priority": new_priority, "preempted": True},
                )
                self._main_worker_task.cancel()

            elif command.command_type == CommandType.CHECKPOINT:
                logger.info(f"Task {self._task_id} received CHECKPOINT command")
                await self.enqueue_checkpoint()
                await self._send_update(
                    MessageType.STATUS_UPDATE,
                    TaskState.RUNNING,
                    extra={"checkpoint_saved": True},
                )

            elif command.command_type == CommandType.QUERY_STATUS:
                logger.debug(f"Task {self._task_id} received QUERY_STATUS command")
                state = (
                    TaskState.PAUSED
                    if not self._pause_gate.is_set()
                    else TaskState.RUNNING
                )
                await self._send_update(MessageType.STATUS_UPDATE, state)

            else:
                logger.warning(
                    f"Task {self._task_id} received unknown command type: "
                    f"{command.command_type}"
                )

        except asyncio.CancelledError:
            # Don't catch cancellation - let it propagate
            raise
        except Exception as e:
            logger.error(
                f"Error handling command {command.command_type} for task {self._task_id}: {e}",
                exc_info=True,
            )

    async def _run_step(self, step, *args, **kwargs):
        """
        Executes a single step of a process, supporting both synchronous and asynchronous steps.
        Provides detailed logging for debugging purposes, including the step name and associated
        task identifier.

        :param step: Step function to be executed. Can be a coroutine or a regular
            synchronous function.
        :type step: Callable
        :param args: Positional arguments to pass to the step function.
        :param kwargs: Keyword arguments to pass to the step function.
        :return: The result of the executed step function.
        :rtype: Any
        """
        step_name = step.__name__
        logger.debug("Running step %s for task_id=%s", step_name, self._task_id)

        if inspect.iscoroutinefunction(step):
            return await step(*args, **kwargs)
        return await to_thread(step, *args, **kwargs)

    async def steps_runner(self: BaseEvent, *args, **kwargs) -> "EventResult":
        """
        Executes a series of steps for the current task in a checkpointed manner.

        Handles pause/resume via _pause_gate, non-blocking checkpoints via queue,
        phase-based step skipping, and proper error handling.

        :param args: Positional arguments passed to individual steps
        :type args: tuple
        :param kwargs: Keyword arguments passed to individual steps
        :type kwargs: dict
        :return: The result produced by the final step
        :rtype: EventResult
        :raises SuspendTask: When a task is preempted for re-queueing
        :raises asyncio.CancelledError: When task is cancelled
        :raises Exception: Any error during step execution
        """
        logger.debug(
            "Starting checkpointed execution for task_id=%s at phase=%s",
            self._task_id,
            getattr(self._phase, "name", None),
        )

        current_index = 0
        steps = list(self._get_steps())
        total_steps = len(steps)
        error_reported = False

        try:
            await self._send_update(MessageType.STATUS_UPDATE, TaskState.RUNNING)

            for current_index, step in enumerate(steps):
                step_phase = getattr(step, "_phase", None)

                # Skip already-completed phases
                if (
                    self._phase is not None
                    and step_phase is not None
                    and step_phase <= self._phase
                ):
                    logger.debug(
                        "Skipping step %s for task_id=%s (phase %s already complete)",
                        step.__name__,
                        self._task_id,
                        step_phase.name,
                    )
                    continue

                # Blocks here until RESUME sets the gate; returns immediately if already set
                await self._pause_gate.wait()

                # Execute the step
                try:
                    logger.debug(
                        f"Executing step {current_index + 1}/{total_steps}: {step.__name__}"
                    )
                    await self._run_step(step, *args, **kwargs)

                except Exception as e:
                    logger.exception(
                        "Step %s failed for task_id=%s; checkpoint preserved at phase=%s",
                        step.__name__,
                        self._task_id,
                        getattr(self._phase, "name", None),
                    )
                    await self._send_update(
                        MessageType.ERROR,
                        TaskState.FAILED,
                        {
                            "error": str(e),
                            "step": current_index + 1,
                            "total": total_steps,
                            "step_name": step.__name__,
                        },
                    )
                    error_reported = True
                    raise

                # Non-blocking checkpoint after a successful step
                await self.enqueue_checkpoint()

                # Send progress update
                await self._send_update(
                    MessageType.PROGRESS_UPDATE,
                    TaskState.RUNNING,
                    {
                        "step": current_index + 1,
                        "total": total_steps,
                        "step_name": step.__name__,
                        "phase": getattr(self._phase, "name", None),
                    },
                )

                logger.debug(
                    "Checkpoint enqueued for task_id=%s after step %s; phase=%s",
                    self._task_id,
                    step.__name__,
                    getattr(self._phase, "name", None),
                )

            # Final checkpoint for completion
            await self.enqueue_checkpoint()
            await self._send_update(MessageType.STATUS_UPDATE, TaskState.COMPLETED)

            logger.info(
                f"Task {self._task_id} completed all {total_steps} steps successfully"
            )

        except asyncio.CancelledError:
            if self._preempted:
                # Preemption: Save the current state for re-queueing
                logger.info(
                    f"Task {self._task_id} preempted at step {current_index + 1}/{total_steps}"
                )

                await self.enqueue_checkpoint()

                await self._send_update(MessageType.STATUS_UPDATE, TaskState.QUEUED)
                # Consume the flag now so a later hard CANCEL on this
                # instance isn't mistaken for another preemption.
                self._preempted = False
                raise SuspendTask(
                    self, suspension_type=SuspendTask.SuspensionType.PREEMPTION
                )

            # Hard cancellation
            logger.warning(
                f"Task {self._task_id} cancelled at step {current_index + 1}/{total_steps}"
            )
            await self._send_update(MessageType.STATUS_UPDATE, TaskState.CANCELLED)
            raise

        except Exception as e:
            logger.error(
                f"Task {self._task_id} failed with unexpected error at step "
                f"{current_index + 1}/{total_steps}: {e}",
                exc_info=True,
            )
            # Avoid double-reporting a failure already sent by the
            # per-step error handler above.
            if not error_reported:
                await self._send_update(
                    MessageType.ERROR,
                    TaskState.FAILED,
                    {
                        "error": str(e),
                        "step": current_index + 1,
                        "total": total_steps,
                    },
                )
            raise

        # construct event result from the exec_status and exec_result attributes
        result = (
            self.on_success(self.exec_result)
            if self.exec_status
            else self.on_failure(self.exec_result)
        )
        return result
