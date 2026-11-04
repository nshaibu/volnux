import asyncio
import logging
import time
import typing
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

from volnux.exceptions import (
    TaskCancelledError,
    SwitchTask,
    # ExternalCommunicationSuspensionRequest,
    StopProcessingError,
    SuspendTask,
)
from volnux.execution.context import ExecutionContext, ExecutionStatus
from volnux.execution.result import ResultProcessor
from volnux.flows import setup_execution_flow
from volnux.mixins.event.communication.datastructures import (
    ExternalCommunicationQueueEntry,
)

# NEW: Import the sub-phase suspension signal
# from volnux.mixins.event.checkpointing import SubPhaseSuspension

if typing.TYPE_CHECKING:
    from volnux.flows.base import BaseFlow

logger = logging.getLogger(__name__)


# @dataclass
# class ExternalCommunicationQueueEntry:
#     request_id: str
#     workflow_name: str
#     workflow_id: str
#     task_id: str
#     sub_phase_name: Optional[str] = None  # NEW: None for legacy communicate()
#     checkpoint_key: Optional[str] = None
#     request_title: Optional[str] = None
#     request_payload: Any = None
#     options: Optional[Dict[str, Any]] = None
#     timeout_at: Optional[str] = None


class ExecutionError(Exception):
    """Base exception for execution failures."""

    pass


class ExecutionTimeoutError(ExecutionError):
    """Exception raised when execution exceeds timeout."""

    pass


class ExecutionCoordinator:
    """
    Coordinates execution of tasks based on task hierarchy.

    Manages the lifecycle of task execution including setup, running,
    error handling, suspension (HITL + sub_phase), and cleanup operations.
    """

    def __init__(
        self,
        execution_context: ExecutionContext,
        result_processor: Optional[ResultProcessor] = None,
        timeout: Optional[float] = None,
    ):
        self.execution_context = execution_context
        self._result_processor = result_processor or ResultProcessor()
        self._timeout = timeout
        self._flow = None

    def _setup_execution_flow(self) -> "BaseFlow":
        try:
            logger.info("Setting up execution flow")
            flow = setup_execution_flow(self.execution_context)
            logger.debug(f"Execution flow configured: {flow}")
            return flow
        except Exception as e:
            logger.error(f"Failed to setup execution flow: {e}", exc_info=True)
            raise ValueError(f"Invalid execution context: {e}") from e

    async def _process_suspension_request(self, request: SuspendTask):
        if request.suspension_type == SuspendTask.SuspensionType.PREEMPTION:
            # goes to waiting queue
            pass
        elif request.suspension_type == SuspendTask.SuspensionType.CANCELLATION:
            # Goes to dead letter queue
            pass
        elif request.suspension_type in [
            SuspendTask.SuspensionType.HITL,
            SuspendTask.SuspensionType.EXTERNAL_EVENT,
            SuspendTask.SuspensionType.CONDITION,
        ]:
            # Goes to external event waiting queue. When the event is received, the tasks is place in the waiting queue baased on piroity
            pass
        else:
            raise ValueError(f"Invalid suspension type: {request.suspension_type}")

    async def _run_flow(self, flow: "BaseFlow") -> Tuple[Any, Any]:
        """Submit the flow and wait for its tasks to finish.

        ``flow.run()`` returns as soon as the task is *submitted*; the work is
        only done once the returned future resolves, so both steps belong
        inside the timeout.
        """
        future = await flow.run()
        logger.info("Task submitted, waiting for results")
        return await self._result_processor.process_futures([future])

    async def _stop_flow(self, flow: Optional["BaseFlow"]) -> None:
        """Best effort: tell the running tasks to stop; never raises."""
        if flow is None:
            return
        try:
            await flow.cancel()
        except Exception:
            logger.warning("Failed to cancel the execution flow", exc_info=True)

    async def _execute_async(self) -> Tuple[Any, Any]:
        flow: Optional["BaseFlow"] = None
        self._flow = None
        results = None
        errors = None

        try:
            flow = self._setup_execution_flow()
            self._flow = flow
            # Lets ExecutionContext.cancel_async()/pause_tasks() reach this flow
            self.execution_context._active_flow = flow

            await self.execution_context.update_status_async(ExecutionStatus.RUNNING)
            logger.info("Starting task execution")

            if self._timeout:
                results, errors = await asyncio.wait_for(
                    self._run_flow(flow), timeout=self._timeout
                )
            else:
                results, errors = await self._run_flow(flow)

            logger.info("Task execution completed, processing results")

            # Update status based on results
            if errors:
                logger.warning(f"Execution completed with {len(errors)} error(s)")
            else:
                logger.info("Execution completed successfully")

            error_results = await self._result_processor.process_errors(errors)
            results.extend(error_results)

            cancelled = any(isinstance(e, TaskCancelledError) for e in errors)
            await self.execution_context.bulk_update_async(
                ExecutionStatus.CANCELLED if cancelled else ExecutionStatus.COMPLETED,
                errors,
                results,
            )
            self.execution_context.metrics.end_time = time.time()

            if cancelled:
                logger.info("Execution cancelled")
                # Tasks are already stopped: emit the signal, send no more commands
                await self.execution_context.cancel_async(propagate=False)
                return results, errors

            stop_processing_requested = (
                self.execution_context.get_stop_processing_request()
            )
            if stop_processing_requested:
                raise stop_processing_requested

            suspension_request = self.execution_context.get_suspension_request()
            if suspension_request:
                raise suspension_request

            switch_request = typing.cast(
                SwitchTask, self.execution_context.get_switch_request()
            )
            if switch_request is not None:
                results.add(switch_request.result)
                current_task_profile = (
                    self.execution_context.get_decision_task_profile()
                )
                if current_task_profile is not None:
                    if not current_task_profile.get_descriptor(
                        switch_request.next_task_descriptor
                    ):
                        logger.error(
                            f"Task profile has no configured descriptor "
                            f"{switch_request.next_task_descriptor}"
                        )
                        # the tasks have finished: nothing to send commands to
                        await self.execution_context.cancel_async(propagate=False)
                        switch_request.descriptor_configured = False
                    else:
                        switch_request.descriptor_configured = True
                else:
                    logger.warning(
                        "No decision task profile found for switch task handling"
                    )

            return results, errors

        except SuspendTask as sps:
            logger.info(
                "Coordinator: workflow '%s' suspended at phase '%s'",
                self.execution_context.workflow_name,
                sps.get_phase(),
            )

            await self._process_suspension_request(sps)

            await self.execution_context.paused_async()
            return results, errors

        except asyncio.TimeoutError as e:
            logger.error(f"Execution exceeded timeout of {self._timeout}s")
            # wait_for cancelled the wait, not the tasks behind it
            await self._stop_flow(flow)
            await self.execution_context.failed_async()
            raise ExecutionTimeoutError(
                f"Task execution timed out after {self._timeout}s"
            ) from e

        # Must precede the RuntimeError handler below: StopProcessingError is a
        # RuntimeError, and a requested stop is a controlled cancellation, not a
        # failure.
        except StopProcessingError as e:
            logger.info(f"Execution stopped due to stop condition: {e}")
            await self.execution_context.cancel_async(propagate=False)
            return results, errors

        except asyncio.CancelledError:
            # The coordinator itself was cancelled (e.g. the run was torn
            # down): don't leave the tasks running behind it.
            logger.info("Execution coordinator cancelled")
            await self._stop_flow(flow)
            try:
                await self.execution_context.cancel_async(propagate=False)
            except Exception:
                logger.warning("Failed to mark the context cancelled", exc_info=True)
            raise

        except (RuntimeError, ValueError) as e:
            logger.error(
                f"Execution failed with {type(e).__name__}: {e}", exc_info=True
            )
            await self.execution_context.failed_async()
            raise ExecutionError(f"Task execution failed: {e}") from e

        except Exception as e:
            logger.error(f"Unexpected execution error: {e}", exc_info=True)
            await self.execution_context.failed_async()
            raise

        finally:
            self.execution_context._active_flow = None
            self._flow = None
            if flow is not None:
                try:
                    await flow.close()
                except Exception:
                    # never let cleanup replace the real outcome
                    logger.warning("Failed to close the execution flow", exc_info=True)

        return results, errors

    async def _get_latest_checkpoint_key(self, task_id: str) -> typing.Optional[str]:
        logger.debug(
            "Persisting checkpoint for suspended task '%s' before suspension wait",
            task_id,
        )
        await self.execution_context.save()
        return self.execution_context.state_id

    def _compute_timeout(
        self, timeout_hours: typing.Optional[float]
    ) -> typing.Optional[str]:
        if not timeout_hours:
            return None
        return (datetime.now(timezone.utc) + timedelta(hours=timeout_hours)).isoformat()

    async def execute_async(self) -> Tuple[Any, Any]:
        return await self._execute_async()

    async def cancel(self) -> None:
        flow = self._flow
        if flow is not None:
            logger.warning("Cancelling execution flow")
            await flow.cancel()
            await self.execution_context.cancel_async(propagate=False)

    def __repr__(self) -> str:
        return (
            f"ExecutionCoordinator("
            f"context={self.execution_context}, "
            f"timeout={self._timeout})"
        )
