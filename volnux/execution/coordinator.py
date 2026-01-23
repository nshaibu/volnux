import asyncio
import logging
import time
import typing
from typing import Any, Optional, Tuple

from volnux.exceptions import SwitchTask
from volnux.execution.context import ExecutionContext
from volnux.execution.result import ResultProcessor
from volnux.execution.state_manager import ExecutionStatus
from volnux.flows import setup_execution_flow

if typing.TYPE_CHECKING:
    from volnux.flows.base import BaseFlow

logger = logging.getLogger(__name__)


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
    error handling, and cleanup operations.
    """

    def __init__(
        self,
        execution_context: ExecutionContext,
        result_processor: Optional[ResultProcessor] = None,
        timeout: Optional[float] = None,
    ):
        """
        Initialize the ExecutionCoordinator.

        Args:
            execution_context: The execution context containing task configuration
            result_processor: Custom result processor (creates default if None)
            timeout: Optional timeout in seconds for execution
        """
        self.execution_context = execution_context
        self._result_processor = result_processor or ResultProcessor()
        self._timeout = timeout
        self._flow = None

    def _setup_execution_flow(self) -> "BaseFlow":
        """
        Setup the execution flow based on task dependencies.

        Returns:
            Configured execution flow ready for running

        Raises:
            ValueError: If execution context is invalid
        """
        try:
            logger.info("Setting up execution flow")
            flow = setup_execution_flow(self.execution_context)
            logger.debug(f"Execution flow configured: {flow}")
            return flow
        except Exception as e:
            logger.error(f"Failed to setup execution flow: {e}", exc_info=True)
            raise ValueError(f"Invalid execution context: {e}") from e

    async def _execute_async(self) -> Tuple[Any, Any]:
        """
        Execute the tasks asynchronously.

        Returns:
            Tuple of (results, errors) from task execution

        Raises:
            ExecutionTimeoutError: If execution exceeds timeout
            ExecutionError: If execution fails due to runtime errors
            Exception: If execution fails critically
        """
        flow = self._setup_execution_flow()
        self._flow = flow

        try:
            await self.execution_context.update_status_async(ExecutionStatus.RUNNING)
            logger.info("Starting task execution")

            # Run with optional timeout - if timeout set
            run_coro = flow.run()
            future = (
                await asyncio.wait_for(run_coro, timeout=self._timeout)
                if self._timeout
                else await run_coro
            )

            logger.info("Task execution completed, processing results")
            results, errors = await self._result_processor.process_futures([future])

            # Update status based on results
            if errors:
                logger.warning(f"Execution completed with {len(errors)} error(s)")
            else:
                logger.info("Execution completed successfully")

            error_results = await self._result_processor.process_errors(errors)

            results.extend(error_results)

            await self.execution_context.bulk_update_async(
                ExecutionStatus.COMPLETED, errors, results
            )
            self.execution_context.metrics.end_time = time.time()

            # check if stop processing request was raised
            execution_state = await self.execution_context.state_async
            stop_processing_requested = execution_state.get_stop_processing_request()
            if stop_processing_requested:
                logger.info(
                    f"Execution stopped due to stop condition: {stop_processing_requested}"
                )
                await self.execution_context.cancel_async()

            if stop_processing_requested is None:
                # check for switch task request
                switch_request = execution_state.get_switch_request()

                if typing.TYPE_CHECKING:
                    switch_request = typing.cast(SwitchTask, switch_request)

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
                                f"Task profile has no configured descriptor {switch_request.next_task_descriptor}"
                            )
                            await self.execution_context.cancel_async()
                            switch_request.descriptor_configured = False
                        else:
                            switch_request.descriptor_configured = True
                    else:
                        logger.warning(
                            "No decision task profile found for switch task handling"
                        )

            return results, errors

        except asyncio.TimeoutError as e:
            logger.error(f"Execution exceeded timeout of {self._timeout}s")
            await self.execution_context.failed_async()
            raise ExecutionTimeoutError(
                f"Task execution timed out after {self._timeout}s"
            ) from e

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

    async def execute_async(self) -> Tuple[Any, Any]:
        """
        Execute tasks asynchronously when already in an async context.

        Use this method when calling from async code instead of execute().

        Returns:
            Tuple of (results, errors) from task execution

        Raises:
            RuntimeError: If called from within an existing event loop
            Exception: If execution fails
        """
        return await self._execute_async()

    async def cancel(self) -> None:
        """Cancel the currently running execution."""
        if self._flow:
            logger.warning("Cancelling execution flow")
            await self._flow.cancel()
            self.execution_context.update_status(ExecutionStatus.CANCELLED)

    def __repr__(self) -> str:
        return (
            f"ExecutionCoordinator("
            f"context={self.execution_context}, "
            f"timeout={self._timeout})"
        )
