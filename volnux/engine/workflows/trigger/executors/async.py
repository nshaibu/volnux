import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

from .base import BaseWorkflowConfigExecutor

if TYPE_CHECKING:
    from volnux.engine.workflows import WorkflowRegistry

logger = logging.getLogger(__name__)

__all__ = ["AsyncTaskWorkflowExecutor"]


@dataclass
class _TaskRecord:
    """Internal record for a running workflow task."""

    execution_id: str
    workflow_name: str
    task: asyncio.Task
    started_at: Union[float, int] = field(default_factory=lambda: time.monotonic())

    @property
    def task_id(self) -> str:
        return f"{self.workflow_name}-{self.execution_id}"


class AsyncTaskWorkflowExecutor(BaseWorkflowConfigExecutor):
    """
    Executes workflows as asyncio Tasks in the same event loop.

    Concurrency
    -----------
    ``max_concurrent`` limits the number of simultaneously running workflows.
    When the limit is reached:
        - ``execute()`` suspends the caller until a slot opens.
        - ``execute_background()`` schedules the workflow as a background Task
          that starts when a slot opens, and returns an execution ID
          immediately without blocking the caller.

    Triggers that want fire-and-forget semantics should use
    ``execute_background()``. Callers that need the workflow result should
    use ``execute()``.

    Timeout
    -------
    ``timeout`` applies at the WORKFLOW level, not the event level. A workflow
    that exceeds the timeout is cancelled — all running events within it
    receive CancelledError and checkpoint before stopping.

    Cancellation
    ------------
    Cancellation via ``cancel_workflow()`` awaits the task's completion so
    that callers receive control only after the workflow has fully stopped and
    its cleanup has run. This prevents the race where ``cancel_workflow()``
    returns but the task is still running.
    """

    def __init__(
        self,
        workflow_registry: "WorkflowRegistry",
        max_concurrent: int = 10,
        timeout: Optional[float] = None,
    ):
        """
        Initialise the async task executor.

        Args:
            workflow_registry:
                Registry of all available WorkflowConfig classes.
            max_concurrent:
                Maximum number of concurrently executing workflows.
                Additional calls to execute() suspend until a slot opens.
                Additional calls to execute_background() queue internally.
                Must be >= 1.
            timeout:
                Maximum workflow duration in seconds. The workflow is
                cancelled and WorkflowExecutionError is raised if exceeded.
                None means no timeout.
        """
        if max_concurrent < 1:
            raise ValueError(f"max_concurrent must be >= 1, got {max_concurrent}.")

        super().__init__(workflow_registry)
        self._max_concurrent = max_concurrent
        self._timeout = timeout
        self._semaphore = asyncio.Semaphore(max_concurrent)

        # Keyed by execution_id for O(1) cancellation by ID
        # Keyed separately by workflow_name for name-based lookup
        self._records_by_id: Dict[str, _TaskRecord] = {}
        self._ids_by_name: Dict[str, List[str]] = {}

        self._completed_count: int = 0
        self._failed_count: int = 0

    async def execute(
        self,
        workflow_name: str,
        params: Dict[str, Any],
    ) -> Any:
        """
        Execute a workflow and wait for its result.

        Suspends the caller until a concurrency slot is available, then
        runs the workflow and returns its result. Raises on failure or timeout.

        Args:
            workflow_name: Name of the workflow to execute.
            params:        Parameters passed to the workflow.

        Returns:
            The workflow's return value.

        Raises:
            WorkflowExecutionError: On failure, timeout, or cancellation.
            ValueError:             If workflow_name is not registered.
        """

        execution_id = self._new_execution_id()

        logger.info(
            "Workflow %r queued — execution_id=%s active=%d limit=%d",
            workflow_name,
            execution_id,
            self._active_count(),
            self._max_concurrent,
        )

        # Acquire a concurrency slot — suspends if max_concurrent are running
        async with self._semaphore:
            return await self._run(
                workflow_name=workflow_name,
                params=params,
                execution_id=execution_id,
                WorkflowError=WorkflowExecutionError,
            )

    async def execute_background(
        self,
        workflow_name: str,
        params: Dict[str, Any],
    ) -> str:
        """
        Schedule workflow execution as a background asyncio Task.

        Returns the execution_id immediately without waiting for the
        workflow to start or complete. The workflow runs when a concurrency
        slot becomes available.

        Use this from trigger engines that want fire-and-forget semantics.
        Use ``execute()`` when you need the workflow result.

        Args:
            workflow_name: Name of the workflow to execute.
            params:        Parameters passed to the workflow.

        Returns:
            execution_id: Unique ID for this execution.
                          Use with ``cancel_execution()`` or ``get_stats()``.
        """

        execution_id = self._new_execution_id()

        async def _background():
            async with self._semaphore:
                await self._run(
                    workflow_name=workflow_name,
                    params=params,
                    execution_id=execution_id,
                    WorkflowError=WorkflowExecutionError,
                )

        task = asyncio.create_task(
            _background(),
            name=f"{workflow_name}-{execution_id}",
        )
        record = _TaskRecord(
            execution_id=execution_id,
            workflow_name=workflow_name,
            task=task,
        )
        self._register(record)

        logger.info(
            "Workflow %r scheduled as background task — execution_id=%s",
            workflow_name,
            execution_id,
        )
        return execution_id

    async def cancel_execution(self, execution_id: str) -> bool:
        """
        Cancel a specific workflow execution by its execution ID.

        Awaits the task's cancellation so the caller knows the workflow
        has fully stopped before this method returns.

        Args:
            execution_id: The execution ID returned by execute_background()
                          or from get_stats().

        Returns:
            True if the task was found and cancellation was requested.
            False if no matching execution was found.
        """
        record = self._records_by_id.get(execution_id)
        if record is None:
            logger.warning(
                "cancel_execution: no running execution with id=%s", execution_id
            )
            return False

        return await self._cancel_record(record, reason="cancel_execution called")

    async def cancel_workflow(self, workflow_name: str) -> int:
        """
        Cancel all running executions of a given workflow.

        Awaits all cancellations before returning.

        Args:
            workflow_name: Name of the workflow to cancel.

        Returns:
            Number of executions that were cancelled.
        """
        ids = list(self._ids_by_name.get(workflow_name, []))
        if not ids:
            logger.warning(
                "cancel_workflow: no running executions for %r", workflow_name
            )
            return 0

        results = await asyncio.gather(
            *[self.cancel_execution(eid) for eid in ids],
            return_exceptions=True,
        )
        cancelled = sum(1 for r in results if r is True)
        logger.info(
            "cancel_workflow %r — cancelled %d of %d execution(s)",
            workflow_name,
            cancelled,
            len(ids),
        )
        return cancelled

    async def cancel_all(self) -> int:
        """
        Cancel all currently running workflow executions.

        Awaits all cancellations before returning.

        Returns:
            Number of executions cancelled.
        """
        ids = list(self._records_by_id.keys())
        if not ids:
            return 0

        results = await asyncio.gather(
            *[self.cancel_execution(eid) for eid in ids],
            return_exceptions=True,
        )
        cancelled = sum(1 for r in results if r is True)
        logger.info("cancel_all — cancelled %d execution(s)", cancelled)
        return cancelled

    def get_stats(self) -> Dict[str, Any]:
        """
        Return a snapshot of executor state for health monitoring.

        Returns:
            Dict with active task count, per-task details, and cumulative
            completed/failed counters.
        """
        active = []
        for record in list(self._records_by_id.values()):
            task = record.task
            exc = None
            is_done = task.done()
            cancelled = task.cancelled()

            if is_done and not cancelled:
                try:
                    raw_exc = task.exception()
                    exc = str(raw_exc) if raw_exc is not None else None
                except Exception:
                    exc = "unknown"

            active.append(
                {
                    "execution_id": record.execution_id,
                    "workflow_name": record.workflow_name,
                    "task_id": record.task_id,
                    "running_for_s": round(time.monotonic() - record.started_at, 2),
                    "done": is_done,
                    "cancelled": cancelled,
                    "error": exc,
                }
            )

        return {
            "active_count": len(active),
            "max_concurrent": self._max_concurrent,
            "timeout": self._timeout,
            "completed": self._completed_count,
            "failed": self._failed_count,
            "active_tasks": active,
        }

    def available_slots(self) -> int:
        """
        Return the number of concurrency slots currently available.

        This is a point-in-time value — it may be stale by the time
        the caller acts on it. Use for informational/observability purposes
        only, not for flow control.
        """
        # asyncio.Semaphore exposes its internal counter via _value
        # This is a private attribute but stable across CPython versions.
        # The alternative — tracking _active_count ourselves — is cleaner:
        return max(0, self._max_concurrent - self._active_count())

    async def _run(
        self,
        workflow_name: str,
        params: Dict[str, Any],
        execution_id: str,
        WorkflowError: type,
    ) -> Any:
        """
        Core execution path. Caller must already hold the semaphore.

        Creates the asyncio Task, registers it for tracking, applies the
        timeout, awaits completion, and handles cleanup.
        """
        task_name = f"{workflow_name}-{execution_id}"

        coro = self._run_workflow(
            workflow_name=workflow_name,
            params=params,
            execution_id=execution_id,
        )
        task = asyncio.create_task(coro, name=task_name)
        record = _TaskRecord(
            execution_id=execution_id,
            workflow_name=workflow_name,
            task=task,
        )
        self._register(record)

        logger.info(
            "Workflow %r started — execution_id=%s",
            workflow_name,
            execution_id,
        )

        try:
            if self._timeout is not None:
                # Apply timeout to the await, not to the coroutine.
                # This correctly cancels the task on timeout and lets
                # the task's finally/cleanup blocks run.
                result = await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=self._timeout,
                )
            else:
                result = await task

            self._completed_count += 1
            logger.info(
                "Workflow %r completed — execution_id=%s",
                workflow_name,
                execution_id,
            )
            return result

        except asyncio.TimeoutError:
            # Timeout expired — cancel the shielded task explicitly
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            self._failed_count += 1
            raise WorkflowError(
                f"Workflow {workflow_name!r} timed out after {self._timeout}s "
                f"(execution_id={execution_id})"
            )

        except asyncio.CancelledError:
            # Workflow was cancelled from outside (cancel_execution, cancel_all)
            self._failed_count += 1
            raise WorkflowError(
                f"Workflow {workflow_name!r} was cancelled "
                f"(execution_id={execution_id})"
            )

        except WorkflowError:
            self._failed_count += 1
            raise

        except Exception as exc:
            self._failed_count += 1
            logger.error(
                "Workflow %r failed — execution_id=%s: %s",
                workflow_name,
                execution_id,
                exc,
                exc_info=True,
            )
            raise WorkflowError(
                f"Workflow {workflow_name!r} failed: {exc} "
                f"(execution_id={execution_id})"
            ) from exc

        finally:
            self._deregister(record)

    async def _run_workflow(
        self,
        workflow_name: str,
        params: Dict[str, Any],
        execution_id: str,
    ) -> Any:
        """
        Resolve the WorkflowConfig and delegate to run_workflow_async().

        Separated from _run() so subclasses can override only the dispatch
        logic without duplicating the task lifecycle boilerplate.
        """
        config = self._workflow_registry.get_workflow_config(workflow_name)
        if config is None:
            raise ValueError(
                f"Workflow {workflow_name!r} is not registered. "
                "Ensure it is declared in WorkflowConfig.ready()."
            )

        logger.debug(
            "Dispatching workflow %r to config %r — execution_id=%s",
            workflow_name,
            type(config).__name__,
            execution_id,
        )

        return await config.run_workflow_async(
            params=params,
            run_type="single",
        )

    async def _cancel_record(
        self,
        record: _TaskRecord,
        reason: str,
    ) -> bool:
        """
        Cancel a TaskRecord's task and await its completion.

        Returns True if cancellation was requested (task was not already done).
        """
        task = record.task
        if task.done():
            logger.debug("cancel: task %s is already done — skipping", record.task_id)
            return False

        task.cancel()
        logger.info(
            "Cancellation requested for %r (execution_id=%s reason=%s)",
            record.workflow_name,
            record.execution_id,
            reason,
        )

        # Await the task so callers know cleanup has completed before returning.
        # CancelledError from the task is expected and swallowed here.
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=30.0)
        except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
            pass

        return True

    # ── Task registry ─────────────────────────────────────────────────────────

    def _register(self, record: _TaskRecord) -> None:
        """Register a TaskRecord for tracking. Idempotent by execution_id."""
        self._records_by_id[record.execution_id] = record
        self._ids_by_name.setdefault(record.workflow_name, [])
        if record.execution_id not in self._ids_by_name[record.workflow_name]:
            self._ids_by_name[record.workflow_name].append(record.execution_id)

    def _deregister(self, record: _TaskRecord) -> None:
        """Remove a TaskRecord from all tracking structures."""
        self._records_by_id.pop(record.execution_id, None)
        id_list = self._ids_by_name.get(record.workflow_name, [])
        try:
            id_list.remove(record.execution_id)
        except ValueError:
            pass
        if not id_list:
            self._ids_by_name.pop(record.workflow_name, None)

    def _active_count(self) -> int:
        return len(self._records_by_id)

    @staticmethod
    def _new_execution_id() -> str:
        """Generate a time-sortable unique execution ID."""
        timestamp = int(time.time() * 1000)
        short_uid = uuid.uuid4().hex[:8]
        return f"{timestamp}-{short_uid}"
