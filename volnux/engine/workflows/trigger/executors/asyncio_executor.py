import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Union, cast, TYPE_CHECKING

from volnux.config import setting
from volnux.context import get_current_correlation_id, volnux_context
from volnux.engine.workflows.workflow import WorkflowExecutionError

from .base import WorkflowExecutor

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


class AsyncTaskWorkflowExecutor(WorkflowExecutor):
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

    Context
    -------
    Each workflow runs under a ``volnux.context`` of its own (workflow id,
    execution id, correlation id) on top of the context of the caller, so the
    workflow sees the configuration that was active when it was started and
    concurrent workflows do not see each other's identifiers.

    Cancellation
    ------------
    Cancellation via ``cancel_workflow()`` awaits the task's completion so
    that callers receive control only after the workflow has fully stopped and
    its cleanup has run. This prevents the race where ``cancel_workflow()``
    returns but the task is still running.
    """

    supports_control = True

    def __init__(
        self,
        workflow_registry: "WorkflowRegistry",
        max_concurrent: Optional[int] = None,
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
                Must be >= 1. Defaults to the ``ASYNC_EXECUTOR_MAX_CONCURRENT``
                setting.
            timeout:
                Maximum workflow duration in seconds. The workflow is
                cancelled and WorkflowExecutionError is raised if exceeded.
                None means the ``ASYNC_EXECUTOR_TIMEOUT`` setting (no timeout
                unless it is set).
        """
        if max_concurrent is None:
            max_concurrent = int(setting("ASYNC_EXECUTOR_MAX_CONCURRENT", 10))
        if timeout is None:
            configured = setting("ASYNC_EXECUTOR_TIMEOUT", None)
            timeout = float(configured) if configured is not None else None
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

        # Runs started with an execution id from the caller (the platform's
        # Execution record): their live pipelines, and which of them were stopped.
        self._external_ids: set = set()
        self._pipelines: Dict[str, Any] = {}
        self._stop_requested: set = set()

    async def execute(
        self,
        workflow_name: str,
        params: Dict[str, Any],
        *,
        execution_id: Optional[str] = None,
    ) -> Any:
        """
        Execute a workflow and wait for its result.

        Suspends the caller until a concurrency slot is available, then
        runs the workflow and returns its result. Raises on failure or timeout.

        Args:
            workflow_name: Name of the workflow to execute.
            params:        Parameters passed to the workflow.
            execution_id:  The caller's id for this run. It becomes the run's id
                           (``pause_execution`` and the other controls take it) and
                           the listeners are told when the run ends.

        Returns:
            The workflow's return value.

        Raises:
            WorkflowExecutionError: On failure, timeout, or cancellation.
        """

        execution_id = self._claim_execution_id(execution_id)

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
        # A task takes the context that is active when it is created, so the
        # workflow's context is entered around the creation, not inside the task.
        with volnux_context(
            workflow_id=workflow_name,
            execution_id=execution_id,
            correlation_id=get_current_correlation_id() or execution_id,
        ):
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
            await self._finish(execution_id, "completed")
            return result

        except asyncio.TimeoutError:
            # Timeout expired — cancel the shielded task explicitly
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
            self._failed_count += 1
            message = (
                f"Workflow {workflow_name!r} timed out after {self._timeout}s "
                f"(execution_id={execution_id})"
            )
            await self._finish(execution_id, "failed", message)
            raise WorkflowError(message)

        except asyncio.CancelledError:
            # Workflow was cancelled from outside (cancel_execution, cancel_all)
            self._failed_count += 1
            message = (
                f"Workflow {workflow_name!r} was cancelled "
                f"(execution_id={execution_id})"
            )
            self._stop_requested.add(execution_id)
            await self._finish(execution_id, "stopped", message)
            raise WorkflowError(message)

        except WorkflowError as exc:
            self._failed_count += 1
            await self._finish(execution_id, "failed", str(exc))
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
            message = (
                f"Workflow {workflow_name!r} failed: {exc} "
                f"(execution_id={execution_id})"
            )
            await self._finish(execution_id, "failed", message)
            raise WorkflowError(message) from exc

        finally:
            self._deregister(record)

    async def _run_workflow(
        self,
        workflow_name: str,
        params: Dict[str, Any],
        execution_id: str,
    ) -> Any:
        """
        Resolve the WorkflowConfig and run it.

        Separated from _run() so subclasses can override only the dispatch
        logic without duplicating the task lifecycle boilerplate.

        ``params["run_type"]`` (``"single"``, the default, or ``"batch"``) picks
        how the workflow runs and is not passed on to it. The caller's dict is
        not changed.
        """
        if not self._workflow_registry.is_ready():
            raise WorkflowExecutionError("Workflow registry is not ready.")

        config = self._workflow_registry.get_workflow_config(workflow_name)
        if config is None:
            raise WorkflowExecutionError(
                f"Workflow {workflow_name!r} is not registered "
                "(workflow config not found)."
            )

        run_type = cast(Literal["batch", "single"], params.get("run_type", "single"))
        workflow_params = {k: v for k, v in params.items() if k != "run_type"}

        logger.debug(
            "Dispatching workflow %r to config %r — execution_id=%s",
            workflow_name,
            type(config).__name__,
            execution_id,
        )

        if execution_id in self._external_ids:
            # (the pipeline is held so the run can be paused, resumed or stopped)
            return await config.run_workflow(
                params=workflow_params,
                run_type=run_type,
                on_pipeline=lambda pipeline: self._pipelines.__setitem__(
                    execution_id, pipeline
                ),
            )
        return await config.run_workflow(params=workflow_params, run_type=run_type)

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

    # ── Control of live runs ──────────────────────────────────────────────────

    def _claim_execution_id(self, execution_id: Optional[str]) -> str:
        if execution_id is None:
            return self._new_execution_id()
        if execution_id in self._external_ids or execution_id in self._records_by_id:
            raise ValueError(f"Execution {execution_id!r} is already running")
        self._external_ids.add(execution_id)
        return execution_id

    def _pipeline(self, execution_id: str) -> Any:
        return self._pipelines.get(execution_id)

    async def pause_execution(self, execution_id: str) -> bool:
        pipeline = self._pipeline(execution_id)
        if pipeline is None or not hasattr(pipeline, "pause"):
            return False
        await pipeline.pause()
        return True

    async def resume_execution(self, execution_id: str) -> bool:
        pipeline = self._pipeline(execution_id)
        if pipeline is None or not hasattr(pipeline, "resume"):
            return False
        await pipeline.resume()
        return True

    async def stop_execution(self, execution_id: str) -> bool:
        pipeline = self._pipeline(execution_id)
        if pipeline is None or not hasattr(pipeline, "stop"):
            return False
        self._stop_requested.add(execution_id)
        await pipeline.stop()
        return True

    async def _finish(
        self, execution_id: str, state: str, error: Optional[str] = None
    ) -> None:
        """Forget a run the caller knows by its own id, and tell the listeners."""
        self._pipelines.pop(execution_id, None)
        if execution_id not in self._external_ids:
            return
        self._external_ids.discard(execution_id)
        if execution_id in self._stop_requested:
            self._stop_requested.discard(execution_id)
            if state in ("completed", "failed"):
                state = "stopped"  # (a stopped run may end either way)
        await self._notify(execution_id, state, error)

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
