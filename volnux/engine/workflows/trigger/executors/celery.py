"""
CeleryWorkflowExecutor — dispatches workflow execution to Celery workers.

Architecture
------------
The executor lives in the API/trigger process. It submits tasks to the
Celery broker. A separate Celery worker process picks them up.

                API / TriggerEngine process
                ┌────────────────────────────┐
                │ CeleryWorkflowExecutor     │
                │   execute()                │
                │   └─ apply_async()         │──► Redis broker
                │   └─ _poll_result()        │◄── Redis result backend
                └────────────────────────────┘

                Celery worker process (separate OS process)
                ┌────────────────────────────┐
                │ worker_process_init signal  │
                │   └─ initialise_workflows() │ ← once per process lifetime
                │                            │
                │ _volnux_run_workflow task   │
                │   └─ asyncio.run(           │
                │        config.run_workflow  │
                │        _async()            │
                │      )                     │
                └────────────────────────────┘

Key design decisions
--------------------
D1. Engine initialised once at worker startup, not per task.
    The `worker_process_init` signal calls `initialise_workflows()` once when
    the worker process forks. All tasks in that process share the singleton.
    This avoids re-connecting to PostgreSQL, re-compiling Pointy-Lang graphs,
    and re-loading EventHub packages on every workflow dispatch.

D2. `asyncio.run()` inside the Celery task.
    Celery workers are synchronous by default. Each task invocation creates
    its own event loop via `asyncio.run()`, runs the workflow to completion,
    then tears it down. This is the correct pattern for sync Celery +
    async Volnux — it never shares an event loop across task invocations.

D3. Celery app is injectable.
    The executor accepts an existing `Celery` app instance OR a `broker_url`
    from which it constructs one. Projects that already manage a Celery app
    can reuse it without creating a second connection pool.

D4. Task name is queue-scoped, not hardcoded.
    Multiple executors with different queues register tasks named
    `"volnux.run_workflow.{queue}"` so they coexist without overwriting
    each other's registrations.

D5. `_project_dir` is a task argument, not a hidden key in `params`.
    User workflow params are never mutated. Project directory is passed
    as an explicit positional argument to the task.

D6. `celery_result.ready()` runs in a thread via run_in_executor.
    The call is a synchronous network round trip to the result backend.
    Running it on the event loop thread would stall all concurrent coroutines
    during the poll. `run_in_executor` releases the event loop between polls.

D7. `revoke()` is implemented.
    `pool_manager.py` calls `executor.revoke(task_id, terminate=True)` during
    context cancellation. This method must exist on all Celery executor
    implementations.
"""

import asyncio
import json
import logging
import time
import warnings
from pathlib import Path
from typing import Any, Dict, Optional, Union, TYPE_CHECKING

from volnux.config import setting
from volnux.context import (
    dispatch_worker_task,
    get_current_correlation_id,
    inject_carrier,
    volnux_context,
)
from volnux.engine.workflows.workflow import WorkflowExecutionError

from .base import WorkflowExecutor

if TYPE_CHECKING:
    from volnux.engine.workflows import WorkflowRegistry

logger = logging.getLogger(__name__)

__all__ = ["CeleryWorkflowExecutor"]


# Set once by `worker_process_init` signal. Never written by task code.
_WORKER_ENGINE = None


def _get_worker_engine():
    """Return the per-process engine singleton. Raises if not initialised."""
    if _WORKER_ENGINE is None:
        raise RuntimeError(
            "Volnux engine is not initialised on this Celery worker. "
            "Ensure the worker was started with VOLNUX_PROJECT_DIR set "
            "and the worker_process_init signal is connected. "
            "See register_worker_init()."
        )
    return _WORKER_ENGINE


def register_worker_init(celery_app, project_dir: Union[str, Path]) -> None:
    """
    Connect the Volnux engine initialisation to Celery's worker_process_init
    signal. Call this once when setting up the Celery application.

    Must be called in the same module where the Celery app is created so
    that the signal is registered before workers fork.

    Args:
        celery_app:  The Celery application instance.
        project_dir: Path to the Volnux project root (where init.py lives).

    Usage:
        app = Celery("myproject", broker=BROKER_URL)
        register_worker_init(app, project_dir="/opt/volnux")
    """
    from celery.signals import worker_process_init

    project_dir = Path(project_dir)

    @worker_process_init.connect(weak=False)
    def _init_volnux_engine(sender=None, **kwargs):
        global _WORKER_ENGINE
        if _WORKER_ENGINE is not None:
            return
        try:
            from volnux.setup import initialise_workflows

            logger.info(
                "Worker process init: loading Volnux engine from %s", project_dir
            )
            # (initialise_workflows is a coroutine; this signal handler is not)
            _WORKER_ENGINE = asyncio.run(initialise_workflows(project_dir))
            logger.info("Volnux engine initialised on worker process")
        except Exception as exc:
            logger.critical(
                "Failed to initialise Volnux engine on worker: %s",
                exc,
                exc_info=True,
            )
            # Re-raise so the worker process fails fast rather than
            # running tasks against an uninitialised engine
            raise


class CeleryWorkflowExecutor(WorkflowExecutor):
    """
    Dispatches workflow execution to Celery workers.

    The executor runs in the API/trigger process. It submits tasks to the
    Celery broker. Workers pick them up and run the workflows in isolated
    processes. The executor polls the result backend for completion.

    Setup (two-step)
    ----------------
    Step 1 — Register worker init (in your Celery app module):
        from volnux.engine.workflows.trigger.executors.celery import register_worker_init
        register_worker_init(celery_app, project_dir="/opt/volnux")

    Step 2 — Create executor (in WorkflowConfig.ready()):
        def ready(self):
            self.executor = CeleryWorkflowExecutor(
                workflow_registry = self._workflow_registry,
                celery_app = celery_app,
                queue = "volnux_workflows",
            )

    Cancellation
    ------------
    Use ``revoke(task_id, terminate=True)`` to stop a dispatched task.
    Task IDs are available from ``get_pending_tasks()``.
    """

    def __init__(
        self,
        workflow_registry: "WorkflowRegistry",
        celery_app: Any = None,  # Celery instance or None
        broker_url: Optional[str] = None,
        result_backend: Optional[str] = None,
        queue: Optional[str] = None,
        task_soft_time_limit: Optional[int] = None,
        task_time_limit: Optional[int] = None,
        poll_interval: Optional[float] = None,
        result_timeout: Optional[float] = None,
        **celery_kwargs,
    ):
        """
        Initialise the Celery workflow executor.

        Exactly one of ``celery_app`` or ``broker_url`` must be provided.

        Args:
            workflow_registry:
                Registry for resolving workflow names to WorkflowConfig classes.

            celery_app:
                An existing Celery application instance. Preferred — reuses
                the app's connection pool and signal handlers.

            broker_url:
                Broker URL (e.g. ``"redis://localhost:6379/0"``). Used to
                construct a new Celery app when no existing app is provided.

            result_backend:
                Result backend URL. Defaults to ``broker_url`` when not set.
                Warning: using the same Redis for broker and results forces
                a single eviction policy. Use separate instances in production.

            queue:
                Celery queue name for workflow tasks. Must match the queue
                the worker is consuming from (``celery -Q volnux_workflows``).

            task_soft_time_limit:
                Soft time limit in seconds. Raises ``SoftTimeLimitExceeded``
                in the task, allowing graceful checkpoint drain.

            task_time_limit:
                Hard time limit in seconds. Must be > task_soft_time_limit
                so the checkpoint drain has time to complete after the soft
                limit fires. The worker is killed after this.

            poll_interval:
                Seconds between result backend polls in ``execute()``.
                Lower = more responsive, higher = lower backend load.
                Default: 0.5s.

            result_timeout:
                Seconds ``execute()`` waits for the result before it revokes the
                task and raises. None: the ``CELERY_EXECUTOR_RESULT_TIMEOUT``
                setting (wait indefinitely unless it is set).

            **celery_kwargs:
                Additional keyword arguments forwarded to the Celery
                constructor when ``broker_url`` is provided.

        Raises:
            ValueError: If neither or both of celery_app and broker_url
                        are provided.
        """
        super().__init__(workflow_registry)

        if celery_app is None and broker_url is None:
            raise ValueError(
                "Provide either 'celery_app' (preferred) or 'broker_url' "
                "to CeleryWorkflowExecutor."
            )
        if celery_app is not None and broker_url is not None:
            raise ValueError("Provide either 'celery_app' or 'broker_url', not both.")

        # (None: the CELERY_EXECUTOR_QUEUE / CELERY_EXECUTOR_POLL_INTERVAL settings)
        self._queue = queue or setting("CELERY_EXECUTOR_QUEUE", "volnux_workflows")
        self._task_soft_time_limit = _optional_number(
            task_soft_time_limit, "CELERY_EXECUTOR_SOFT_TIME_LIMIT"
        )
        self._task_time_limit = _optional_number(
            task_time_limit, "CELERY_EXECUTOR_TIME_LIMIT"
        )
        self._result_timeout = _optional_number(
            result_timeout, "CELERY_EXECUTOR_RESULT_TIMEOUT"
        )
        self._poll_interval = float(
            poll_interval
            if poll_interval is not None
            else setting("CELERY_EXECUTOR_POLL_INTERVAL", 0.5)
        )

        # Resolve or create the Celery app
        if celery_app is not None:
            self._celery_app = celery_app
            self._owns_app = False
            logger.debug(
                "CeleryWorkflowExecutor using existing Celery app: %s",
                self._celery_app.main,
            )
        else:
            self._celery_app = self._create_celery_app(
                broker_url=broker_url,
                result_backend=result_backend,
                celery_kwargs=celery_kwargs,
            )
            self._owns_app = True

        # Task name is queue-scoped so multiple executors with different queues
        # coexist without overwriting each other's registrations.
        self._task_name = f"volnux.run_workflow.{self._queue}"
        self._task = self._register_task()

        # Track submitted tasks for revocation and status
        # Maps execution_id → AsyncResult
        self._pending: Dict[str, Any] = {}

    async def execute(
        self,
        workflow_name: str,
        params: Dict[str, Any],
        *,
        execution_id: Optional[str] = None,
    ) -> Any:
        """
        Dispatch a workflow to Celery and await its result.

        The workflow is serialised and sent to the Celery broker. A worker
        picks it up and executes it asynchronously. This method polls the
        result backend until the task completes.

        Args:
            workflow_name: Name of the workflow to execute.
            params:        Parameters passed to the workflow. Never mutated.

        Returns:
            The workflow's return value.

        Raises:
            WorkflowExecutionError: If the workflow fails or is revoked.
        """
        project_dir = self._resolve_project_dir()
        execution_id = _new_execution_id()

        # The payload is a new dict: the caller's ``params`` are not touched. The
        # carrier is the context of this process (its non-secret configuration,
        # the workflow/trace/correlation ids); the worker runs under it.
        with volnux_context(
            workflow_id=workflow_name,
            execution_id=execution_id,
            correlation_id=get_current_correlation_id() or execution_id,
        ):
            payload = inject_carrier(
                {
                    "workflow_name": workflow_name,
                    "params": params,
                    "project_dir": str(project_dir),
                }
            )

        logger.info(
            "Dispatching workflow %r to queue %r — execution_id=%s",
            workflow_name,
            self._queue,
            execution_id,
        )

        try:
            celery_result = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: self._task.apply_async(
                    args=(payload,),
                    queue=self._queue,
                    task_id=execution_id,
                ),
            )
            self._pending[execution_id] = celery_result

            result = await self._poll_result(
                celery_result=celery_result,
                workflow_name=workflow_name,
                execution_id=execution_id,
            )

            logger.info(
                "Workflow %r completed on Celery worker — execution_id=%s",
                workflow_name,
                execution_id,
            )
            return result

        except WorkflowExecutionError:
            raise
        except Exception as exc:
            logger.error(
                "Workflow %r failed on Celery — execution_id=%s: %s",
                workflow_name,
                execution_id,
                exc,
                exc_info=True,
            )
            raise WorkflowExecutionError(
                f"Celery dispatch failed for workflow {workflow_name!r}: {exc}"
            ) from exc
        finally:
            self._pending.pop(execution_id, None)

    def revoke(self, task_id: str, terminate: bool = False) -> None:  # noqa: D401
        """
        Revoke a pending or running Celery task.

        Called by VolnuxPoolManager.cancel_context() during context
        cancellation. Safe to call even if the task has already completed —
        Celery ignores revocations for completed tasks.

        Args:
            task_id:   The execution_id / Celery task ID to revoke.
            terminate: When True, sends SIGTERM to the worker process
                       running the task. The worker's checkpoint drain
                       (task_soft_time_limit) should trigger before SIGTERM
                       is received.
        """
        try:
            self._celery_app.control.revoke(
                task_id,
                terminate=terminate,
                signal="SIGTERM" if terminate else None,
            )
            logger.info("Revoked Celery task %s (terminate=%s)", task_id, terminate)
        except Exception as exc:
            logger.warning("Failed to revoke task %s: %s", task_id, exc)

    def get_pending_tasks(self) -> Dict[str, str]:
        """
        Return currently tracked pending task IDs.

        Returns:
            Dict mapping execution_id → Celery task state.
        """
        result = {}
        for execution_id, ar in list(self._pending.items()):
            try:
                result[execution_id] = ar.state
            except Exception:
                result[execution_id] = "UNKNOWN"
        return result

    def get_queue_name(self) -> str:
        """Return the Celery queue name used by this executor."""
        return self._queue

    async def get_worker_stats(self) -> Dict[str, Any]:
        """
        Query Celery worker statistics.

        Issues inspect broadcast to all workers and returns aggregated stats.
        Runs in a thread so the event loop is not blocked during the
        broadcast wait.

        Returns:
            Dict with worker count, active task count, and reserved task count.
        """

        def _inspect():
            inspect = self._celery_app.control.inspect(timeout=3.0)
            # Single broadcast for all three metrics
            # (inspect() issues one broadcast per call — 3 calls = 3 broadcasts)
            stats = inspect.stats() or {}
            active = inspect.active() or {}
            reserved = inspect.reserved() or {}
            return stats, active, reserved

        try:
            stats, active, reserved = await asyncio.get_running_loop().run_in_executor(
                None, _inspect
            )
            return {
                "queue": self._queue,
                "worker_count": len(stats),
                "active_tasks": sum(len(t) for t in active.values()),
                "reserved_tasks": sum(len(t) for t in reserved.values()),
                "workers": list(stats.keys()),
            }
        except Exception as exc:
            logger.warning("get_worker_stats failed: %s", exc)
            return {
                "queue": self._queue,
                "worker_count": 0,
                "error": str(exc),
            }

    async def _poll_result(
        self,
        celery_result: Any,
        workflow_name: str,
        execution_id: str,
    ) -> Any:
        """
        Poll the result backend until the task completes.

        Runs `celery_result.ready()` in a thread pool on each iteration so
        the event loop is not blocked during the network round trip.

        Args:
            celery_result: The Celery AsyncResult to poll.
            workflow_name: For error message context only.
            execution_id:  For error message context only.

        Returns:
            The workflow result.

        Raises:
            WorkflowExecutionError: If the task failed or was revoked.
        """
        loop = asyncio.get_running_loop()
        deadline = (
            time.monotonic() + self._result_timeout
            if self._result_timeout is not None
            else None
        )

        while True:
            # Run the blocking ready() check in a thread — never block the
            # event loop thread on a network call
            is_ready = await loop.run_in_executor(None, celery_result.ready)
            if is_ready:
                break
            if deadline is not None and time.monotonic() >= deadline:
                self.revoke(execution_id, terminate=True)
                raise WorkflowExecutionError(
                    f"Workflow {workflow_name!r} gave no result within "
                    f"{self._result_timeout}s; the task was revoked — "
                    f"execution_id={execution_id}"
                )
            await asyncio.sleep(self._poll_interval)

        # Task is done — read the terminal state exactly once
        state = celery_result.state  # Cached locally after ready() is True

        if state == "SUCCESS":
            raw = celery_result.result
            # Unwrap the result envelope if present
            if isinstance(raw, dict) and "result" in raw:
                return raw["result"]
            return raw

        if state == "FAILURE":
            tb = celery_result.traceback or str(celery_result.result)
            raise WorkflowExecutionError(
                f"Workflow {workflow_name!r} failed on Celery worker — "
                f"execution_id={execution_id}\n{tb}"
            )

        if state == "REVOKED":
            raise WorkflowExecutionError(
                f"Workflow {workflow_name!r} was revoked — "
                f"execution_id={execution_id}"
            )

        raise WorkflowExecutionError(
            f"Workflow {workflow_name!r} ended in unexpected state {state!r} — "
            f"execution_id={execution_id}"
        )

    def _register_task(self) -> Any:
        """
        Register the Celery task that executes workflows on workers.

        The task is registered once at executor construction time. On the
        worker side, `_get_worker_engine()` returns the singleton initialised
        by `register_worker_init()`. The task never calls `initialise_workflows`.

        Returns:
            The registered Celery task callable.
        """
        task_name = self._task_name
        task_soft_time_limit = self._task_soft_time_limit
        task_time_limit = self._task_time_limit

        @self._celery_app.task(
            name=task_name,
            bind=False,  # No self-reference needed
            soft_time_limit=task_soft_time_limit,
            time_limit=task_time_limit,
            max_retries=0,  # Retry logic belongs in EventBase.RetryMixin
            acks_late=True,  # Ack only after completion (at-least-once)
            reject_on_worker_lost=True,  # Requeue if worker dies mid-task
            track_started=True,
        )
        @dispatch_worker_task
        def _volnux_run_workflow(payload: Dict[str, Any]) -> Dict[str, Any]:
            """
            Execute a Volnux workflow inside a Celery worker process.

            ``dispatch_worker_task`` activates the context the executor injected
            (the dispatcher's configuration and ids) for the whole call. The
            workflow runs in a fresh asyncio event loop (``asyncio.run``), which
            starts from the active context. Uses the singleton engine initialised
            at worker startup — never calls initialise_workflows() itself.

            Args:
                payload: ``workflow_name``, ``params`` (the user's workflow
                    parameters, not mutated), ``project_dir`` (for error context
                    only; the engine is already initialised from it) and the
                    context carrier.
            """
            workflow_name = payload["workflow_name"]
            params = payload.get("params") or {}
            project_dir = payload.get("project_dir")

            engine = _get_worker_engine()
            registry = engine.get_workflow_registry()
            config = registry.get_workflow_config(workflow_name)

            if config is None:
                raise WorkflowExecutionError(
                    f"Workflow {workflow_name!r} is not registered on this worker. "
                    f"Ensure the workflow is declared in WorkflowConfig.ready() "
                    f"and the worker's project at {project_dir!r} includes it."
                )

            run_type = params.get("run_type", "single")
            workflow_params = {k: v for k, v in params.items() if k != "run_type"}

            try:
                result = asyncio.run(
                    config.run_workflow(params=workflow_params, run_type=run_type)
                )
            except Exception as exc:
                if type(exc).__name__ == "SoftTimeLimitExceeded":
                    raise  # Celery's own signal: let Celery handle it
                raise WorkflowExecutionError(
                    f"Workflow {workflow_name!r} raised an exception: {exc}"
                ) from exc

            return {
                "workflow_name": workflow_name,
                # (the result backend serialises JSON: what JSON cannot hold, such
                # as the Pipeline object, is returned as its text)
                "result": json.loads(json.dumps(result, default=str)),
            }

        return _volnux_run_workflow

    def _create_celery_app(
        self,
        broker_url: str,
        result_backend: Optional[str],
        celery_kwargs: dict,
    ) -> Any:
        """
        Construct a Celery app from a broker URL.

        Warns when broker and result backend share the same Redis instance —
        they have incompatible optimal eviction policies in production.
        """
        try:
            from celery import Celery
        except ImportError:
            raise RuntimeError(
                "Celery is not installed. " "Install with: pip install 'volnux[celery]'"
            )

        effective_backend = result_backend or broker_url
        if effective_backend == broker_url:
            warnings.warn(
                "CeleryWorkflowExecutor: broker and result backend are the same "
                f"Redis instance ({broker_url}). In production, use separate "
                "Redis instances: the broker needs 'noeviction' policy while "
                "the result backend benefits from 'volatile-lru'. "
                "Set result_backend to a separate Redis URL.",
                UserWarning,
                stacklevel=3,
            )

        app = Celery(
            "volnux_workflows",
            broker=broker_url,
            backend=effective_backend,
            **celery_kwargs,
        )
        app.conf.update(
            task_default_queue=self._queue,
            task_acks_late=True,
            task_reject_on_worker_lost=True,
            task_track_started=True,
            task_serializer="json",
            result_serializer="json",
            accept_content=["json"],
        )
        logger.debug(
            "CeleryWorkflowExecutor created Celery app — broker=%s queue=%s",
            broker_url,
            self._queue,
        )
        return app

    @staticmethod
    def _resolve_project_dir() -> Path:
        """
        Resolve the project directory to pass to the Celery task as context.

        The Celery task does not use this for engine initialisation —
        the engine is already initialised at worker startup. It is included
        in the task arguments for error messages and audit trail context.
        """
        env_dir = setting("PROJECT_DIR", None)
        if env_dir:
            return Path(env_dir)
        # Fall back to the directory that contains init.py
        for parent in Path.cwd().parents:
            if (parent / "init.py").exists() and (
                (parent / "settings.py").exists() or (parent / "config.py").exists()
            ):
                return parent
        return Path.cwd()


def _optional_number(value: Optional[float], setting_name: str) -> Optional[float]:
    """``value`` if given, else the setting (None when it is not set)."""
    if value is None:
        value = setting(setting_name, None)
    return None if value is None else float(value)


def _new_execution_id() -> str:
    """Generate a time-sortable unique execution ID for a Celery task."""
    import uuid

    return f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
