"""
Volnux Global Pool Manager

Unified task queue and worker pool for all CPU-bound execution in a project.
Every task — local events, remote manager requests, workflow executions —
flows through a single priority-ordered queue into a fixed-size worker pool.

The pool size is configured in config.py and never grows at runtime.
The queue orders tasks by priority with starvation prevention.
FIFO ordering is guaranteed within the same priority level.
"""

import heapq
import itertools
import logging
import multiprocessing
import threading
import time
import warnings
from concurrent.futures import Future, ProcessPoolExecutor, wait, FIRST_EXCEPTION
from dataclasses import dataclass, field
from enum import IntEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
)

try:
    from celery import Celery
    from volnux.executors.celery import CeleryExecutor
except ImportError:
    warnings.warn(
        "Celery is not installed. Celery-based execution will not be available.",
        ImportWarning,
    )
    Celery = None
    CeleryExecutor = None

if TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext

logger = logging.getLogger(__name__)

BackendType = Literal["local", "celery"]

_TASK_COUNTER = itertools.count(1)  # Thread-safe monotonic counter

__all__ = [
    "VolnuxPoolManager",
    "TaskPriority",
    "QueueTask",
    "get_pool_manager",
]


class TaskPriority(IntEnum):
    """
    Priority levels for task dispatch ordering.

    Lower numeric value = higher priority. The gap between levels determines
    starvation resistance: a CRITICAL task always dispatches before HIGH, etc.
    """

    CRITICAL = 0
    HIGH = 100
    NORMAL = 500
    LOW = 1000

    def demote(self) -> "TaskPriority":
        """Return the next lower priority level, or self if already LOW."""
        order = [
            TaskPriority.CRITICAL,
            TaskPriority.HIGH,
            TaskPriority.NORMAL,
            TaskPriority.LOW,
        ]
        idx = order.index(self)
        return order[min(idx + 1, len(order) - 1)]

    def promote(self) -> "TaskPriority":
        """Return the next higher priority level, or self if already CRITICAL."""
        order = [
            TaskPriority.CRITICAL,
            TaskPriority.HIGH,
            TaskPriority.NORMAL,
            TaskPriority.LOW,
        ]
        idx = order.index(self)
        return order[max(idx - 1, 0)]


@dataclass(order=True)
class QueueTask:
    """
    A task waiting for a worker slot in the pool.

    Ordered by (priority, enqueue_time) — FIFO within the same priority level.
    All non-ordering fields carry compare=False to keep heap comparisons fast.

    Fields used for heap ordering MUST be listed before fields with
    compare=False, because @dataclass(order=True) generates comparison
    methods in field declaration order.
    """

    priority: TaskPriority  # lower = higher urgency
    enqueue_time: float  # tie-break: earlier = higher urgency (FIFO)
    context_id: str = field(compare=False)
    task_id: str = field(compare=False)
    dispatch_fn: Callable = field(compare=False)
    args: tuple = field(compare=False, default_factory=tuple)
    kwargs: dict = field(compare=False, default_factory=dict)
    source: str = field(compare=False, default="local")

    def __post_init__(self) -> None:
        if not isinstance(self.priority, TaskPriority):
            # Accept bare int but coerce to TaskPriority for .name access
            self.priority = TaskPriority(int(self.priority))


class PriorityTaskQueue:
    """
    Priority-ordered task queue with starvation prevention.

    Tasks are ordered by (priority, enqueue_time). Within the same priority
    level, tasks are FIFO. Tasks that have waited longer than
    ``starvation_threshold`` seconds are promoted one priority level.

    A ``max_size`` limit provides backpressure: ``enqueue()`` raises
    ``QueueFull`` when the limit is reached, allowing callers to apply
    backpressure rather than growing the queue without bound.

    Thread-safe. All public methods acquire the internal lock.
    """

    class QueueFull(Exception):
        """Raised by enqueue() when the queue is at capacity."""

    # Sentinel pushed to unblock dequeue() when the queue is shutting down
    _STOP = object()

    def __init__(
        self,
        starvation_threshold: float = 60.0,
        max_size: int = 0,
    ) -> None:
        """
        Args:
            starvation_threshold:
                Seconds a task may wait before being promoted one priority
                level. Default 60s. Set to 0 to disable starvation promotion.
            max_size:
                Maximum number of tasks the queue will hold. 0 = unlimited.
                When the limit is reached, ``enqueue()`` raises ``QueueFull``.
        """
        self._heap: List[QueueTask] = []
        self._starvation_threshold: float = starvation_threshold
        self._max_size: int = max_size
        self._total_enqueued: int = 0
        self._total_dequeued: int = 0
        self._lock = threading.Lock()
        self._item_available = threading.Event()
        self._stopped: bool = False

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._heap)

    @property
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            by_priority: Dict[str, int] = {}
            for p in TaskPriority:
                by_priority[p.name.lower()] = sum(
                    1 for t in self._heap if t.priority == p
                )
            return {
                "queue_depth": len(self._heap),
                "total_enqueued": self._total_enqueued,
                "total_dequeued": self._total_dequeued,
                "max_size": self._max_size or "unlimited",
                "priorities": by_priority,
            }

    def enqueue(self, task: QueueTask) -> None:
        """
        Add a task to the queue. Thread-safe.

        Raises:
            PriorityTaskQueue.QueueFull:
                If max_size is set and the queue is at capacity.
            RuntimeError:
                If the queue has been stopped.
        """
        with self._lock:
            if self._stopped:
                raise RuntimeError("Cannot enqueue to a stopped PriorityTaskQueue.")
            if self._max_size and len(self._heap) >= self._max_size:
                raise PriorityTaskQueue.QueueFull(
                    f"Queue is full ({self._max_size} tasks). "
                    "Apply backpressure or increase max_size."
                )
            task.enqueue_time = time.monotonic()
            heapq.heappush(self._heap, task)
            self._total_enqueued += 1
            self._item_available.set()

        logger.debug(
            "Task %s enqueued (priority=%s source=%s depth=%d)",
            task.task_id,
            task.priority.name,
            task.source,
            len(self._heap),
        )

    def dequeue(self) -> Optional[QueueTask]:
        """
        Remove and return the highest-priority task.

        Blocks until a task is available or ``stop()`` is called.
        Returns ``None`` when the queue has been stopped and is empty,
        signalling the caller to exit.

        Thread-safe.
        """
        while True:
            with self._lock:
                # Apply starvation promotion before each dequeue
                if self._heap:
                    self._promote_starving_tasks()

                if self._heap:
                    task = heapq.heappop(self._heap)
                    self._total_dequeued += 1
                    if not self._heap:
                        self._item_available.clear()
                    logger.debug(
                        "Task %s dequeued (priority=%s waited=%.2fs remaining=%d)",
                        task.task_id,
                        task.priority.name,
                        time.monotonic() - task.enqueue_time,
                        len(self._heap),
                    )
                    return task

                if self._stopped:
                    # Queue is empty and stopped — signal caller to exit
                    return None

            # Queue is empty and not stopped — wait for the next item
            # Short timeout so we re-check _stopped frequently
            self._item_available.wait(timeout=0.1)

    def remove_task(self, task_id: str) -> Optional[QueueTask]:
        """
        Remove a specific task from the queue by task_id.
        Returns the removed task, or None if not found.
        Thread-safe.
        """
        with self._lock:
            for i, task in enumerate(self._heap):
                if task.task_id == task_id:
                    # Swap with last element and re-heapify
                    self._heap[i] = self._heap[-1]
                    self._heap.pop()
                    heapq.heapify(self._heap)
                    return task
        return None

    def remove_by_context(self, context_id: str) -> List[QueueTask]:
        """
        Remove all queued (not yet dispatched) tasks for a given context_id.
        Returns the list of removed tasks.
        Thread-safe.
        """
        with self._lock:
            remaining = []
            removed = []
            for task in self._heap:
                if task.context_id == context_id:
                    removed.append(task)
                else:
                    remaining.append(task)

            if removed:
                self._heap = remaining
                heapq.heapify(self._heap)
                if not self._heap:
                    self._item_available.clear()

        if removed:
            logger.debug(
                "Removed %d queued task(s) for context=%s",
                len(removed),
                context_id[:8],
            )
        return removed

    def stop(self) -> None:
        """
        Signal the queue to stop. Unblocks any waiting ``dequeue()`` calls.
        After stop(), ``dequeue()`` returns None when the queue is empty.
        Safe to call multiple times.
        """
        with self._lock:
            self._stopped = True
            self._item_available.set()  # wake any blocked dequeue()

    def _promote_starving_tasks(self) -> None:
        """
        Promote tasks that have waited beyond the starvation threshold by
        one priority level.

        Must be called with self._lock held.

        Rather than mutating heap elements in place (which corrupts the heap
        invariant until heapify is called and risks corruption on exception),
        we rebuild the heap from scratch. The heap is typically small
        (<1,000 items) so this is O(n) — acceptable.
        """
        if not self._starvation_threshold:
            return

        now = time.monotonic()
        changed = False

        for task in self._heap:
            if task.priority == TaskPriority.CRITICAL:
                continue  # Already at maximum urgency
            wait_time = now - task.enqueue_time
            if wait_time > self._starvation_threshold:
                old_priority = task.priority
                task.priority = task.priority.promote()
                logger.info(
                    "Task %s promoted %s → %s after %.1fs waiting",
                    task.task_id,
                    old_priority.name,
                    task.priority.name,
                    wait_time,
                )
                changed = True

        if changed:
            # Rebuild the heap to restore the invariant after in-place mutations
            heapq.heapify(self._heap)


class _ContextFutureRegistry:
    """
    Thread-safe registry mapping context_id → set[Future].

    Tracks in-flight futures so drain_context() and cancel_context() can
    wait on or cancel all work belonging to a given ExecutionContext.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._store: Dict[str, Set[Future]] = {}

    def add(self, ctx_id: str, future: Future) -> None:
        with self._lock:
            self._store.setdefault(ctx_id, set()).add(future)

    def discard(self, ctx_id: str, future: Future) -> None:
        with self._lock:
            bucket = self._store.get(ctx_id)
            if bucket is None:
                return
            bucket.discard(future)
            if not bucket:
                del self._store[ctx_id]

    def snapshot(self, ctx_id: str) -> Set[Future]:
        with self._lock:
            return set(self._store.get(ctx_id, ()))

    def pop_all(self, ctx_id: str) -> Set[Future]:
        with self._lock:
            return self._store.pop(ctx_id, set())

    def active_count(self, ctx_id: str) -> int:
        with self._lock:
            return len(self._store.get(ctx_id, ()))

    def all_context_ids(self) -> List[str]:
        with self._lock:
            return list(self._store.keys())


class VolnuxPoolManager:
    """
    Unified task queue and worker pool for a Volnux project.

    All CPU-bound work — local events, remote manager tasks, workflow
    executions — flows through a single priority-ordered queue into a
    fixed-size worker pool.

    Lifecycle
    ---------
    One manager instance per project. Created at startup via the singleton
    accessor, lives until ``shutdown()``. Use ``get_pool_manager()`` rather
    than constructing directly.

    Features
    --------
    * Priority-ordered dispatch with FIFO ordering within priority levels
    * Starvation prevention via automatic priority promotion
    * Backpressure via configurable queue size limit
    * Unified queue for local, remote, and workflow tasks
    * Context-based task tracking for drain and cancel
    * Both queued and in-flight task cancellation
    * Two backends: local (ProcessPoolExecutor) and Celery
    * Clean shutdown via stop sentinel — no thread join race
    """

    _instance: Optional["VolnuxPoolManager"] = None
    _singleton_lock = threading.Lock()

    def __new__(cls) -> "VolnuxPoolManager":
        with cls._singleton_lock:
            if cls._instance is None:
                instance = object.__new__(cls)
                instance._initialized = False  # guard for __init__
                cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        # __init__ is called every time __new__ returns the singleton.
        # The _initialized guard ensures setup runs only once.
        if self._initialized:
            return
        self._executor: Optional[Union[ProcessPoolExecutor, Any]] = None
        self._backend_type: Optional[BackendType] = None
        self._initialized: bool = False
        self._shut_down: bool = False
        self._state_lock = threading.Lock()
        self._registry = _ContextFutureRegistry()
        self._task_queue: Optional[PriorityTaskQueue] = None
        self._dispatch_thread: Optional[threading.Thread] = None
        self._dispatch_started: bool = False

    def initialize(
        self,
        backend: BackendType = "local",
        *,
        max_workers: Optional[int] = None,
        celery_app: Optional[Any] = None,
        queue: Optional[str] = None,
        task_soft_time_limit: Optional[int] = None,
        task_time_limit: Optional[int] = None,
        starvation_threshold: float = 60.0,
        max_queue_size: int = 0,
    ) -> None:
        """
        Configure and start the executor and dispatch loop.

        Must be called once at project startup before any task is submitted.
        Idempotent — a second call with the same backend is a no-op.

        Args:
            backend:
                ``"local"`` (ProcessPoolExecutor) or ``"celery"``.
            max_workers:
                Worker count for the local backend. Defaults to CPU count.
            celery_app:
                Required when backend is ``"celery"``.
            queue:
                Celery queue name (celery backend only).
            task_soft_time_limit / task_time_limit:
                Forwarded to CeleryExecutor (celery backend only).
            starvation_threshold:
                Seconds before a task is promoted one priority level.
                Set to 0 to disable. Default: 60s.
            max_queue_size:
                Maximum tasks held in the priority queue. 0 = unlimited.
                When full, ``submit_task()`` raises ``PriorityTaskQueue.QueueFull``.

        Raises:
            RuntimeError: If already initialized with a different backend,
                          or if the manager has been shut down.
            ValueError:   If arguments are inconsistent.
        """
        backend = backend.lower()  # type: ignore[assignment]

        with self._state_lock:
            if self._shut_down:
                raise RuntimeError(
                    "VolnuxPoolManager has been shut down. "
                    "Call reset() before re-initializing."
                )
            if self._initialized:
                if self._backend_type == backend:
                    return  # Idempotent — same backend, no-op
                raise RuntimeError(
                    f"VolnuxPoolManager is already initialized with backend "
                    f"{self._backend_type!r}. Call reset() first to switch backends."
                )

            if backend == "local":
                if max_workers is not None and max_workers < 1:
                    raise ValueError(f"max_workers must be >= 1, got {max_workers}.")
                num_workers = max_workers or multiprocessing.cpu_count()
                # "spawn" is mandatory — "fork" + asyncio produces undefined behaviour
                ctx = multiprocessing.get_context("spawn")
                self._executor = ProcessPoolExecutor(
                    max_workers=num_workers,
                    mp_context=ctx,
                )
                logger.info(
                    "VolnuxPoolManager (local) initialised — %d workers.", num_workers
                )

            elif backend == "celery":
                if CeleryExecutor is None:
                    raise RuntimeError(
                        "Celery is not installed. "
                        "Install with: pip install 'volnux[celery]'"
                    )
                if celery_app is None:
                    raise ValueError("celery_app is required when backend='celery'.")
                self._executor = CeleryExecutor(
                    celery_app=celery_app,
                    queue=queue,
                    task_soft_time_limit=task_soft_time_limit,
                    task_time_limit=task_time_limit,
                )
                logger.info(
                    "VolnuxPoolManager (celery) initialised — queue=%r.",
                    queue or "default",
                )

            else:
                raise ValueError(
                    f"Unknown backend {backend!r}. " "Valid choices: 'local', 'celery'."
                )

            self._task_queue = PriorityTaskQueue(
                starvation_threshold=starvation_threshold,
                max_size=max_queue_size,
            )
            self._backend_type = backend  # type: ignore[assignment]
            self._initialized = True

            # Held inside _state_lock to prevent concurrent start() calls
            self._start_dispatch_loop()

    def submit_task(
        self,
        context: "ExecutionContext",
        task_func: Callable,
        /,
        *args: Any,
        priority: TaskPriority = TaskPriority.NORMAL,
        source: str = "local",
        **kwargs: Any,
    ) -> str:
        """
        Enqueue a task for execution.

        The task is added to the priority queue. The dispatch loop picks it
        up when a worker slot becomes available.

        Args:
            context:   ExecutionContext that owns this task.
            task_func: Callable to execute.
            priority:  Dispatch priority. Default: NORMAL.
            source:    Origin label for logging ("local", "remote", "workflow").
            *args, **kwargs: Forwarded to task_func on execution.

        Returns:
            task_id: The ID assigned to this task. Useful for targeted
                     cancellation via remove_task().

        Raises:
            RuntimeError: If not initialised or shut down.
            TypeError:  If task_func is not callable.
            PriorityTaskQueue.QueueFull: If max_queue_size is set and full.
        """
        self._assert_ready()

        if not callable(task_func):
            raise TypeError(
                f"task_func must be callable, got {type(task_func).__name__!r}."
            )

        # next(_TASK_COUNTER) is thread-safe: itertools.count() uses a C-level
        # lock internally; it never produces duplicate values under concurrency.
        task_id = f"task-{next(_TASK_COUNTER)}-{context.state_id[:8]}"

        task = QueueTask(
            priority=priority,
            enqueue_time=0.0,  # overwritten by PriorityTaskQueue.enqueue()
            context_id=context.state_id,
            task_id=task_id,
            dispatch_fn=task_func,
            args=args,
            kwargs=kwargs,
            source=source,
        )

        self._task_queue.enqueue(task)  # type: ignore[union-attr]
        return task_id

    def drain_context(
        self,
        context: "ExecutionContext",
        *,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Block until every in-flight task belonging to ``context`` finishes.

        Raises:
            TimeoutError: If timeout elapses before all futures settle.
        """
        ctx_id = context.state_id
        futures = self._registry.snapshot(ctx_id)
        if not futures:
            return

        logger.debug(
            "Draining context=%s — %d in-flight task(s).", ctx_id[:8], len(futures)
        )
        done, not_done = wait(futures, timeout=timeout)
        if not_done:
            raise TimeoutError(
                f"drain_context timed out for context {ctx_id!r}: "
                f"{len(not_done)} task(s) still running."
            )
        logger.debug("Context %s drained.", ctx_id[:8])

    def cancel_context(
        self,
        context: "ExecutionContext",
        *,
        terminate: bool = False,
    ) -> Tuple[int, int]:
        """
        Cancel all queued and in-flight tasks for ``context``.

        Queued tasks are removed from the priority queue before they are
        dispatched to the worker pool. In-flight tasks are cancelled via
        their Future. For Celery tasks, ``terminate=True`` sends a REVOKE
        signal to the worker.

        Args:
            context:   ExecutionContext whose tasks should be cancelled.
            terminate: Whether to terminate in-flight Celery tasks.

        Returns:
            (cancelled, skipped): Tasks successfully cancelled vs already
                                  running or completed.
        """
        ctx_id = context.state_id
        cancelled = 0
        skipped = 0

        queued_removed = self._task_queue.remove_by_context(ctx_id)  # type: ignore[union-attr]
        cancelled += len(queued_removed)

        in_flight = self._registry.pop_all(ctx_id)
        for future in in_flight:
            if terminate and isinstance(self._executor, CeleryExecutor):
                task_id = getattr(future, "task_id", None)
                if task_id:
                    self._executor.revoke(task_id, terminate=True)

            if future.cancel():
                cancelled += 1
            else:
                # Already running or done — cannot cancel
                skipped += 1

        logger.info(
            "cancel_context %s — queued_removed=%d futures_cancelled=%d "
            "futures_skipped=%d",
            ctx_id[:8],
            len(queued_removed),
            cancelled - len(queued_removed),
            skipped,
        )
        return cancelled, skipped

    def wait_any(
        self,
        context: "ExecutionContext",
        *,
        timeout: Optional[float] = None,
    ) -> Tuple[Set[Future], Set[Future]]:
        """
        Wait until at least one task in ``context`` finishes or raises.

        Returns:
            (done, not_done): Sets of futures in each state.
        """
        futures = self._registry.snapshot(context.state_id)
        if not futures:
            return set(), set()
        return wait(futures, timeout=timeout, return_when=FIRST_EXCEPTION)

    @property
    def is_initialized(self) -> bool:
        return self._initialized and not self._shut_down

    @property
    def backend_type(self) -> Optional[BackendType]:
        return self._backend_type

    def active_count(self, context: "ExecutionContext") -> int:
        """Number of in-flight tasks belonging to ``context``."""
        return self._registry.active_count(context.state_id)

    def active_context_ids(self) -> List[str]:
        """All context IDs that currently have in-flight tasks."""
        return self._registry.all_context_ids()

    def report(self) -> Dict[str, Any]:
        """Snapshot of pool and queue state for health monitoring."""
        queue_stats = self._task_queue.stats if self._task_queue else {}
        return {
            "queue": queue_stats,
            "pool": {
                "backend": self._backend_type,
                "initialized": self._initialized,
                "shut_down": self._shut_down,
            },
            "contexts": {
                "active_count": len(self._registry.all_context_ids()),
            },
        }

    def shutdown(
        self,
        *,
        wait_for_tasks: bool = True,
        cancel_futures: bool = False,
    ) -> None:
        """
        Stop the dispatch loop and tear down the executor.

        Args:
            wait_for_tasks: Block until all in-flight tasks complete.
            cancel_futures: Cancel pending futures before shutdown.

        Safe to call multiple times.
        """
        with self._state_lock:
            if self._shut_down:
                return
            self._shut_down = True

        # Stop the queue — this unblocks the dispatch thread immediately
        if self._task_queue is not None:
            self._task_queue.stop()

        # Join the dispatch thread with a reasonable timeout
        if self._dispatch_thread and self._dispatch_thread.is_alive():
            self._dispatch_thread.join(timeout=10.0)
            if self._dispatch_thread.is_alive():
                logger.warning(
                    "Dispatch thread did not exit within 10s — continuing shutdown."
                )

        # Shut down the executor
        if self._executor is not None:
            logger.info(
                "VolnuxPoolManager (%s) shutting down executor.",
                self._backend_type,
            )
            self._executor.shutdown(
                wait=wait_for_tasks,
                cancel_futures=cancel_futures,
            )
            self._executor = None

        logger.info("VolnuxPoolManager shut down complete.")

    @classmethod
    def reset(cls) -> None:
        """
        Destroy the singleton and reset to pre-initialized state.

        The existing instance is shut down if still running. After reset(),
        the next call to ``VolnuxPoolManager()`` or ``get_pool_manager()``
        returns a fresh, uninitialized instance.

        Primarily used in tests. Use with caution in production.
        """
        with cls._singleton_lock:
            if cls._instance is not None:
                instance = cls._instance
                cls._instance = None  # Detach first to prevent re-use during shutdown
                if instance._initialized and not instance._shut_down:
                    try:
                        instance.shutdown(wait_for_tasks=False, cancel_futures=True)
                    except Exception:
                        logger.exception("Error during implicit shutdown in reset().")
        logger.debug("VolnuxPoolManager reset.")

    def __enter__(self) -> "VolnuxPoolManager":
        return self

    def __exit__(self, *_: Any) -> None:
        self.shutdown(wait_for_tasks=True)

    def _start_dispatch_loop(self) -> None:
        """
        Start the background dispatch thread.

        Must be called with self._state_lock held to prevent concurrent starts.
        """
        if self._dispatch_started:
            return
        self._dispatch_started = True
        self._dispatch_thread = threading.Thread(
            target=self._dispatch_loop,
            name="volnux-pool-dispatcher",
            daemon=True,
        )
        self._dispatch_thread.start()
        logger.debug("Dispatch loop started.")

    def _dispatch_loop(self) -> None:
        """
        Continuously dequeue tasks from the priority queue and submit them
        to the worker pool.

        Runs in a background thread. Each task is submitted to the executor
        and the resulting Future is registered under its context ID so that
        drain_context() and cancel_context() can track it.

        Exits cleanly when the queue returns None (queue stopped and empty).
        """
        logger.debug(
            "Dispatch loop running on thread %s.", threading.current_thread().name
        )

        while True:
            task = self._task_queue.dequeue()  # type: ignore[union-attr]

            if task is None:
                # Queue has been stopped and is empty — exit cleanly
                logger.debug("Dispatch loop received stop signal — exiting.")
                break

            if self._executor is None:
                logger.warning("Task %s dropped — executor is gone.", task.task_id)
                continue

            try:
                future = self._executor.submit(
                    task.dispatch_fn, *task.args, **task.kwargs
                )
                self._registry.add(task.context_id, future)
                future.add_done_callback(
                    lambda f, ctx_id=task.context_id: self._registry.discard(ctx_id, f)
                )
                logger.debug(
                    "Task %s dispatched (context=%s source=%s).",
                    task.task_id,
                    task.context_id[:8],
                    task.source,
                )
            except Exception as exc:
                # Log and continue — a single task submit failure should
                # not bring down the dispatch loop
                logger.error(
                    "Failed to submit task %s to executor: %s",
                    task.task_id,
                    exc,
                    exc_info=True,
                )

        logger.debug("Dispatch loop exited.")

    def _assert_ready(self) -> None:
        if self._shut_down:
            raise RuntimeError(
                "VolnuxPoolManager has been shut down. "
                "Call reset() then initialize() to reuse."
            )
        if not self._initialized or self._executor is None or self._task_queue is None:
            raise RuntimeError(
                "VolnuxPoolManager is not initialised. "
                "Call initialize() before submitting tasks."
            )


def get_pool_manager() -> VolnuxPoolManager:
    """Return the singleton VolnuxPoolManager instance."""
    return VolnuxPoolManager()
