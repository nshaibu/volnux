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
from concurrent.futures import Future, ProcessPoolExecutor, wait, FIRST_COMPLETED
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
    # When the task last changed level (or was queued): starvation is counted from
    # here, so a task is promoted one level per threshold - not one level on every
    # dequeue once it has waited long enough. ``enqueue_time`` stays the FIFO key.
    waiting_since: float = field(compare=False, default=0.0)

    def __post_init__(self) -> None:
        if not isinstance(self.priority, TaskPriority):
            try:
                # Accept a bare int, coerced so ``.name`` works
                self.priority = TaskPriority(int(self.priority))
            except (TypeError, ValueError):
                valid = [f"{p.name}={p.value}" for p in TaskPriority]
                raise ValueError(
                    f"Invalid task priority {self.priority!r}; expected one of {valid}."
                ) from None


class PriorityTaskQueue:
    """
    Priority-ordered task queue with starvation prevention.

    Tasks are ordered by (priority, enqueue_time). Within the same priority
    level, tasks are FIFO. A task that has waited longer than
    ``starvation_threshold`` seconds at its level is promoted one level (and its
    waiting clock restarts, so it is promoted again only after another threshold).

    A ``max_size`` limit provides backpressure: ``enqueue()`` raises
    ``QueueFull`` when the limit is reached.

    A task that has been dequeued but not yet handed to the executor is *in
    transit*: it is still counted by :meth:`pending_count` and can still be
    cancelled (:meth:`cancel_context`), so there is no window in which a task
    belongs to neither the queue nor the executor.

    Thread-safe. All public methods acquire the internal lock.
    """

    class QueueFull(Exception):
        """Raised by enqueue() when the queue is at capacity."""

    def __init__(
        self,
        starvation_threshold: float = 60.0,
        max_size: int = 0,
    ) -> None:
        """
        Args:
            starvation_threshold:
                Seconds a task may wait at one level before being promoted one
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
        self._cond = threading.Condition(threading.Lock())
        self._stopped: bool = False
        # dequeued, not yet handed over: task_id -> task; and those cancelled meanwhile
        self._in_transit: Dict[str, QueueTask] = {}
        self._cancelled_in_transit: Set[str] = set()

    @property
    def size(self) -> int:
        with self._cond:
            return len(self._heap)

    @property
    def stopped(self) -> bool:
        with self._cond:
            return self._stopped

    @property
    def stats(self) -> Dict[str, Any]:
        with self._cond:
            by_priority: Dict[str, int] = {}
            for p in TaskPriority:
                by_priority[p.name.lower()] = sum(
                    1 for t in self._heap if t.priority == p
                )
            return {
                "queue_depth": len(self._heap),
                "in_transit": len(self._in_transit),
                "total_enqueued": self._total_enqueued,
                "total_dequeued": self._total_dequeued,
                "max_size": self._max_size or "unlimited",
                "priorities": by_priority,
            }

    def pending_count(self, context_id: str) -> int:
        """Tasks of ``context_id`` that are queued or in transit (not yet handed over)."""
        with self._cond:
            queued = sum(1 for t in self._heap if t.context_id == context_id)
            moving = sum(
                1 for t in self._in_transit.values() if t.context_id == context_id
            )
            return queued + moving

    def enqueue(self, task: QueueTask) -> None:
        """
        Add a task to the queue. Thread-safe.

        Raises:
            PriorityTaskQueue.QueueFull:
                If max_size is set and the queue is at capacity.
            RuntimeError:
                If the queue has been stopped.
        """
        with self._cond:
            if self._stopped:
                raise RuntimeError("Cannot enqueue to a stopped PriorityTaskQueue.")
            if self._max_size and len(self._heap) >= self._max_size:
                raise PriorityTaskQueue.QueueFull(
                    f"Queue is full ({self._max_size} tasks). "
                    "Apply backpressure or increase max_size."
                )
            task.enqueue_time = task.waiting_since = time.monotonic()
            heapq.heappush(self._heap, task)
            self._total_enqueued += 1
            depth = len(self._heap)
            self._cond.notify()

        logger.debug(
            "Task %s enqueued (priority=%s source=%s depth=%d)",
            task.task_id,
            task.priority.name,
            task.source,
            depth,
        )

    def dequeue(self, timeout: Optional[float] = None) -> Optional[QueueTask]:
        """
        Remove and return the highest-priority task.

        Blocks until a task is available, ``stop()`` is called, or ``timeout``
        seconds pass. Returns ``None`` when the queue has been stopped and is
        empty (see :attr:`stopped`), or when the timeout elapsed.

        The task is *in transit* until :meth:`finish_dispatch` is called.
        Thread-safe.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while True:
                if self._heap:
                    self._promote_starving_tasks()
                    task = heapq.heappop(self._heap)
                    self._total_dequeued += 1
                    self._in_transit[task.task_id] = task
                    logger.debug(
                        "Task %s dequeued (priority=%s waited=%.2fs remaining=%d)",
                        task.task_id,
                        task.priority.name,
                        time.monotonic() - task.enqueue_time,
                        len(self._heap),
                    )
                    return task
                if self._stopped:
                    return None
                if deadline is None:
                    self._cond.wait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                    self._cond.wait(remaining)

    def is_cancelled(self, task: QueueTask) -> bool:
        """True if ``task`` was cancelled while in transit."""
        with self._cond:
            return task.task_id in self._cancelled_in_transit

    def finish_dispatch(self, task: QueueTask) -> None:
        """The task has reached the executor (or was dropped): it is no longer in transit."""
        with self._cond:
            self._in_transit.pop(task.task_id, None)
            self._cancelled_in_transit.discard(task.task_id)

    def remove_task(self, task_id: str) -> Optional[QueueTask]:
        """
        Remove a specific queued task by task_id.
        Returns the removed task, or None if it is not queued.
        Thread-safe.
        """
        with self._cond:
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
        Remove all queued (not yet dequeued) tasks for a given context_id.
        Returns the list of removed tasks.
        Thread-safe.
        """
        removed, _ = self.cancel_context(context_id, flag_in_transit=False)
        return removed

    def cancel_context(
        self, context_id: str, *, flag_in_transit: bool = True
    ) -> Tuple[List[QueueTask], int]:
        """
        Remove the queued tasks of ``context_id`` and flag those in transit as
        cancelled, so the dispatcher drops them instead of submitting them.

        Returns ``(removed_tasks, flagged_in_transit)``.
        """
        with self._cond:
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

            flagged = 0
            if flag_in_transit:
                for task in self._in_transit.values():
                    if (
                        task.context_id == context_id
                        and task.task_id not in self._cancelled_in_transit
                    ):
                        self._cancelled_in_transit.add(task.task_id)
                        flagged += 1

        if removed:
            logger.debug(
                "Removed %d queued task(s) for context=%s",
                len(removed),
                context_id[:8],
            )
        return removed, flagged

    def clear(self) -> List[QueueTask]:
        """Remove and return every queued task (those in transit are flagged cancelled)."""
        with self._cond:
            removed, self._heap = self._heap, []
            self._cancelled_in_transit.update(self._in_transit)
            return removed

    def stop(self) -> None:
        """
        Signal the queue to stop. Unblocks any waiting ``dequeue()`` calls.
        After stop(), ``dequeue()`` returns tasks until the queue is empty, then
        None. Safe to call multiple times.
        """
        with self._cond:
            self._stopped = True
            self._cond.notify_all()

    def _promote_starving_tasks(self) -> None:
        """
        Promote tasks that have waited beyond the starvation threshold at their
        level by one priority level, and restart their waiting clock.

        Must be called with the lock held. The heap is re-heapified once after the
        in-place changes; it is typically small, so O(n) is acceptable.
        """
        if not self._starvation_threshold:
            return

        now = time.monotonic()
        changed = False

        for task in self._heap:
            if task.priority == TaskPriority.CRITICAL:
                continue  # Already at maximum urgency
            wait_time = now - task.waiting_since
            if wait_time > self._starvation_threshold:
                old_priority = task.priority
                task.priority = task.priority.promote()
                task.waiting_since = now  # one level per threshold
                logger.info(
                    "Task %s promoted %s → %s after %.1fs waiting",
                    task.task_id,
                    old_priority.name,
                    task.priority.name,
                    wait_time,
                )
                changed = True

        if changed:
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
                # All state is created here, once, under the singleton lock.
                # (It used to be created in __init__, which Python runs on every
                # VolnuxPoolManager() call: two threads making the first calls
                # could each replace the other's lock and registry.)
                instance = object.__new__(cls)
                instance._executor = None
                instance._backend_type = None
                instance._initialized = False
                instance._shut_down = False
                instance._state_lock = threading.Lock()
                instance._registry = _ContextFutureRegistry()
                instance._task_queue = None
                instance._dispatch_thread = None
                instance._dispatch_started = False
                # In-flight limit: the dispatcher waits for a free slot before it takes
                # the next task, which is what makes priority order matter.
                instance._slots = None
                instance._max_in_flight = 0
                # Held while a task goes from the queue to the executor, and by
                # cancel_context, so a cancel can never miss a task half-way.
                instance._dispatch_lock = threading.Lock()
                cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        # State is created in __new__; __init__ runs on every call and does nothing.
        pass

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
        max_in_flight: Optional[int] = None,
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
            max_in_flight:
                Most tasks handed to the executor at once. The dispatcher takes
                the next task from the priority queue only when one of these
                slots is free, so priority order, starvation promotion and
                cancellation of queued tasks apply to every task that is waiting
                for a worker. Default: ``max_workers`` (local); unlimited for
                celery (its workers are elsewhere). 0 = unlimited.

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

            if max_in_flight is not None and max_in_flight < 0:
                raise ValueError(f"max_in_flight must be >= 0, got {max_in_flight}.")

            in_flight_limit = max_in_flight or 0
            if backend == "local":
                if max_workers is not None and max_workers < 1:
                    raise ValueError(f"max_workers must be >= 1, got {max_workers}.")
                num_workers = max_workers or multiprocessing.cpu_count()
                if max_in_flight is None:
                    in_flight_limit = num_workers
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
            self._max_in_flight = in_flight_limit
            self._slots = (
                threading.BoundedSemaphore(in_flight_limit) if in_flight_limit else None
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
        Block until every task of ``context`` has finished: those still waiting in
        the priority queue (they used to be ignored, so a drain could return with
        the work not yet started) and those in flight.

        Raises:
            TimeoutError: If timeout elapses before all tasks settle.
        """
        ctx_id = context.state_id
        deadline = None if timeout is None else time.monotonic() + timeout
        waited: Set[Future] = set()

        while True:
            # Queued first, then the futures: a task is registered before it stops
            # being pending, so a task that is not pending any more is in the second
            # read. (The other order let a task slip between the two reads.)
            queued = self._task_queue.pending_count(ctx_id) if self._task_queue else 0
            futures = self._registry.snapshot(ctx_id) - waited
            if not futures and not queued:
                break

            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(
                    f"drain_context timed out for context {ctx_id!r}: "
                    f"{len(futures) + queued} task(s) still running or queued."
                )

            if futures:
                logger.debug(
                    "Draining context=%s — %d in-flight task(s), %d queued.",
                    ctx_id[:8],
                    len(futures),
                    queued,
                )
                # Wait in short slices while tasks are still queued, so those are
                # noticed as they are dispatched.
                slice_ = remaining
                if queued:
                    slice_ = 0.05 if remaining is None else min(0.05, remaining)
                done, _ = wait(futures, timeout=slice_)
                waited |= done
            else:
                time.sleep(
                    0.01 if remaining is None else max(0.0, min(0.01, remaining))
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

        # Under the dispatch lock a task is either still the queue's (removed or
        # flagged here) or already the executor's (registered, so cancelled below):
        # a cancel can no longer fall between the two.
        with self._dispatch_lock:
            queued_removed, flagged = (
                self._task_queue.cancel_context(ctx_id)
                if self._task_queue is not None
                else ([], 0)
            )
            in_flight = self._registry.pop_all(ctx_id)
        cancelled += len(queued_removed) + flagged

        for future in in_flight:
            if (
                terminate
                and CeleryExecutor is not None
                and isinstance(self._executor, CeleryExecutor)
            ):
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
            len(queued_removed) + flagged,
            cancelled - len(queued_removed) - flagged,
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
        Wait until at least one dispatched task of ``context`` finishes (normally,
        by raising, or cancelled).

        (It waited for the first *exception* - which, if nothing raised, meant for
        every task - although it is documented to return as soon as one finishes.)
        Tasks still waiting in the queue have no future yet and are not waited for.

        Returns:
            (done, not_done): Sets of futures in each state.
        """
        futures = self._registry.snapshot(context.state_id)
        if not futures:
            return set(), set()
        return wait(futures, timeout=timeout, return_when=FIRST_COMPLETED)

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
                # contexts with something in flight (not the number of tasks)
                "active_count": len(self._registry.all_context_ids()),
            },
            "in_flight": {
                "tasks": sum(
                    self._registry.active_count(c)
                    for c in self._registry.all_context_ids()
                ),
                "limit": self._max_in_flight or "unlimited",
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
            wait_for_tasks: Run what is already queued and wait for everything in
                flight. When False the tasks still waiting in the priority queue are
                dropped (they used to be pushed at an executor that was shutting
                down, and failed there).
            cancel_futures: Drop the queued tasks and cancel the executor's pending
                futures.

        Safe to call multiple times.
        """
        with self._state_lock:
            if self._shut_down:
                return
            self._shut_down = True

        dropped: List[QueueTask] = []
        if self._task_queue is not None:
            if cancel_futures or not wait_for_tasks:
                dropped = self._task_queue.clear()
            # Stop: the dispatcher finishes what is left, then exits
            self._task_queue.stop()
        if dropped:
            logger.info("Shutdown dropped %d queued task(s).", len(dropped))

        # The dispatcher must be gone before the executor is, or it would submit to
        # a closed one. With tasks to run it may take as long as they do.
        thread = self._dispatch_thread
        if (
            thread is not None
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=None if wait_for_tasks and not cancel_futures else 10.0)
            if thread.is_alive():
                logger.warning(
                    "Dispatch thread did not exit in time — continuing shutdown."
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

    def _release_slot(self) -> None:
        slots = self._slots
        if slots is None:
            return
        try:
            slots.release()
        except ValueError:  # released more often than acquired (e.g. after a reset)
            pass

    def _on_done(self, ctx_id: str, future: Future) -> None:
        """A task finished (or was cancelled): forget it and free its slot."""
        self._registry.discard(ctx_id, future)
        self._release_slot()

    def _dispatch_loop(self) -> None:
        """
        Take tasks from the priority queue, in order, and hand them to the executor.

        Runs in a background thread. A task is taken only when an in-flight slot is
        free (``max_in_flight``), so the executor's own FIFO queue never holds more
        than the pool can run and the priority order of what is waiting is decided
        here, when a worker is actually available. The future of each task is
        registered under its context ID so drain_context() and cancel_context() can
        track it, and its slot is given back when it finishes.

        Exits cleanly when the queue is stopped and empty.
        """
        queue = self._task_queue
        logger.debug(
            "Dispatch loop running on thread %s.", threading.current_thread().name
        )

        while True:
            # A free slot first, then the best waiting task.
            slots = self._slots
            if slots is not None:
                acquired = False
                while not acquired:
                    acquired = slots.acquire(timeout=0.1)
                    if not acquired and queue.stopped and queue.size == 0:  # type: ignore[union-attr]
                        break
                if not acquired:
                    logger.debug("Dispatch loop received stop signal — exiting.")
                    break

            task = queue.dequeue()  # type: ignore[union-attr]
            if task is None:
                # Queue has been stopped and is empty — exit cleanly
                self._release_slot()
                logger.debug("Dispatch loop received stop signal — exiting.")
                break

            submitted = False
            try:
                with self._dispatch_lock:
                    if queue.is_cancelled(task):  # type: ignore[union-attr]
                        logger.debug("Task %s cancelled in transit.", task.task_id)
                    elif self._executor is None:
                        logger.warning(
                            "Task %s dropped — executor is gone.", task.task_id
                        )
                    else:
                        future = self._executor.submit(
                            task.dispatch_fn, *task.args, **task.kwargs
                        )
                        submitted = True
                        self._registry.add(task.context_id, future)
                        future.add_done_callback(
                            lambda f, ctx_id=task.context_id: self._on_done(ctx_id, f)
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
            finally:
                # (only now is the task no longer pending: it is registered)
                queue.finish_dispatch(task)  # type: ignore[union-attr]
                if not submitted:
                    self._release_slot()  # nothing will run, so nothing to wait for

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
