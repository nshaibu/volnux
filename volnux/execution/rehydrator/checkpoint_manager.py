import asyncio
import logging
import weakref
import typing
import random
from collections import defaultdict

from .protocol import Monitorable, Snapshot

if typing.TYPE_CHECKING:
    from ..resilience.protocols import TaskInfo

logger = logging.getLogger(__name__)


class PeekableQueue(asyncio.Queue):
    """A queue that supports peeking at the front element."""

    async def peek(self) -> typing.Optional[typing.Any]:
        """Peek at the front element of the queue without removing it."""
        if self.empty():
            return None
        return self._queue[0]


class VolnuxCheckPointManager:
    """
    Manages checkpoints for monitoring and persisting application state.

    The VolnuxCheckPointManager is designed to handle periodic state snapshotting,
    queued persistence operations, and long-lived monitoring of execution contexts.
    It supports concurrency, retry mechanisms, and snapshot expiration policies,
    while offloading persistence to an asynchronous worker.

    :ivar checkpoint_interval: Interval in seconds between periodic state snapshots.
    :type checkpoint_interval: float
    :ivar retry_attempts: Number of retry attempts for persistence operations.
    :type retry_attempts: int
    :ivar retry_delay: Delay in seconds between retry attempts for persistence operations.
    :type retry_delay: float
    :ivar snapshot_ttl: Time-to-live (in seconds) for captured snapshots in the state store.
    :type snapshot_ttl: int
    """

    def __init__(
        self,
        *,
        checkpoint_interval: float = 5.0,
        max_concurrent: int = 5,
        retry_attempts: int = 3,
        retry_delay: float = 1.0,
        snapshot_ttl: int = 3600,
        max_queue_size: int = 1000,
    ):
        self.checkpoint_interval = checkpoint_interval
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.snapshot_ttl = snapshot_ttl

        # Push Queue
        maxsize = max_queue_size if max_queue_size > 0 else 0
        self._queue: PeekableQueue[Snapshot] = PeekableQueue(maxsize=maxsize)

        # Monitored Set: For periodic snapshotting
        self._monitored_contexts: weakref.WeakSet[Monitorable] = weakref.WeakSet()
        self._context_error_counts: defaultdict[str, int] = defaultdict(int)
        self._max_context_errors = 5

        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._worker_task: typing.Optional[asyncio.Task] = None
        self._monitor_task: typing.Optional[asyncio.Task] = None
        self._running = False

    def enqueue(self, snapshot: Snapshot):
        """
        Enqueue a snapshot for persistence.
        :param snapshot: The snapshot data to be enqueued.
        :return: None
        """
        self._queue.put_nowait(snapshot)

    def monitor(self, context: Monitorable):
        """
        Standard 'Pull' interface for long-lived Monitorable contexts.

        Adds the specified context to the monitored contexts, enabling long-lived
        tracking and management of execution states related to the provided context.

        :param context: The monitorable context to be added to the monitored list.
          Must be an instance of Monitorable, such as ExecutionContext.
        :type context: Monitorable
        :return: None
        """
        self._monitored_contexts.add(context)

    def unmonitor(self, context: Monitorable):
        """
        Unmonitored the given monitorable context by discarding it from the set of
        monitored contexts.

        This method provides a standard 'Pull' interface for handling long-lived
        execution contexts. Once unmonitored, the specified context will no longer
        be tracked.

        :param context: The monitorable context to be unmonitored.
        :type context: Monitorable
        :return: None
        """
        self._monitored_contexts.discard(context)

    async def start(self):
        """Start both the consumer and the periodic monitor."""
        if self._running:
            logger.warning("CheckpointManager already running")
            return

        self._running = True
        await self._start_worker()
        await self._start_monitor()
        logger.info("CheckpointManager started")

    async def _start_worker(self):
        """Start the persistence worker task (Restartable protocol)."""
        if self._worker_task and not self._worker_task.done():
            logger.debug("Worker task already running")
            return

        self._worker_task = asyncio.create_task(
            self._persistence_loop(), name="checkpoint-worker"
        )
        logger.debug("Persistence worker started")

    async def _start_monitor(self):
        """Start the periodic monitor task (Restartable protocol)."""
        if self._monitor_task and not self._monitor_task.done():
            logger.debug("Monitor task already running")
            return

        self._monitor_task = asyncio.create_task(
            self._periodic_monitor(), name="checkpoint-monitor"
        )
        logger.debug("Periodic monitor started")

    async def _periodic_monitor(self):
        while self._running:
            await asyncio.sleep(self.checkpoint_interval)

            for context in list(self._monitored_contexts):
                context_id = getattr(context, "id", id(context))
                try:
                    # Offload the work to the queue
                    snapshot = await context.create_snapshot()
                    self.enqueue(snapshot)

                    # Reset error count
                    self._context_error_counts.pop(context_id, None)
                except Exception as e:
                    self._context_error_counts[context_id] += 1
                    error_count = self._context_error_counts[context_id]

                    logger.error(
                        f"Failed to snapshot context {context_id} "
                        f"({error_count}/{self._max_context_errors}): {e}"
                    )

                    # Remove context if it's failing repeatedly
                    if error_count >= self._max_context_errors:
                        logger.warning(
                            f"Removing context {context_id} from monitoring "
                            f"after {error_count} consecutive failures"
                        )
                        self.unmonitor(context)
                        self._context_error_counts.pop(context_id, None)

    async def get_latest_snapshot(self) -> typing.Optional[Snapshot]:
        """
        Get the latest snapshot from the queue, skipping duplicates.

        Returns:
            Snapshot: The latest snapshot is available, or None if the queue is empty.
        """
        if self._queue.empty():
            return None

        latest = await self._queue.get()
        self._queue.task_done()

        while not self._queue.empty():
            next_snapshot = await self._queue.peek()

            if next_snapshot.id == latest.id:
                latest = await self._queue.get()
                self._queue.task_done()
            else:
                break

        return latest

    async def _persistence_loop(self):
        """The single consumer for all persistence requests."""
        while self._running:
            snapshot = await self.get_latest_snapshot()
            if snapshot is None:
                continue
            async with self._semaphore:
                await self._persist_with_retry(snapshot)

    async def _persist_with_retry(self, snapshot: Snapshot):
        """Implements your original retry logic with backoff."""
        last_error = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                await snapshot.save_async(ttl=self.snapshot_ttl)
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last_error = e

                base_delay = self.retry_delay * (2 ** (attempt - 1))
                jitter = random.uniform(0, base_delay * 0.1)  # 10% jitter
                delay = min(base_delay + jitter, 30.0)  # Cap at 30 seconds

                logger.warning(
                    f"Persist failed for {snapshot.id} (attempt {attempt}/{self.retry_attempts}): {e}"
                    f"Retrying in {delay:.2f}s"
                )

                # if attempt < self.retry_attempts:
                await asyncio.sleep(delay)

        logger.error(
            f"Persistence failed after {self.retry_attempts} tries: {last_error}"
        )

    async def flush(self):
        """The Preemption Barrier: Ensures the queue is empty before a swap."""
        await self._queue.join()

    def get_managed_tasks(self) -> typing.Dict[str, "TaskInfo"]:
        """
        Get information about all managed tasks.

        Returns:
            Dictionary mapping task names to TaskInfo
        """
        return {
            "worker": {
                "alive": self._worker_task is not None and not self._worker_task.done(),
                "task": self._worker_task,
                "restart_function": "_start_worker",
            },
            "monitor": {
                "alive": self._monitor_task is not None
                and not self._monitor_task.done(),
                "task": self._monitor_task,
                "restart_function": "_start_monitor",
            },
        }

    async def restart_task(self, task_name: str) -> None:
        """
        Restart a specific task by name.

        Args:
            task_name: Name of the task to restart

        Raises:
            ValueError: If task_name is not recognized,
            RuntimeError: If restart fails
        """
        if task_name == "worker":
            await self._start_worker()
        elif task_name == "monitor":
            await self._start_monitor()
        else:
            raise ValueError(
                f"Unknown task name: {task_name}. "
                f"Valid tasks: {list(self.get_managed_tasks().keys())}"
            )

    def is_healthy(self) -> bool:
        """Check if the checkpoint manager is healthy."""
        if not self._running:
            return False

        tasks = self.get_managed_tasks()
        return all(task["alive"] for task in tasks.values())

    async def health_check(self) -> typing.Dict[str, typing.Any]:
        """Comprehensive health check."""
        tasks = self.get_managed_tasks()

        return {
            "healthy": self.is_healthy(),
            "running": self._running,
            "tasks": {name: {"alive": info["alive"]} for name, info in tasks.items()},
            "queue_size": self._queue.qsize(),
            "queue_max_size": self._queue.maxsize,
            "monitored_contexts": len(list(self._monitored_contexts)),
        }

    async def stop(self):
        """
        Stop the checkpoint manager gracefully.

        Cancels monitoring, flushes pending checkpoints, and stops the worker.
        """
        if not self._running:
            return

        self._running = False
        logger.info("Stopping checkpoint manager...")

        # Cancel monitor task
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            logger.debug("Monitor task stopped")

        logger.info(f"Flushing {self._queue.qsize()} pending checkpoints...")
        await self.flush()

        # Cancel worker task
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            logger.debug("Worker task stopped")

        logger.info("Checkpoint manager stopped successfully")
