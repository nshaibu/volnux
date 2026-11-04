import asyncio
import threading
from collections import deque
from typing import Any, Deque, Generic, Optional, Tuple, TypeVar, TYPE_CHECKING

from .base import CommandChannelBase

if TYPE_CHECKING:
    from ..base import TaskCommand, TaskMessage

T = TypeVar("T")


class ThreadSafeAsyncQueue(Generic[T]):
    """An unbounded FIFO usable from any thread and any event loop.

    ``asyncio.Queue`` is bound to one loop and is not thread safe, but an event
    run by a thread executor lives on a private loop while its channel was made
    on the engine loop. Items live in a lock-protected deque; a waiter parks a
    future on *its own* loop and ``put`` wakes it with ``call_soon_threadsafe``.
    """

    def __init__(self) -> None:
        self._items: Deque[T] = deque()
        self._lock = threading.Lock()
        self._waiters: Deque[
            Tuple[asyncio.AbstractEventLoop, "asyncio.Future[None]"]
        ] = deque()

    def put_nowait(self, item: T) -> None:
        with self._lock:
            self._items.append(item)
        self._wake_one()

    def _wake_one(self) -> None:
        """Wake the first waiter that can still be woken."""
        while True:
            with self._lock:
                if not self._items or not self._waiters:
                    return
                loop, future = self._waiters.popleft()
            try:
                loop.call_soon_threadsafe(self._resolve, future)
                return
            except RuntimeError:  # that waiter's loop is closed: try the next
                continue

    @staticmethod
    def _resolve(future: "asyncio.Future[None]") -> None:
        if not future.done():
            future.set_result(None)

    def _forget(self, loop: asyncio.AbstractEventLoop, future: Any) -> None:
        with self._lock:
            try:
                self._waiters.remove((loop, future))
            except ValueError:
                pass

    async def get(self, timeout: Optional[float] = None) -> Optional[T]:
        """Next item, or ``None`` once ``timeout`` seconds pass (None = forever,
        0 = do not wait)."""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout

        while True:
            with self._lock:
                if self._items:
                    return self._items.popleft()
                if deadline is not None and deadline - loop.time() <= 0:
                    return None
                future = loop.create_future()
                self._waiters.append((loop, future))

            # Awaiting the future directly (with our own timer) rather than via
            # asyncio.wait_for: before Python 3.12 wait_for returns the result
            # and drops a cancellation that lands in the same loop iteration as
            # the completion, so a cancelled consumer would still take the item.
            timer = None
            if deadline is not None:
                timer = loop.call_later(
                    max(deadline - loop.time(), 0.0), self._resolve, future
                )
            try:
                await future
            except BaseException:
                # Cancelled while parked. If a put had already picked us to
                # wake, pass that wake-up on, so the item is not stranded
                # behind a waiter that will never consume it.
                self._forget(loop, future)
                self._wake_one()
                raise
            finally:
                if timer is not None:
                    timer.cancel()
            self._forget(loop, future)  # timer fired: leave the queue of waiters


class LocalCommandChannel(CommandChannelBase):
    """
    Channel for a task running in the *same process* as its coordinator: the
    default (async) executor, or a thread-pool executor.

    Both directions are in-memory thread-safe queues, so the task may consume
    commands on its own event loop in another thread.

    :ivar task_id: The unique identifier for the task associated with this channel.
    """

    poll_interval = 0.25

    def __init__(self, task_id: str):
        super().__init__(task_id)

        self._command_queue: ThreadSafeAsyncQueue["TaskCommand"] = (
            ThreadSafeAsyncQueue()
        )
        self._message_queue: ThreadSafeAsyncQueue["TaskMessage"] = (
            ThreadSafeAsyncQueue()
        )

    async def send_command(self, command: "TaskCommand") -> None:
        """Send command from coordinator to task"""
        self._command_queue.put_nowait(command)

    async def receive_command(
        self, timeout: Optional[float] = None
    ) -> Optional["TaskCommand"]:
        """Receive command in a task"""
        return await self._command_queue.get(timeout)

    async def send_message(self, message: "TaskMessage") -> None:
        """Send a message from a task to coordinator"""
        self._message_queue.put_nowait(message)

    async def receive_message(
        self, timeout: Optional[float] = None
    ) -> Optional["TaskMessage"]:
        """Receive a message in coordinator"""
        return await self._message_queue.get(timeout)
