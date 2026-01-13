import asyncio
import logging
import concurrent.futures
import threading
from typing import Callable, Any, Optional
from functools import wraps

logger = logging.getLogger(__name__)


class AsyncExecutor(concurrent.futures.Executor):
    """
    A robust bridge between concurrent.futures and asyncio, supporting
    both sync and async callables with strict worker limits and graceful shutdown.
    """

    def __init__(
        self, max_workers: Optional[int] = None, shutdown_timeout: float = 10.0
    ):
        self._max_workers = max_workers
        self._shutdown_timeout = shutdown_timeout
        self._shutdown = False
        self._lock = threading.Lock()

        self._loop = asyncio.new_event_loop()
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="AsyncExecutor-SyncWorker"
        )
        self._loop.set_default_executor(self._thread_pool)

        # Limit concurrent async tasks
        self._semaphore = asyncio.Semaphore(max_workers) if max_workers else None

        # Start the event loop thread
        self._loop_thread = threading.Thread(
            target=self._run_event_loop, args=(self._loop,), daemon=True
        )
        self._loop_thread.start()

    def _run_event_loop(self, loop: asyncio.AbstractEventLoop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def submit(self, fn: Callable, *args, **kwargs) -> concurrent.futures.Future:
        with self._lock:
            if self._shutdown:
                logger.warning("AsyncExecutor has already been shutdown")
                raise RuntimeError("Cannot schedule new tasks after shutdown")

        future = concurrent.futures.Future()

        async def run_task():
            try:
                if self._semaphore:
                    async with self._semaphore:
                        result = await self._execute_callable(fn, *args, **kwargs)
                else:
                    result = await self._execute_callable(fn, *args, **kwargs)

                if not future.cancelled():
                    future.set_result(result)
            except Exception as e:
                logger.exception("Exception while executing task", exc_info=e)
                if not future.cancelled():
                    future.set_exception(e)

        asyncio.run_coroutine_threadsafe(run_task(), self._loop)
        return future

    async def _execute_callable(self, fn: Callable, *args, **kwargs) -> Any:
        if asyncio.iscoroutinefunction(fn):
            return await fn(*args, **kwargs)
        else:
            # Runs in the bounded self._thread_pool
            return await self._loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    def map(self, fn: Callable, *iterables, timeout: Optional[float] = None):
        futures = [self.submit(fn, *args) for args in zip(*iterables)]

        def result_iterator():
            for f in futures:
                yield f.result(timeout=timeout)

        return result_iterator()

    def shutdown(self, wait: bool = True, cancel_futures: bool = False):
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True

        def _internal_cleanup():
            if cancel_futures:
                for task in asyncio.all_tasks(self._loop):
                    task.cancel()
            self._loop.stop()

        if self._loop.is_running():
            self._loop.call_soon_threadsafe(_internal_cleanup)

        # Shut down the internal thread pool
        self._thread_pool.shutdown(wait=False)

        if wait and self._loop_thread:
            self._loop_thread.join(timeout=self._shutdown_timeout)
            if self._loop_thread.is_alive():
                logger.warning(
                    f"AsyncExecutor shutdown timed out after {self._shutdown_timeout}s."
                )

        if not self._loop.is_closed():
            try:
                # Close the loop only after the thread has likely finished
                self._loop.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(wait=True)
        return False
