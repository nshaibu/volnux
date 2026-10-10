from __future__ import annotations

import concurrent.futures
import threading
from typing import Any, Callable, Optional

from celery import Celery
from celery.app.control import Control
from celery.result import AsyncResult


def make_remote_task(celery_app: Celery):
    """
    Registers and returns the generic remote-execution task bound to the
    given Celery app. Defined as a factory so that callers can supply their
    own app instance rather than relying on a module-level singleton.
    """

    @celery_app.task(bind=True, name="celery_executor.execute_remotely")
    def execute_remotely(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """
        Execute any picklable callable on a Celery worker.

        Exceptions raised inside `func` are re-raised here so that Celery
        marks the task as FAILED and stores the traceback — which
        CeleryFuture.result() will then surface to the caller.
        """
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            # Retry=False prevents automatic retries; re-raise so Celery
            # records the failure properly.
            raise self.retry(exc=exc, max_retries=0)

    return execute_remotely


class CeleryFuture(concurrent.futures.Future):
    """
    A :class:`concurrent.futures.Future` backed by a Celery
    :class:`~celery.result.AsyncResult`.

    The standard Future interface (``result()``, ``cancel()``,
    ``done()``, ``add_done_callback()``, …) is preserved so that
    CeleryExecutor is a drop-in replacement for
    :class:`~concurrent.futures.ThreadPoolExecutor`.
    """

    # Celery state → Future state mapping
    _CELERY_DONE_STATES = {"SUCCESS", "FAILURE", "REVOKED"}

    def __init__(self, async_result: AsyncResult) -> None:
        super().__init__()
        self._async_result = async_result

    @property
    def task_id(self) -> str:
        """The underlying Celery task id."""
        return self._async_result.id

    def cancel(self) -> bool:
        """
        Attempt to cancel the task.

        Returns True if the task was successfully revoked (i.e. it had not
        yet started executing on a worker). Returns False if the task is
        already running or completed.
        """
        state = self._async_result.state
        if state in self._CELERY_DONE_STATES or state == "STARTED":
            return False

        self._async_result.revoke(terminate=False)
        with self._condition:
            if self._state not in (
                concurrent.futures._base.CANCELLED,
                concurrent.futures._base.CANCELLED_AND_NOTIFIED,
                concurrent.futures._base.FINISHED,
            ):
                self._state = concurrent.futures._base.CANCELLED
                self._condition.notify_all()
        self._invoke_callbacks()
        return True

    def cancelled(self) -> bool:
        return super().cancelled() or self._async_result.state == "REVOKED"

    def running(self) -> bool:
        return self._async_result.state == "STARTED"

    def done(self) -> bool:
        return self._async_result.state in self._CELERY_DONE_STATES

    def result(self, timeout: Optional[float] = None) -> Any:
        """
        Block until the task completes and return its return value.

        Raises the original exception if the task failed.
        Raises `concurrent.futures.TimeoutError` on timeout.
        Raises `concurrent.futures.CancelledError` if revoked.
        """
        if self.cancelled():
            raise concurrent.futures.CancelledError()

        try:
            return self._async_result.get(timeout=timeout, propagate=True)
        except TimeoutError as exc:
            raise concurrent.futures.TimeoutError(str(exc)) from exc
        except Exception:
            raise

    def exception(self, timeout: Optional[float] = None) -> Optional[BaseException]:
        """Return the exception raised by the task, or None on success."""
        if self.cancelled():
            return concurrent.futures.CancelledError()

        try:
            self._async_result.get(timeout=timeout, propagate=False)
            if self._async_result.failed():
                return self._async_result.result
            return None
        except TimeoutError as exc:
            raise concurrent.futures.TimeoutError(str(exc)) from exc


class CeleryExecutor(concurrent.futures.Executor):
    """
    A `concurrent.futures.Executor` that dispatches callables to
    Celery worker queues.
    """

    def __init__(
        self,
        celery_app: Celery,
        queue: Optional[str] = None,
        *,
        task_soft_time_limit: Optional[int] = None,
        task_time_limit: Optional[int] = None,
    ) -> None:
        if not isinstance(celery_app, Celery):
            raise TypeError(
                f"celery_app must be a Celery instance, got {type(celery_app)!r}"
            )

        self._celery_app = celery_app
        self._queue = queue
        self._soft_time_limit = task_soft_time_limit
        self._time_limit = task_time_limit

        self._lock = threading.Lock()
        self._futures: dict[str, CeleryFuture] = {}
        self._shutdown = False

        self._remote_task = make_remote_task(celery_app)
        self._control: Control = celery_app.control

    def submit(
        self,
        fn: Callable,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> CeleryFuture:
        if self._shutdown:
            raise RuntimeError(
                "Cannot submit new tasks after executor has been shut down."
            )
        if not callable(fn):
            raise TypeError(f"fn must be callable, got {type(fn)!r}")

        apply_kwargs: dict[str, Any] = {"args": (fn,) + args, "kwargs": kwargs}
        if self._queue is not None:
            apply_kwargs["queue"] = self._queue
        if self._soft_time_limit is not None:
            apply_kwargs["soft_time_limit"] = self._soft_time_limit
        if self._time_limit is not None:
            apply_kwargs["time_limit"] = self._time_limit

        async_result: AsyncResult = self._remote_task.apply_async(**apply_kwargs)
        future = CeleryFuture(async_result)

        def _cleanup(_done_future: concurrent.futures.Future) -> None:
            with self._lock:
                self._futures.pop(async_result.id, None)

        future.add_done_callback(_cleanup)

        with self._lock:
            self._futures[async_result.id] = future

        return future

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Signal the executor that no more tasks will be submitted.

        Params:
        wait:
            If True, block until all pending futures are done.
        cancel_futures:
            If True, attempt to revoke all unfinished tasks before waiting.
        timeout:
            Maximum number of seconds to wait for unfinished futures.
            ``None`` means wait indefinitely.
        """
        with self._lock:
            self._shutdown = True
            pending = {tid: fut for tid, fut in self._futures.items() if not fut.done()}

        if cancel_futures:
            for task_id in pending:
                self._control.revoke(task_id, terminate=False)

        if wait and pending:
            deadline = (
                None if timeout is None else (__import__("time").monotonic() + timeout)
            )

            for future in pending.values():
                remaining = None
                if deadline is not None:
                    remaining = deadline - __import__("time").monotonic()
                    if remaining <= 0:
                        break

                try:
                    future.result(timeout=remaining)
                except Exception:
                    pass

    def invoke(
        self,
        task_id: str,
        signal: str,
        *,
        reply: bool = False,
        timeout: Optional[float] = 1.0,
        **kwargs: Any,
    ) -> Optional[list]:
        """
        Send a custom broadcast signal/event to the worker processing
        *task_id*.

        This is a broadcast mechanism; worker-side handlers are responsible
        for filtering by task id.
        """
        self._validate_task_id(task_id)

        return self._control.broadcast(
            signal,
            arguments={"task_id": task_id, **kwargs},
            reply=reply,
            timeout=timeout,
        )

    def revoke(
        self,
        task_id: str,
        *,
        terminate: bool = False,
        signal: str = "SIGTERM",
        countdown: Optional[int] = None,
    ) -> None:
        self._validate_task_id(task_id)

        revoke_kwargs: dict[str, Any] = {"terminate": terminate, "signal": signal}
        if countdown is not None:
            revoke_kwargs["countdown"] = countdown

        self._control.revoke(task_id, **revoke_kwargs)

        with self._lock:
            future = self._futures.get(task_id)
        if future is not None:
            future.cancel()

    def futures(self) -> dict[str, CeleryFuture]:
        """Return a snapshot of all submitted futures keyed by task id."""
        with self._lock:
            return dict(self._futures)

    def pending_task_ids(self) -> list[str]:
        """Return the task ids of all futures that are not yet done."""
        with self._lock:
            return [tid for tid, f in self._futures.items() if not f.done()]

    def _validate_task_id(self, task_id: str) -> None:
        with self._lock:
            if task_id not in self._futures:
                raise ValueError(
                    f"Task id {task_id!r} was not submitted through this executor."
                )
