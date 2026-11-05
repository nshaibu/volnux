from __future__ import annotations

import collections
import concurrent.futures
import logging
import threading
import time
from typing import Any, Callable, Optional

from celery import Celery
from celery.app.control import Control
from celery.exceptions import TimeoutError as CeleryTimeoutError
from celery.result import AsyncResult

#: ``AsyncResult.get(timeout=...)`` raises Celery's own TimeoutError, which is not a
#: ``concurrent.futures.TimeoutError``: a caller written for a Future never caught it.
_TIMEOUT_ERRORS = (TimeoutError, CeleryTimeoutError)

#: Task ids remembered after their future finished (for revoke / invoke).
_RECENT_TASK_IDS = 4096

logger = logging.getLogger(__name__)


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

        An exception raised inside `func` propagates, so Celery marks the task
        FAILED and stores the traceback - which CeleryFuture.result() surfaces to
        the caller. (It used to be wrapped in ``self.retry(exc=exc, max_retries=0)``,
        which only ever re-raised the same exception through Celery's retry
        machinery.)
        """
        return func(*args, **kwargs)

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
        self._cancel_locally()
        return True

    def _is_settled(self) -> bool:
        return self._state in (
            concurrent.futures._base.CANCELLED,
            concurrent.futures._base.CANCELLED_AND_NOTIFIED,
            concurrent.futures._base.FINISHED,
        )

    def _cancel_locally(self) -> None:
        """Mark this future cancelled (no call to Celery) and tell everyone.

        The state used to be set by hand, which woke ``result()`` callers and the
        callbacks but not the waiters ``concurrent.futures.wait`` / ``as_completed``
        sit on: they never saw the cancellation.
        """
        if self._is_settled():
            return  # a second cancel (or one after the outcome was recorded)
        if concurrent.futures.Future.cancel(self):  # state + callbacks
            self.set_running_or_notify_cancel()  # and the waiters

    def cancelled(self) -> bool:
        return super().cancelled() or self._async_result.state == "REVOKED"

    @property
    def async_result(self) -> AsyncResult:
        """The Celery ``AsyncResult`` behind this future."""
        return self._async_result

    def _sync_from_backend(self) -> bool:
        """Record the task's final outcome on this future and fire its callbacks.

        ``done()``/``result()`` read the result backend directly, so on their own
        they never notify anyone: asyncio's ``wrap_future`` (hence
        ``run_in_executor``) waits for callbacks that would never come. The
        executor's watcher calls this to close that gap.

        Returns True once the future is finished (by this or an earlier call).
        """
        state = self._async_result.state
        if state not in self._CELERY_DONE_STATES:
            return False
        if self._is_settled():
            return True

        try:
            if state == "SUCCESS":
                self.set_result(self._async_result.result)
            elif state == "FAILURE":
                error = self._async_result.result
                self.set_exception(
                    error
                    if isinstance(error, BaseException)
                    else RuntimeError(str(error))
                )
            else:  # REVOKED
                self._cancel_locally()
        except concurrent.futures.InvalidStateError:
            pass  # settled by someone else a moment ago
        return True

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
        except _TIMEOUT_ERRORS as exc:
            raise concurrent.futures.TimeoutError(str(exc)) from exc

    def exception(self, timeout: Optional[float] = None) -> Optional[BaseException]:
        """Return the exception raised by the task, or None on success."""
        if self.cancelled():
            return concurrent.futures.CancelledError()

        try:
            self._async_result.get(timeout=timeout, propagate=False)
            if self._async_result.failed():
                return self._async_result.result
            return None
        except _TIMEOUT_ERRORS as exc:
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
        poll_interval: float = 0.2,
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
        # ids of futures that finished (bounded), so revoking a task that has just
        # finished is not mistaken for revoking one this executor never submitted
        self._recent_ids: "collections.deque[str]" = collections.deque(
            maxlen=_RECENT_TASK_IDS
        )
        self._failing_ids: set[str] = set()  # tasks whose state could not be read
        # wakes the watcher early (a task finished, or the executor shut down), so it
        # neither sleeps through a whole poll interval nor outlives its work
        self._wake = threading.Event()
        self._shutdown = False
        self._poll_interval = poll_interval
        self._watcher: Optional[threading.Thread] = None

        self._remote_task = make_remote_task(celery_app)
        self._control: Control = celery_app.control

        for problem in self.configuration_problems():
            logger.warning("CeleryExecutor: %s", problem)

    def configuration_problems(self) -> list[str]:
        """What in the Celery app's configuration will stop this executor working.

        The callable travels in the task message and its return value in the result,
        so both need the ``pickle`` serializer, and workers must accept it. With the
        defaults (JSON) a submit fails to encode, or - worse - a worker drops the
        message, and the future never completes.
        """
        conf = self._celery_app.conf
        problems = []
        if conf.task_serializer != "pickle":
            problems.append(
                f"task_serializer is {conf.task_serializer!r}: a callable cannot be "
                "sent as JSON; set task_serializer='pickle'"
            )
        if conf.result_serializer != "pickle":
            problems.append(
                f"result_serializer is {conf.result_serializer!r}: results that are "
                "not JSON-serializable cannot be returned; set result_serializer='pickle'"
            )
        accepted = {str(c).lower() for c in (conf.accept_content or [])}
        if (
            "pickle" not in accepted
            and "application/x-python-serialize" not in accepted
        ):
            problems.append(
                "accept_content does not include 'pickle': workers will reject the "
                "tasks; set accept_content=['pickle'] (on the workers too)"
            )
        return problems

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
                if self._futures.pop(async_result.id, None) is not None:
                    self._recent_ids.append(async_result.id)
                self._failing_ids.discard(async_result.id)
            self._wake.set()

        # Registered first, then the callback: a future that finishes in between
        # would otherwise run the cleanup before it was in the dict and stay in it.
        with self._lock:
            self._futures[async_result.id] = future
        future.add_done_callback(_cleanup)
        self._ensure_watcher()

        return future

    def _ensure_watcher(self) -> None:
        """Make sure a thread is resolving finished tasks onto their futures."""
        with self._lock:
            if self._watcher is not None and self._watcher.is_alive():
                return
            self._watcher = threading.Thread(
                target=self._watch, name="volnux-celery-watcher", daemon=True
            )
            self._watcher.start()

    def _watch(self) -> None:
        """Poll the result backend and complete futures as their tasks finish.

        Exits when nothing is pending; the next ``submit`` starts a new one.
        """
        while True:
            with self._lock:
                pending = list(self._futures.values())
                if not pending:
                    self._watcher = None
                    return
            for future in pending:
                try:
                    future._sync_from_backend()
                except Exception:
                    # logged once per task, not on every poll
                    if future.task_id not in self._failing_ids:
                        self._failing_ids.add(future.task_id)
                        logger.exception(
                            "Could not read the state of Celery task %s", future.task_id
                        )
            self._wake.wait(self._poll_interval)
            self._wake.clear()

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
            futures = dict(self._futures)
        self._wake.set()

        # (the state of each future is read from the result backend: not under the lock)
        pending = {tid: fut for tid, fut in futures.items() if not self._is_done(fut)}

        if cancel_futures:
            for task_id, future in pending.items():
                self._control.revoke(task_id, terminate=False)
                # A task that has not started is cancelled now; waiting for a revoked
                # task nobody picks up (no worker is listening) never ended.
                if not self._is_running(future):
                    future._cancel_locally()

        if wait and pending:
            deadline = None if timeout is None else time.monotonic() + timeout

            for future in pending.values():
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break

                try:
                    future.result(timeout=remaining)
                except Exception:
                    pass

    @staticmethod
    def _is_running(future: "CeleryFuture") -> bool:
        try:
            return future.running()
        except Exception:
            return False

    @staticmethod
    def _is_done(future: "CeleryFuture") -> bool:
        """done(), but a result backend that cannot be read counts as not done (shutdown
        must not fail because the backend is down)."""
        try:
            return future.done()
        except Exception:
            return False

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
        if self._validate_task_id(task_id, allow_finished=True) == "finished":
            logger.debug("Task %s has already finished; nothing to revoke.", task_id)
            return

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

    def _validate_task_id(self, task_id: str, *, allow_finished: bool = False) -> str:
        """Check ``task_id`` came from this executor; return ``"pending"`` or ``"finished"``.

        A task that has just finished is no longer tracked, and revoking it used to
        fail with "was not submitted through this executor" - so cancelling a pool
        context raced with its tasks completing.
        """
        with self._lock:
            if task_id in self._futures:
                return "pending"
            if allow_finished and task_id in self._recent_ids:
                return "finished"
        raise ValueError(
            f"Task id {task_id!r} was not submitted through this executor"
            + (" (or it finished long ago)." if allow_finished else ".")
        )
