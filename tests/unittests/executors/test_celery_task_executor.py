import concurrent.futures
from unittest.mock import Mock

import pytest
from celery import Celery

from volnux.executors.celery import CeleryExecutor, CeleryFuture


class FakeAsyncResult:
    def __init__(self, task_id: str, state: str = "PENDING", result=None):
        self.id = task_id
        self.state = state
        self.result = result
        self.get_calls = []
        self.revoke_calls = []

    def get(self, timeout=None, propagate=True):
        self.get_calls.append((timeout, propagate))
        if self.state == "FAILURE" and propagate:
            raise ValueError("worker failed")
        if self.state == "TIMEOUT":
            raise TimeoutError("timed out")
        return self.result

    def revoke(self, terminate=False):
        self.revoke_calls.append(terminate)
        self.state = "REVOKED"

    def failed(self):
        return self.state == "FAILURE"


class FakeTask:
    def __init__(self):
        self.apply_async_calls = []
        self._counter = 0

    def apply_async(self, **kwargs):
        self._counter += 1
        self.apply_async_calls.append(kwargs)
        return FakeAsyncResult(task_id=f"task-{self._counter}")


class FakeControl:
    def __init__(self):
        self.revoke_calls = []
        self.broadcast_calls = []

    def revoke(self, task_id, **kwargs):
        self.revoke_calls.append((task_id, kwargs))

    def broadcast(self, signal, arguments=None, reply=False, timeout=None):
        self.broadcast_calls.append(
            {
                "signal": signal,
                "arguments": arguments,
                "reply": reply,
                "timeout": timeout,
            }
        )
        return [{"worker": "ok"}] if reply else None


@pytest.fixture
def celery_app():
    app = Celery("test_app")
    app.conf.update(task_always_eager=False)
    app.control = FakeControl()
    return app


@pytest.fixture
def executor(celery_app):
    executor = CeleryExecutor(
        celery_app,
        queue="high",
        task_soft_time_limit=3,
        task_time_limit=10,
    )
    executor._remote_task = FakeTask()
    return executor


def test_executor_rejects_non_celery_app():
    with pytest.raises(TypeError, match="must be a Celery instance"):
        CeleryExecutor(object())


def test_submit_registers_task_and_forwards_limits_and_queue(executor):
    fn = Mock(return_value="done")
    future = executor.submit(fn, 1, x=2)

    assert isinstance(future, CeleryFuture)
    assert future.task_id == "task-1"

    task = executor._remote_task
    assert len(task.apply_async_calls) == 1
    assert task.apply_async_calls[0] == {
        "args": (fn, 1),
        "kwargs": {"x": 2},
        "queue": "high",
        "soft_time_limit": 3,
        "time_limit": 10,
    }

    assert future.task_id in executor.futures()
    assert executor.pending_task_ids() == [future.task_id]


def test_submit_rejects_non_callable(executor):
    with pytest.raises(TypeError, match="fn must be callable"):
        executor.submit("not-callable")


def test_submit_rejects_after_shutdown(executor):
    executor.shutdown(wait=False)

    with pytest.raises(RuntimeError, match="Cannot submit new tasks"):
        executor.submit(lambda: None)


def test_invoke_validates_task_id_and_broadcasts(executor):
    future = executor.submit(lambda: None)

    reply = executor.invoke(
        future.task_id,
        "custom_signal",
        reply=True,
        timeout=2.5,
        payload="hello",
    )

    assert reply == [{"worker": "ok"}]
    assert executor._control.broadcast_calls == [
        {
            "signal": "custom_signal",
            "arguments": {"task_id": future.task_id, "payload": "hello"},
            "reply": True,
            "timeout": 2.5,
        }
    ]


def test_invoke_rejects_unknown_task_id(executor):
    with pytest.raises(ValueError, match="was not submitted through this executor"):
        executor.invoke("missing-task", "custom_signal")


def test_revoke_calls_celery_and_cancels_future(executor):
    future = executor.submit(lambda: None)

    revoked = executor.revoke(
        future.task_id,
        terminate=True,
        signal="SIGKILL",
        countdown=5,
    )

    assert revoked is None
    assert executor._control.revoke_calls == [
        (
            future.task_id,
            {"terminate": True, "signal": "SIGKILL", "countdown": 5},
        )
    ]
    assert future.cancelled() is True
    assert future.task_id not in executor.pending_task_ids()


def test_revoke_rejects_unknown_task_id(executor):
    with pytest.raises(ValueError, match="was not submitted through this executor"):
        executor.revoke("missing-task")


def test_shutdown_can_cancel_futures_before_waiting(executor):
    future = executor.submit(lambda: None)

    called = {"count": 0}

    def fake_result(timeout=None):
        called["count"] += 1
        return None

    future.result = fake_result

    executor.shutdown(wait=True, cancel_futures=True)

    assert executor._control.revoke_calls == [(future.task_id, {"terminate": False})]
    assert called["count"] == 1
    assert executor._shutdown is True


def test_shutdown_without_wait_keeps_pending_until_done(executor):
    future = executor.submit(lambda: None)

    executor.shutdown(wait=False)

    assert future.task_id in executor.futures()
    assert executor._shutdown is True


def test_shutdown_with_timeout_stops_waiting_when_deadline_expires(executor):
    first = executor.submit(lambda: None)
    second = executor.submit(lambda: None)

    call_order = []

    def slow_result(timeout=None):
        call_order.append(timeout)
        raise concurrent.futures.TimeoutError("timed out")

    first.result = slow_result
    second.result = lambda timeout=None: call_order.append(timeout)

    executor.shutdown(wait=True, timeout=0.0)

    assert call_order == []


def test_future_properties_reflect_async_result_state():
    pending = FakeAsyncResult("task-1", state="PENDING")
    running = FakeAsyncResult("task-2", state="STARTED")
    success = FakeAsyncResult("task-3", state="SUCCESS", result=123)
    failure = FakeAsyncResult("task-4", state="FAILURE")

    pending_future = CeleryFuture(pending)
    running_future = CeleryFuture(running)
    success_future = CeleryFuture(success)
    failure_future = CeleryFuture(failure)

    assert pending_future.task_id == "task-1"
    assert pending_future.done() is False
    assert running_future.running() is True
    assert success_future.done() is True
    assert failure_future.done() is True


def test_future_cancel_revokes_and_marks_cancelled():
    async_result = FakeAsyncResult("task-1", state="PENDING")
    future = CeleryFuture(async_result)

    assert future.cancel() is True
    assert async_result.revoke_calls == [False]
    assert future.cancelled() is True


def test_future_cancel_returns_false_for_running_task():
    async_result = FakeAsyncResult("task-1", state="STARTED")
    future = CeleryFuture(async_result)

    assert future.cancel() is False
    assert async_result.revoke_calls == []


def test_future_cancel_returns_false_for_completed_task():
    async_result = FakeAsyncResult("task-1", state="SUCCESS")
    future = CeleryFuture(async_result)

    assert future.cancel() is False
    assert async_result.revoke_calls == []


def test_future_result_returns_value_on_success():
    async_result = FakeAsyncResult("task-1", state="SUCCESS", result={"ok": True})
    future = CeleryFuture(async_result)

    assert future.result() == {"ok": True}
    assert async_result.get_calls == [(None, True)]


def test_future_result_raises_cancelled_error_when_revoked():
    async_result = FakeAsyncResult("task-1", state="REVOKED")
    future = CeleryFuture(async_result)

    with pytest.raises(concurrent.futures.CancelledError):
        future.result()


def test_future_result_translates_timeout_error():
    async_result = FakeAsyncResult("task-1", state="TIMEOUT")
    future = CeleryFuture(async_result)

    with pytest.raises(concurrent.futures.TimeoutError):
        future.result(timeout=0.1)


def test_future_result_propagates_worker_exception():
    async_result = FakeAsyncResult("task-1", state="FAILURE")
    future = CeleryFuture(async_result)

    with pytest.raises(ValueError, match="worker failed"):
        future.result()


def test_future_exception_returns_exception_object_on_failure():
    async_result = FakeAsyncResult(
        "task-1", state="FAILURE", result=ValueError("worker failed")
    )
    future = CeleryFuture(async_result)

    exc = future.exception()

    assert isinstance(exc, ValueError)
    assert str(exc) == "worker failed"


def test_future_exception_returns_none_on_success():
    async_result = FakeAsyncResult("task-1", state="SUCCESS", result=5)
    future = CeleryFuture(async_result)

    assert future.exception() is None


def test_future_exception_returns_cancelled_error_when_revoked():
    async_result = FakeAsyncResult("task-1", state="REVOKED")
    future = CeleryFuture(async_result)

    exc = future.exception()

    assert isinstance(exc, concurrent.futures.CancelledError)


def test_future_add_done_callback_is_triggered_on_cancel():
    async_result = FakeAsyncResult("task-1", state="PENDING")
    future = CeleryFuture(async_result)

    callback = Mock()
    future.add_done_callback(callback)

    future.cancel()

    assert callback.call_count == 1
