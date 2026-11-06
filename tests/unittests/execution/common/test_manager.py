from unittest.mock import Mock

import pytest
from celery import Celery

from volnux.executors.celery import CeleryExecutor
from volnux.execution.common._pool import VolnuxPoolManager


class FakeContext:
    def __init__(self, state_id: str):
        self.state_id = state_id


class FakeFuture:
    def __init__(self, cancel_result=True):
        self.cancel_result = cancel_result
        self.cancel_calls = 0
        self.done_calls = 0
        self.add_done_callback_calls = []
        self._done = False
        self.task_id = "task-1"

    def cancel(self):
        self.cancel_calls += 1
        return self.cancel_result

    def done(self):
        self.done_calls += 1
        return self._done

    def add_done_callback(self, callback):
        self.add_done_callback_calls.append(callback)

    def set_done(self, value: bool = True):
        self._done = value


class FakeExecutor:
    def __init__(self):
        self.submit_calls = []
        self.shutdown_calls = []

    def submit(self, fn, *args, **kwargs):
        self.submit_calls.append((fn, args, kwargs))
        return FakeFuture()

    def shutdown(self, wait=True, cancel_futures=False):
        self.shutdown_calls.append({"wait": wait, "cancel_futures": cancel_futures})


class FakeCeleryExecutor(FakeExecutor):
    def __init__(self, celery_app: Celery = None):
        super().__init__()
        self.revoke_calls = []
        self._celery_app = celery_app

    def revoke(self, task_id, terminate=False):
        self.revoke_calls.append({"task_id": task_id, "terminate": terminate})


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


@pytest.fixture(autouse=True)
def reset_pool_manager():
    VolnuxPoolManager.reset()
    yield
    VolnuxPoolManager.reset()


@pytest.fixture
def pool_manager():
    return VolnuxPoolManager()


def test_singleton_returns_same_instance():
    first = VolnuxPoolManager()
    second = VolnuxPoolManager()

    assert first is second


def make_ready(pool_manager, executor=None, backend="local"):
    """A manager that is initialised but has no dispatch thread running yet."""
    from volnux.execution.common._pool import PriorityTaskQueue

    pool_manager._executor = executor if executor is not None else FakeExecutor()
    pool_manager._task_queue = PriorityTaskQueue()
    pool_manager._initialized = True
    pool_manager._backend_type = backend
    return pool_manager


def wait_until(predicate, timeout=3.0):
    import time

    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.005)
    return False


def test_submit_task_requires_initialization(pool_manager):
    with pytest.raises(RuntimeError, match="not initialised"):
        pool_manager.submit_task(FakeContext("ctx-1"), lambda: None)


def test_submit_task_after_shutdown_is_refused(pool_manager):
    make_ready(pool_manager)
    pool_manager.shutdown(wait_for_tasks=False)
    with pytest.raises(RuntimeError, match="shut down"):
        pool_manager.submit_task(FakeContext("ctx-1"), lambda: None)


def test_submit_task_requires_callable(pool_manager):
    make_ready(pool_manager)

    with pytest.raises(TypeError, match="task_func must be callable"):
        pool_manager.submit_task(FakeContext("ctx-1"), "not-callable")


def test_submit_task_enqueues_and_returns_a_task_id(pool_manager):
    make_ready(pool_manager)

    task_id = pool_manager.submit_task(FakeContext("ctx-12345678"), lambda x: x, 123)

    assert task_id.startswith("task-") and task_id.endswith("ctx-1234")
    assert pool_manager._task_queue.size == 1
    # nothing is dispatched until the dispatch loop runs
    assert pool_manager._executor.submit_calls == []


def test_dispatch_loop_submits_and_tracks_the_future(pool_manager):
    fake_executor = FakeExecutor()
    future = FakeFuture()
    fake_executor.submit = Mock(return_value=future)
    make_ready(pool_manager, fake_executor)

    def work(x):
        return x

    context = FakeContext("ctx-1")
    pool_manager.submit_task(context, work, 123)
    pool_manager._start_dispatch_loop()

    assert wait_until(lambda: fake_executor.submit.called)
    fake_executor.submit.assert_called_once_with(work, 123)
    assert wait_until(lambda: pool_manager.active_count(context) == 1)
    assert context.state_id in pool_manager.active_context_ids()
    assert len(future.add_done_callback_calls) == 1

    # the done callback releases the future from the registry
    future.add_done_callback_calls[0](future)
    assert pool_manager.active_count(context) == 0
    pool_manager.shutdown(wait_for_tasks=False)


def test_a_failing_submit_does_not_stop_the_dispatch_loop(pool_manager):
    fake_executor = FakeExecutor()
    good = FakeFuture()
    fake_executor.submit = Mock(side_effect=[RuntimeError("boom"), good])
    make_ready(pool_manager, fake_executor)

    context = FakeContext("ctx-1")
    pool_manager.submit_task(context, lambda: 1)
    pool_manager.submit_task(context, lambda: 2)
    pool_manager._start_dispatch_loop()

    assert wait_until(lambda: fake_executor.submit.call_count == 2)
    assert wait_until(lambda: pool_manager.active_count(context) == 1)
    pool_manager.shutdown(wait_for_tasks=False)


def test_higher_priority_task_is_dispatched_first(pool_manager):
    from volnux.execution.common._pool import TaskPriority

    fake_executor = FakeExecutor()
    make_ready(pool_manager, fake_executor)
    context = FakeContext("ctx-1")

    def low():
        pass

    def high():
        pass

    pool_manager.submit_task(context, low, priority=TaskPriority.LOW)
    pool_manager.submit_task(context, high, priority=TaskPriority.HIGH)
    pool_manager._start_dispatch_loop()

    assert wait_until(lambda: len(fake_executor.submit_calls) == 2)
    assert [call[0] for call in fake_executor.submit_calls] == [high, low]
    pool_manager.shutdown(wait_for_tasks=False)


def test_active_count_and_context_ids_update_after_manual_registry_discard(
    pool_manager,
):
    pool_manager._initialized = True
    pool_manager._backend_type = "local"
    pool_manager._executor = FakeExecutor()

    future = FakeFuture()
    context = FakeContext("ctx-1")

    pool_manager._registry.add(context.state_id, future)
    pool_manager._registry.discard(context.state_id, future)

    assert pool_manager.active_count(context) == 0
    assert pool_manager.active_context_ids() == []


def test_drain_context_returns_when_no_futures(pool_manager):
    assert pool_manager.drain_context(FakeContext("missing")) is None


def test_drain_context_waits_for_futures(monkeypatch, pool_manager):
    future = FakeFuture()
    context = FakeContext("ctx-1")
    pool_manager._registry.add(context.state_id, future)

    waited = {"called": 0}

    def fake_wait(futures, timeout=None, return_when=None):
        waited["called"] += 1
        assert future in futures
        return set([future]), set()

    monkeypatch.setattr("volnux.execution.common._pool.wait", fake_wait)

    pool_manager.drain_context(context, timeout=1.0)

    assert waited["called"] == 1


def test_drain_context_raises_timeout_when_tasks_remain(monkeypatch, pool_manager):
    future = FakeFuture()
    context = FakeContext("ctx-1")
    pool_manager._registry.add(context.state_id, future)

    def fake_wait(futures, timeout=None, return_when=None):
        return set(), set(futures)

    monkeypatch.setattr("volnux.execution.common._pool.wait", fake_wait)

    with pytest.raises(TimeoutError, match="drain_context timed out"):
        pool_manager.drain_context(context, timeout=0.1)


def test_cancel_context_removes_queued_tasks_too(pool_manager):
    make_ready(pool_manager)
    context = FakeContext("ctx-1")
    other = FakeContext("ctx-2")
    pool_manager.submit_task(context, lambda: None)
    pool_manager.submit_task(context, lambda: None)
    pool_manager.submit_task(other, lambda: None)

    cancelled, skipped = pool_manager.cancel_context(context)

    assert (cancelled, skipped) == (2, 0)
    assert pool_manager._task_queue.size == 1  # the other context's task stays


def test_cancel_context_cancels_pending_futures(pool_manager):
    make_ready(pool_manager)

    context = FakeContext("ctx-1")
    future_1 = FakeFuture(cancel_result=True)
    future_2 = FakeFuture(cancel_result=False)

    pool_manager._registry.add(context.state_id, future_1)
    pool_manager._registry.add(context.state_id, future_2)

    cancelled, skipped = pool_manager.cancel_context(context)

    assert cancelled == 1
    assert skipped == 1
    assert future_1.cancel_calls == 1
    assert future_2.cancel_calls == 1
    assert pool_manager.active_count(context) == 0


def test_cancel_context_terminates_celery_tasks_when_requested(pool_manager):
    celery_app = Celery("test_app")
    celery_app.control = FakeControl()

    celery_executor = CeleryExecutor(celery_app)
    celery_executor._control = celery_app.control
    make_ready(pool_manager, celery_executor, backend="celery")

    context = FakeContext("ctx-1")
    future = FakeFuture(cancel_result=True)
    future.task_id = "task-1"

    pool_manager._registry.add(context.state_id, future)

    # register the task id in the executor itself so revoke/cancel
    # can validate it successfully.
    celery_executor._futures[future.task_id] = future

    cancelled, skipped = pool_manager.cancel_context(context, terminate=True)

    assert cancelled == 1
    assert skipped == 0
    assert celery_app.control.revoke_calls == [
        ("task-1", {"terminate": True, "signal": "SIGTERM"})
    ]


def test_wait_any_returns_empty_sets_when_no_futures(pool_manager):
    done, not_done = pool_manager.wait_any(FakeContext("missing"))

    assert done == set()
    assert not_done == set()


def test_shutdown_marks_manager_as_shutdown(pool_manager):
    fake_executor = FakeExecutor()
    make_ready(pool_manager, fake_executor)

    pool_manager.shutdown(wait_for_tasks=False, cancel_futures=True)

    assert pool_manager.is_initialized is False
    assert pool_manager._shut_down is True
    assert fake_executor.shutdown_calls == [{"wait": False, "cancel_futures": True}]


def test_shutdown_is_idempotent(pool_manager):
    fake_executor = FakeExecutor()
    make_ready(pool_manager, fake_executor)

    pool_manager.shutdown(wait_for_tasks=False)
    pool_manager.shutdown(wait_for_tasks=False)

    assert pool_manager._shut_down is True
    assert len(fake_executor.shutdown_calls) == 1


def test_reset_clears_singleton_and_allows_recreation():
    first = VolnuxPoolManager()
    VolnuxPoolManager.reset()
    second = VolnuxPoolManager()

    assert first is not second
    assert second.is_initialized is False
    assert second.backend_type is None
