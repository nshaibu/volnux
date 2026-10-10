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


def test_submit_task_requires_initialization(pool_manager):
    with pytest.raises(
        RuntimeError, match="must be initialized before submitting tasks"
    ):
        pool_manager.submit_task(FakeContext("ctx-1"), lambda: None)


def test_submit_task_requires_callable(monkeypatch, pool_manager):
    pool_manager._executor = FakeExecutor()
    pool_manager._initialized = True
    pool_manager._backend_type = "local"

    with pytest.raises(TypeError, match="task_func must be callable"):
        pool_manager.submit_task(FakeContext("ctx-1"), "not-callable")


def test_submit_task_tracks_future_and_adds_done_callback(pool_manager):
    fake_executor = FakeExecutor()
    pool_manager._executor = fake_executor
    pool_manager._initialized = True
    pool_manager._backend_type = "local"

    future = FakeFuture()
    fake_executor.submit = Mock(return_value=future)

    context = FakeContext("ctx-1")
    returned = pool_manager.submit_task(context, lambda x: x, 123)

    assert returned is future
    fake_executor.submit.assert_called_once()
    assert len(future.add_done_callback_calls) == 1
    assert pool_manager.active_count(context) == 1
    assert context.state_id in pool_manager.active_context_ids()


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


def test_cancel_context_cancels_pending_futures(pool_manager):
    pool_manager._initialized = True
    pool_manager._backend_type = "local"
    pool_manager._executor = FakeExecutor()

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
    pool_manager._initialized = True
    pool_manager._backend_type = "celery"
    pool_manager._executor = celery_executor

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
    pool_manager._executor = fake_executor
    pool_manager._initialized = True
    pool_manager._backend_type = "local"

    pool_manager.shutdown(wait=False, cancel_futures=True)

    assert pool_manager.is_initialized is False
    assert pool_manager._shut_down is True
    assert fake_executor.shutdown_calls == [{"wait": False, "cancel_futures": True}]


def test_shutdown_is_idempotent(pool_manager):
    pool_manager._initialized = True
    pool_manager._backend_type = "local"
    pool_manager._executor = FakeExecutor()

    pool_manager.shutdown(wait=False)
    pool_manager.shutdown(wait=False)

    assert pool_manager._shut_down is True


def test_reset_clears_singleton_and_allows_recreation():
    first = VolnuxPoolManager()
    VolnuxPoolManager.reset()
    second = VolnuxPoolManager()

    assert first is not second
    assert second.is_initialized is False
    assert second.backend_type is None
