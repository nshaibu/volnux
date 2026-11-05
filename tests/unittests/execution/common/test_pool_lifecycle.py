"""VolnuxPoolManager: configuring, submitting, the future registry, reset."""

import threading
import time
from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest

from tests.unittests.execution.common.test_pool_scheduling import (
    Ctx,
    ManualExecutor,
    job,
    ready,
    wait_until,
)
from volnux.execution.common import _pool
from volnux.execution.common._pool import (
    PriorityTaskQueue,
    TaskPriority,
    VolnuxPoolManager,
    _ContextFutureRegistry,
)


@pytest.fixture(autouse=True)
def fresh_singleton():
    VolnuxPoolManager.reset()
    yield
    VolnuxPoolManager.reset()


@pytest.fixture
def pool():
    return VolnuxPoolManager()


# --- initialize ------------------------------------------------------------------------------------


def test_a_local_pool_gets_one_in_flight_slot_per_worker(pool):
    pool.initialize("local", max_workers=3)

    assert pool.is_initialized and pool.backend_type == "local"
    assert pool._max_in_flight == 3 and pool._slots is not None
    assert pool._task_queue is not None and pool._dispatch_thread.is_alive()
    pool.shutdown(wait_for_tasks=False, cancel_futures=True)


def test_the_in_flight_limit_can_be_set_or_switched_off(pool):
    pool.initialize("local", max_workers=2, max_in_flight=5)
    assert pool._max_in_flight == 5
    pool.shutdown(wait_for_tasks=False, cancel_futures=True)

    VolnuxPoolManager.reset()
    unlimited = VolnuxPoolManager()
    unlimited.initialize("local", max_workers=2, max_in_flight=0)
    assert unlimited._max_in_flight == 0 and unlimited._slots is None
    unlimited.shutdown(wait_for_tasks=False, cancel_futures=True)


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"max_workers": 0}, "max_workers must be >= 1"),
        ({"max_workers": -2}, "max_workers must be >= 1"),
        ({"max_workers": 1, "max_in_flight": -1}, "max_in_flight must be >= 0"),
    ],
)
def test_bad_sizes_are_refused(pool, kwargs, message):
    with pytest.raises(ValueError, match=message):
        pool.initialize("local", **kwargs)
    assert not pool.is_initialized


def test_an_unknown_backend_is_refused(pool):
    with pytest.raises(ValueError, match="Unknown backend"):
        pool.initialize("threads")
    assert not pool.is_initialized


def test_the_backend_name_is_case_insensitive(pool):
    pool.initialize("LOCAL", max_workers=1)
    assert pool.backend_type == "local"
    pool.shutdown(wait_for_tasks=False, cancel_futures=True)


def test_celery_needs_an_app_and_the_package(pool, monkeypatch):
    with pytest.raises(ValueError, match="celery_app is required"):
        pool.initialize("celery")

    monkeypatch.setattr(_pool, "CeleryExecutor", None)
    with pytest.raises(RuntimeError, match="Celery is not installed"):
        pool.initialize("celery", celery_app=object())
    assert not pool.is_initialized


def test_a_celery_pool_is_not_limited_by_default_and_gets_its_settings(
    pool, monkeypatch
):
    made = {}

    class FakeCelery(ManualExecutor):
        def __init__(self, **kwargs):
            super().__init__()
            made.update(kwargs)

    monkeypatch.setattr(_pool, "CeleryExecutor", FakeCelery)

    pool.initialize(
        "celery",
        celery_app="APP",
        queue="q1",
        task_soft_time_limit=5,
        task_time_limit=9,
    )

    assert made == {
        "celery_app": "APP",
        "queue": "q1",
        "task_soft_time_limit": 5,
        "task_time_limit": 9,
    }
    assert pool.backend_type == "celery" and pool._slots is None
    pool.shutdown(wait_for_tasks=False)


def test_initializing_twice_with_the_same_backend_changes_nothing(pool):
    pool.initialize("local", max_workers=1)
    executor, queue, thread = pool._executor, pool._task_queue, pool._dispatch_thread

    pool.initialize("local", max_workers=4)

    assert pool._executor is executor and pool._task_queue is queue
    assert pool._dispatch_thread is thread and pool._max_in_flight == 1
    pool.shutdown(wait_for_tasks=False, cancel_futures=True)


def test_switching_backend_needs_a_reset(pool, monkeypatch):
    pool.initialize("local", max_workers=1)
    monkeypatch.setattr(_pool, "CeleryExecutor", ManualExecutor)

    with pytest.raises(RuntimeError, match="already initialized with backend 'local'"):
        pool.initialize("celery", celery_app="APP")
    pool.shutdown(wait_for_tasks=False, cancel_futures=True)


def test_a_shut_down_manager_cannot_be_initialized_again(pool):
    pool.initialize("local", max_workers=1)
    pool.shutdown(wait_for_tasks=False, cancel_futures=True)

    with pytest.raises(RuntimeError, match="reset"):
        pool.initialize("local", max_workers=1)


# --- submit_task --------------------------------------------------------------------------------------------


def test_what_is_submitted_reaches_the_queue_with_its_arguments(pool):
    ready(pool)
    ctx = Ctx("ctx-abcdefgh-1")

    task_id = pool.submit_task(
        ctx, job("work"), 1, 2, priority=TaskPriority.HIGH, source="remote", key="v"
    )
    queued = pool._task_queue.dequeue()

    assert queued.task_id == task_id and queued.context_id == "ctx-abcdefgh-1"
    assert (queued.args, queued.kwargs) == ((1, 2), {"key": "v"})
    assert queued.priority is TaskPriority.HIGH and queued.source == "remote"
    assert task_id.endswith("ctx-abcd")


def test_task_ids_are_unique_across_threads(pool):
    ready(pool)
    ids, lock = [], threading.Lock()

    def submit_many():
        for _ in range(100):
            task_id = pool.submit_task(Ctx("ctx-1"), job("t"))
            with lock:
                ids.append(task_id)

    threads = [threading.Thread(target=submit_many) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(ids) == 800 and len(set(ids)) == 800


def test_a_bad_priority_is_refused_at_submit(pool):
    ready(pool)
    with pytest.raises(ValueError, match="Invalid task priority"):
        pool.submit_task(Ctx("ctx-1"), job("t"), priority=250)
    assert pool._task_queue.size == 0


def test_a_full_queue_pushes_back_on_the_caller(pool):
    ready(pool, max_size=2)
    pool.submit_task(Ctx("ctx-1"), job("a"))
    pool.submit_task(Ctx("ctx-1"), job("b"))

    with pytest.raises(PriorityTaskQueue.QueueFull):
        pool.submit_task(Ctx("ctx-1"), job("c"))


# --- the dispatcher ------------------------------------------------------------------------------------------


def test_a_task_whose_executor_has_gone_is_dropped_without_stopping_the_loop(pool):
    ex = ManualExecutor()
    ready(pool, ex)
    pool.submit_task(Ctx("ctx-1"), job("a"))
    pool._executor = None  # shut down underneath the dispatcher
    pool._start_dispatch_loop()

    assert wait_until(lambda: pool._task_queue.pending_count("ctx-1") == 0)
    assert pool._dispatch_thread.is_alive()
    pool._executor = ex
    pool.submit_task(Ctx("ctx-1"), job("b"))
    assert wait_until(lambda: len(ex.calls) == 1)
    pool.shutdown(wait_for_tasks=False)


def test_the_dispatch_loop_is_started_once(pool):
    ready(pool)
    pool._start_dispatch_loop()
    thread = pool._dispatch_thread

    pool._start_dispatch_loop()

    assert pool._dispatch_thread is thread
    pool.shutdown(wait_for_tasks=False)


def test_the_dispatch_thread_exits_on_shutdown(pool):
    ready(pool)
    pool._start_dispatch_loop()
    thread = pool._dispatch_thread

    pool.shutdown(wait_for_tasks=False)

    assert not thread.is_alive()


# --- the registry of futures --------------------------------------------------------------------------------------


def test_the_registry_tracks_futures_per_context():
    registry = _ContextFutureRegistry()
    f1, f2, f3 = Future(), Future(), Future()
    registry.add("a", f1)
    registry.add("a", f2)
    registry.add("b", f3)

    assert registry.snapshot("a") == {f1, f2} and registry.active_count("a") == 2
    assert sorted(registry.all_context_ids()) == ["a", "b"]

    registry.discard("a", f1)
    registry.discard("a", f1)  # twice is harmless
    registry.discard("zzz", f1)
    assert registry.snapshot("a") == {f2}

    registry.discard("a", f2)
    assert registry.all_context_ids() == ["b"]  # an empty bucket is removed


def test_pop_all_takes_a_contexts_futures_and_a_snapshot_is_a_copy():
    registry = _ContextFutureRegistry()
    f = Future()
    registry.add("a", f)
    snap = registry.snapshot("a")
    snap.clear()

    assert registry.snapshot("a") == {f}
    assert registry.pop_all("a") == {f} and registry.pop_all("a") == set()
    assert registry.active_count("a") == 0


def test_the_registry_is_safe_under_concurrent_use():
    registry = _ContextFutureRegistry()

    def work(n):
        for i in range(200):
            f = Future()
            registry.add(f"ctx{n % 3}", f)
            registry.discard(f"ctx{n % 3}", f)

    threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert registry.all_context_ids() == []


# --- reset, context manager, report ---------------------------------------------------------------------------------


def test_reset_shuts_a_running_manager_down_without_waiting(pool):
    ex = ManualExecutor()
    ready(pool, ex)
    pool._start_dispatch_loop()

    VolnuxPoolManager.reset()

    assert pool._shut_down is True
    assert ex.shutdown_calls == [{"wait": False, "cancel_futures": True}]
    assert VolnuxPoolManager() is not pool


def test_reset_of_a_manager_that_was_never_started_is_harmless():
    VolnuxPoolManager()
    VolnuxPoolManager.reset()
    VolnuxPoolManager.reset()


def test_as_a_context_manager_it_waits_for_its_tasks_on_exit(pool):
    ex = ManualExecutor()
    ready(pool, ex)
    with pool:
        pool.submit_task(Ctx("ctx-1"), job("a"))
        pool._start_dispatch_loop()
        assert wait_until(lambda: len(ex.calls) == 1)

    assert pool._shut_down and ex.shutdown_calls == [
        {"wait": True, "cancel_futures": False}
    ]


def test_the_report_before_and_after_work(pool):
    assert pool.report() == {
        "queue": {},
        "pool": {"backend": None, "initialized": False, "shut_down": False},
        "contexts": {"active_count": 0},
        "in_flight": {"tasks": 0, "limit": "unlimited"},
    }

    ex = ManualExecutor()
    ready(pool, ex, slots=2)
    ctx = Ctx("ctx-1")
    for n in range(3):
        pool.submit_task(ctx, job(f"t{n}"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.calls) == 2)

    report = pool.report()

    assert report["in_flight"] == {"tasks": 2, "limit": 2}
    assert report["contexts"]["active_count"] == 1
    assert (
        report["queue"]["queue_depth"] == 1 and report["queue"]["total_dequeued"] == 2
    )
    assert pool.active_count(ctx) == 2 and pool.active_context_ids() == ["ctx-1"]
    pool.shutdown(wait_for_tasks=False)
