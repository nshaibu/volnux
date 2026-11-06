"""VolnuxPoolManager scheduling: slots, priority order, cancel, drain, wait, shutdown.

Most tests use an executor whose futures the test completes by hand, so every
interleaving is deterministic; a few at the end use real spawned worker processes."""

import threading
import time
from concurrent.futures import CancelledError, Future

import pytest

from tests.unittests.execution.common import pool_tasks
from volnux.execution.common._pool import (
    PriorityTaskQueue,
    TaskPriority,
    VolnuxPoolManager,
    get_pool_manager,
)


class Ctx:
    def __init__(self, state_id):
        self.state_id = state_id


class ManualExecutor:
    """Hands out real Futures that the test completes."""

    def __init__(self):
        self.calls = []  # (fn, args, kwargs)
        self.futures = []
        self.shutdown_calls = []
        self.block_submit = None  # an Event: submit waits for it
        self.entered_submit = threading.Event()

    def submit(self, fn, *args, **kwargs):
        self.entered_submit.set()
        if self.block_submit is not None:
            self.block_submit.wait(5)
        future = Future()
        self.calls.append((fn, args, kwargs))
        self.futures.append(future)
        return future

    def shutdown(self, wait=True, cancel_futures=False):
        self.shutdown_calls.append({"wait": wait, "cancel_futures": cancel_futures})


def wait_until(predicate, timeout=3.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(0.005)
    return False


@pytest.fixture(autouse=True)
def fresh_singleton():
    VolnuxPoolManager.reset()
    yield
    VolnuxPoolManager.reset()


@pytest.fixture
def pool():
    return VolnuxPoolManager()


def ready(pool, executor=None, slots=0, **queue_args):
    """Initialised by hand, with the dispatcher not running yet."""
    pool._executor = executor if executor is not None else ManualExecutor()
    pool._task_queue = PriorityTaskQueue(**queue_args)
    pool._initialized = True
    pool._backend_type = "local"
    pool._max_in_flight = slots
    pool._slots = threading.BoundedSemaphore(slots) if slots else None
    return pool


def names(executor):
    return [fn.__name__ for fn, _, _ in executor.calls]


def job(name):
    def fn():
        pass

    fn.__name__ = name
    return fn


# --- in-flight slots: what makes the priority order real ----------------------------------------------


def test_only_as_many_tasks_run_as_there_are_slots(pool):
    """With no limit the dispatcher pushed every task straight into the executor's own FIFO
    queue, so priority was only ever applied to tasks queued within microseconds."""
    ex = ManualExecutor()
    ready(pool, ex, slots=2)
    ctx = Ctx("ctx-1")
    for n in range(5):
        pool.submit_task(ctx, job(f"t{n}"))

    pool._start_dispatch_loop()

    assert wait_until(lambda: len(ex.calls) == 2)
    time.sleep(0.1)
    assert len(ex.calls) == 2  # held back: no free slot
    assert pool._task_queue.size == 3

    ex.futures[0].set_result(None)
    assert wait_until(lambda: len(ex.calls) == 3)
    pool.shutdown(wait_for_tasks=False)


def test_the_best_waiting_task_gets_the_slot_that_frees(pool):
    ex = ManualExecutor()
    ready(pool, ex, slots=1)
    ctx = Ctx("ctx-1")
    pool.submit_task(ctx, job("first"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.calls) == 1)

    pool.submit_task(ctx, job("low"), priority=TaskPriority.LOW)
    pool.submit_task(ctx, job("normal"), priority=TaskPriority.NORMAL)
    pool.submit_task(ctx, job("critical"), priority=TaskPriority.CRITICAL)
    pool.submit_task(ctx, job("high"), priority=TaskPriority.HIGH)

    for i, expected in enumerate(["critical", "high", "normal", "low"], start=1):
        ex.futures[i - 1].set_result(None)
        assert wait_until(lambda: len(ex.calls) == i + 1)
        assert names(ex)[-1] == expected
    pool.shutdown(wait_for_tasks=False)


def test_a_cancelled_or_failed_task_frees_its_slot_too(pool):
    ex = ManualExecutor()
    ready(pool, ex, slots=1)
    ctx = Ctx("ctx-1")
    for n in range(3):
        pool.submit_task(ctx, job(f"t{n}"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.calls) == 1)

    ex.futures[0].set_exception(ValueError("boom"))
    assert wait_until(lambda: len(ex.calls) == 2)
    ex.futures[1].cancel()
    assert wait_until(lambda: len(ex.calls) == 3)
    pool.shutdown(wait_for_tasks=False)


def test_a_submit_that_fails_gives_its_slot_back(pool):
    class Flaky(ManualExecutor):
        def submit(self, fn, *a, **k):
            if fn.__name__ == "bad":
                raise RuntimeError("cannot submit")
            return super().submit(fn, *a, **k)

    ex = Flaky()
    ready(pool, ex, slots=1)
    ctx = Ctx("ctx-1")
    pool.submit_task(ctx, job("bad"))
    pool.submit_task(ctx, job("good"))

    pool._start_dispatch_loop()

    assert wait_until(lambda: names(ex) == ["good"])  # "bad" did not keep the only slot
    pool.shutdown(wait_for_tasks=False)


def test_no_limit_means_every_task_goes_straight_to_the_executor(pool):
    ex = ManualExecutor()
    ready(pool, ex, slots=0)
    ctx = Ctx("ctx-1")
    for n in range(6):
        pool.submit_task(ctx, job(f"t{n}"))
    pool._start_dispatch_loop()

    assert wait_until(lambda: len(ex.calls) == 6)
    pool.shutdown(wait_for_tasks=False)


# --- cancel -----------------------------------------------------------------------------------------------------------


def test_cancel_context_drops_the_tasks_still_waiting_for_a_slot(pool):
    ex = ManualExecutor()
    ready(pool, ex, slots=1)
    ctx, other = Ctx("ctx-1"), Ctx("ctx-2")
    pool.submit_task(ctx, job("running"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.calls) == 1)
    for n in range(3):
        pool.submit_task(ctx, job(f"waiting{n}"))
    pool.submit_task(other, job("theirs"))

    cancelled, skipped = pool.cancel_context(ctx)

    assert cancelled == 3 + 1 and skipped == 0  # three queued, and the pending future
    assert ex.futures[0].cancelled()  # and its slot went to the other context
    assert wait_until(lambda: names(ex)[-1:] == ["theirs"])
    assert "waiting0" not in names(ex)
    pool.shutdown(wait_for_tasks=False)


def test_cancel_context_catches_a_task_between_the_queue_and_the_executor(pool):
    """A task dequeued but not yet registered belonged to neither side, so a cancel missed it
    and the task ran."""
    ex = ManualExecutor()
    ready(pool, ex)
    ctx = Ctx("ctx-1")
    pool.submit_task(ctx, job("in-transit"))
    task = (
        pool._task_queue.dequeue()
    )  # what the dispatcher holds just before submitting

    cancelled, skipped = pool.cancel_context(ctx)

    assert (cancelled, skipped) == (1, 0)
    assert pool._task_queue.is_cancelled(task)


def test_a_cancel_waits_for_a_submit_in_progress_and_then_cancels_the_future(pool):
    ex = ManualExecutor()
    ex.block_submit = threading.Event()
    ready(pool, ex)
    ctx = Ctx("ctx-1")
    pool.submit_task(ctx, job("slow-submit"))
    pool._start_dispatch_loop()
    assert ex.entered_submit.wait(3)  # the dispatcher is inside executor.submit

    result = []
    canceller = threading.Thread(target=lambda: result.append(pool.cancel_context(ctx)))
    canceller.start()
    time.sleep(0.1)
    assert result == []  # waiting: the task is half-way

    ex.block_submit.set()
    canceller.join(3)

    assert result == [(1, 0)]
    assert ex.futures[0].cancelled()
    pool.shutdown(wait_for_tasks=False)


def test_a_running_task_cannot_be_cancelled_and_is_counted_as_skipped(pool):
    ex = ManualExecutor()
    ready(pool, ex)
    ctx = Ctx("ctx-1")
    pool.submit_task(ctx, job("running"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.futures) == 1)
    ex.futures[0].set_running_or_notify_cancel()

    assert pool.cancel_context(ctx) == (0, 1)
    pool.shutdown(wait_for_tasks=False)


# --- drain ------------------------------------------------------------------------------------------------------------


def test_drain_waits_for_queued_tasks_not_only_the_ones_in_flight(pool):
    """It returned as soon as nothing was in flight, with the context's work still queued."""
    ex = ManualExecutor()
    ready(pool, ex, slots=1)
    ctx = Ctx("ctx-1")
    pool.submit_task(ctx, job("a"))
    pool.submit_task(ctx, job("b"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.calls) == 1)

    with pytest.raises(TimeoutError, match="drain_context timed out"):
        pool.drain_context(ctx, timeout=0.15)  # "b" is still waiting for the slot

    done = []
    drainer = threading.Thread(
        target=lambda: (pool.drain_context(ctx, timeout=5), done.append(1))
    )
    drainer.start()
    ex.futures[0].set_result(None)
    assert wait_until(lambda: len(ex.calls) == 2)
    time.sleep(0.05)
    assert done == []  # the second task is in flight
    ex.futures[1].set_result(None)
    drainer.join(3)

    assert done == [1]
    pool.shutdown(wait_for_tasks=False)


def test_drain_of_one_context_does_not_wait_for_another(pool):
    ex = ManualExecutor()
    ready(pool, ex)
    pool.submit_task(Ctx("busy"), job("busy"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.futures) == 1)

    pool.drain_context(Ctx("quiet"), timeout=0.5)  # returns at once
    pool.shutdown(wait_for_tasks=False)


def test_drain_times_out_while_a_task_is_still_running(pool):
    ex = ManualExecutor()
    ready(pool, ex)
    ctx = Ctx("ctx-1")
    pool.submit_task(ctx, job("running"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.futures) == 1)

    start = time.monotonic()
    with pytest.raises(TimeoutError, match="still running"):
        pool.drain_context(ctx, timeout=0.2)

    assert 0.15 < time.monotonic() - start < 2
    pool.shutdown(wait_for_tasks=False)


# --- wait_any -----------------------------------------------------------------------------------------------------------


def test_wait_any_returns_as_soon_as_one_task_finishes(pool):
    """It waited for the first *exception*: with nothing failing, for every task."""
    ex = ManualExecutor()
    ready(pool, ex)
    ctx = Ctx("ctx-1")
    pool.submit_task(ctx, job("a"))
    pool.submit_task(ctx, job("b"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.futures) == 2)

    threading.Timer(0.1, lambda: ex.futures[0].set_result("a")).start()
    start = time.monotonic()
    done, not_done = pool.wait_any(ctx, timeout=5)

    assert time.monotonic() - start < 2
    assert done == {ex.futures[0]} and not_done == {ex.futures[1]}
    pool.shutdown(wait_for_tasks=False)


def test_wait_any_times_out_with_nothing_done(pool):
    ex = ManualExecutor()
    ready(pool, ex)
    ctx = Ctx("ctx-1")
    pool.submit_task(ctx, job("a"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.futures) == 1)

    done, not_done = pool.wait_any(ctx, timeout=0.1)

    assert done == set() and len(not_done) == 1
    pool.shutdown(wait_for_tasks=False)


# --- shutdown ---------------------------------------------------------------------------------------------------------


def test_shutdown_without_waiting_drops_the_queued_tasks(pool):
    """They were still pushed at the executor while it was closing, and failed there."""
    ex = ManualExecutor()
    ready(pool, ex, slots=1)
    ctx = Ctx("ctx-1")
    pool.submit_task(ctx, job("running"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.calls) == 1)
    for n in range(3):
        pool.submit_task(ctx, job(f"queued{n}"))

    start = time.monotonic()
    pool.shutdown(wait_for_tasks=False)
    elapsed = time.monotonic() - start

    assert names(ex) == ["running"]
    assert ex.shutdown_calls == [{"wait": False, "cancel_futures": False}]
    # nothing left to wait for: with the queue kept, the dispatcher would sit on the
    # slot of the running task until the join gave up
    assert elapsed < 3


def test_shutdown_with_cancel_futures_drops_the_queue_and_passes_the_flag_on(pool):
    ex = ManualExecutor()
    ready(pool, ex, slots=1)
    ctx = Ctx("ctx-1")
    pool.submit_task(ctx, job("running"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.calls) == 1)
    pool.submit_task(ctx, job("queued"))

    pool.shutdown(wait_for_tasks=True, cancel_futures=True)

    assert names(ex) == ["running"]
    assert ex.shutdown_calls == [{"wait": True, "cancel_futures": True}]


def test_shutdown_that_waits_runs_everything_already_queued(pool):
    ex = ManualExecutor()
    ready(pool, ex, slots=1)
    ctx = Ctx("ctx-1")
    for n in range(3):
        pool.submit_task(ctx, job(f"t{n}"))
    pool._start_dispatch_loop()
    assert wait_until(lambda: len(ex.calls) == 1)

    def finish_tasks():
        done = 0
        while done < 3:
            if len(ex.futures) > done and not ex.futures[done].done():
                ex.futures[done].set_result(None)
                done += 1
            time.sleep(0.01)

    helper = threading.Thread(target=finish_tasks, daemon=True)
    helper.start()
    pool.shutdown(wait_for_tasks=True)

    assert names(ex) == ["t0", "t1", "t2"]
    assert ex.shutdown_calls[0]["wait"] is True


def test_nothing_can_be_submitted_after_shutdown(pool):
    ready(pool)
    pool.shutdown(wait_for_tasks=False)
    with pytest.raises(RuntimeError, match="shut down"):
        pool.submit_task(Ctx("ctx-1"), job("late"))


# --- the singleton ----------------------------------------------------------------------------------------------------


def test_concurrent_first_calls_get_one_instance_with_one_set_of_state():
    """State was created in __init__, which runs on every call: threads making the first calls
    could replace each other's lock and registry."""
    VolnuxPoolManager.reset()
    seen, barrier = [], threading.Barrier(16)

    def grab():
        barrier.wait()
        manager = get_pool_manager()
        seen.append((id(manager), id(manager._state_lock), id(manager._registry)))

    threads = [threading.Thread(target=grab) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(set(seen)) == 1


def test_calling_the_constructor_again_keeps_the_state():
    manager = VolnuxPoolManager()
    lock, registry = manager._state_lock, manager._registry

    again = VolnuxPoolManager()

    assert (
        again is manager and again._state_lock is lock and again._registry is registry
    )


# --- for real: spawned worker processes ---------------------------------------------------------------------------------------


@pytest.fixture
def real_pool():
    manager = VolnuxPoolManager()
    manager.initialize("local", max_workers=1, starvation_threshold=0)
    yield manager
    manager.shutdown(wait_for_tasks=False, cancel_futures=True)


def read_lines(path):
    return path.read_text().split() if path.exists() else []


def test_with_one_worker_tasks_run_in_priority_order(real_pool, tmp_path):
    """The proof that priority applies when a worker frees up, not just when tasks are enqueued."""
    out = tmp_path / "order.txt"
    ctx = Ctx("ctx-real-order")
    real_pool.submit_task(ctx, pool_tasks.record, str(out), "first", 0.5)
    assert wait_until(
        lambda: pool_tasks is not None and real_pool.active_count(ctx) == 1, 20
    )

    real_pool.submit_task(
        ctx, pool_tasks.record, str(out), "low", priority=TaskPriority.LOW
    )
    real_pool.submit_task(ctx, pool_tasks.record, str(out), "normal")
    real_pool.submit_task(
        ctx, pool_tasks.record, str(out), "critical", priority=TaskPriority.CRITICAL
    )
    real_pool.submit_task(
        ctx, pool_tasks.record, str(out), "high", priority=TaskPriority.HIGH
    )
    real_pool.drain_context(ctx, timeout=60)

    assert read_lines(out) == ["first", "critical", "high", "normal", "low"]


def test_a_cancelled_context_never_runs_its_queued_tasks(real_pool, tmp_path):
    out = tmp_path / "cancel.txt"
    ctx, other = Ctx("ctx-real-cancel"), Ctx("ctx-real-other")
    real_pool.submit_task(ctx, pool_tasks.record, str(out), "running", 0.5)
    assert wait_until(lambda: real_pool.active_count(ctx) == 1, 20)
    for n in range(3):
        real_pool.submit_task(ctx, pool_tasks.record, str(out), f"queued{n}")
    real_pool.submit_task(other, pool_tasks.record, str(out), "other")

    cancelled, skipped = real_pool.cancel_context(ctx)
    real_pool.drain_context(other, timeout=60)
    time.sleep(0.7)

    assert (cancelled, skipped) == (3, 1)  # three queued dropped, one already running
    lines = read_lines(out)
    assert "other" in lines and not any(line.startswith("queued") for line in lines)


def test_a_task_that_raises_does_not_stop_the_pool(real_pool, tmp_path):
    out = tmp_path / "after.txt"
    ctx = Ctx("ctx-real-boom")
    real_pool.submit_task(ctx, pool_tasks.boom)
    real_pool.submit_task(ctx, pool_tasks.record, str(out), "after")

    real_pool.drain_context(ctx, timeout=60)

    assert read_lines(out) == ["after"]


def test_the_report_describes_the_pool(real_pool):
    report = real_pool.report()

    assert (
        report["pool"]["backend"] == "local" and report["pool"]["initialized"] is True
    )
    assert report["in_flight"] == {"tasks": 0, "limit": 1}
    assert report["queue"]["queue_depth"] == 0
