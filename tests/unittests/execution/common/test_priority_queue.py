"""PriorityTaskQueue: ordering, backpressure, starvation, cancellation and shutdown."""

import threading
import time

import pytest

from volnux.execution.common._pool import PriorityTaskQueue, QueueTask, TaskPriority


def task(name="t", priority=TaskPriority.NORMAL, context="ctx"):
    return QueueTask(
        priority=priority,
        enqueue_time=0.0,
        context_id=context,
        task_id=name,
        dispatch_fn=lambda: None,
    )


def drain(queue):
    out = []
    while queue.size:
        out.append(queue.dequeue().task_id)
    return out


# --- priorities --------------------------------------------------------------------------------


def test_higher_priority_comes_out_first():
    q = PriorityTaskQueue()
    for name, prio in (
        ("low", TaskPriority.LOW),
        ("crit", TaskPriority.CRITICAL),
        ("norm", TaskPriority.NORMAL),
        ("high", TaskPriority.HIGH),
    ):
        q.enqueue(task(name, prio))

    assert drain(q) == ["crit", "high", "norm", "low"]


def test_the_same_priority_is_first_in_first_out():
    q = PriorityTaskQueue()
    for i in range(20):
        q.enqueue(task(f"t{i}"))

    assert drain(q) == [f"t{i}" for i in range(20)]


def test_a_bare_int_priority_is_coerced_and_a_bad_one_refused():
    assert task(priority=100).priority is TaskPriority.HIGH
    assert task(priority="500").priority is TaskPriority.NORMAL
    with pytest.raises(ValueError, match="Invalid task priority"):
        task(priority=250)
    with pytest.raises(ValueError, match="Invalid task priority"):
        task(priority=None)


def test_priorities_move_one_level_and_stop_at_the_ends():
    assert TaskPriority.LOW.promote() is TaskPriority.NORMAL
    assert TaskPriority.NORMAL.promote() is TaskPriority.HIGH
    assert TaskPriority.HIGH.promote() is TaskPriority.CRITICAL
    assert TaskPriority.CRITICAL.promote() is TaskPriority.CRITICAL
    assert TaskPriority.CRITICAL.demote() is TaskPriority.HIGH
    assert TaskPriority.LOW.demote() is TaskPriority.LOW


# --- size, stats, backpressure ------------------------------------------------------------------------


def test_stats_count_depth_priorities_and_totals():
    q = PriorityTaskQueue(max_size=10)
    q.enqueue(task("a", TaskPriority.HIGH))
    q.enqueue(task("b", TaskPriority.LOW))
    q.dequeue()

    stats = q.stats

    assert stats["queue_depth"] == 1 and stats["in_transit"] == 1
    assert (stats["total_enqueued"], stats["total_dequeued"]) == (2, 1)
    assert stats["priorities"] == {"critical": 0, "high": 0, "normal": 0, "low": 1}
    assert (
        stats["max_size"] == 10 and PriorityTaskQueue().stats["max_size"] == "unlimited"
    )


def test_a_full_queue_refuses_more_work():
    q = PriorityTaskQueue(max_size=2)
    q.enqueue(task("a"))
    q.enqueue(task("b"))

    with pytest.raises(PriorityTaskQueue.QueueFull, match="full"):
        q.enqueue(task("c"))

    q.dequeue()
    q.enqueue(task("c"))  # room again


# --- blocking and stopping ------------------------------------------------------------------------------


def test_dequeue_waits_for_a_task():
    q = PriorityTaskQueue()
    got = []
    thread = threading.Thread(target=lambda: got.append(q.dequeue()))
    thread.start()
    time.sleep(0.1)
    assert got == []  # still waiting

    q.enqueue(task("late"))
    thread.join(2)

    assert got[0].task_id == "late"


def test_dequeue_gives_up_after_its_timeout():
    q = PriorityTaskQueue()
    start = time.monotonic()

    assert q.dequeue(timeout=0.1) is None

    assert 0.08 <= time.monotonic() - start < 1.0
    assert q.stopped is False  # told apart from a stop by the flag


def test_stop_wakes_a_waiting_dequeue_with_none():
    q = PriorityTaskQueue()
    got = []
    thread = threading.Thread(target=lambda: got.append(q.dequeue()))
    thread.start()
    time.sleep(0.05)

    q.stop()
    thread.join(2)

    assert got == [None] and q.stopped is True


def test_a_stopped_queue_hands_out_what_is_left_then_none():
    q = PriorityTaskQueue()
    q.enqueue(task("a"))
    q.enqueue(task("b"))
    q.stop()
    q.stop()  # twice is fine

    assert [q.dequeue().task_id, q.dequeue().task_id, q.dequeue()] == ["a", "b", None]
    with pytest.raises(RuntimeError, match="stopped"):
        q.enqueue(task("c"))


def test_many_producers_and_consumers_lose_nothing():
    q = PriorityTaskQueue()
    seen, lock = [], threading.Lock()

    def consume():
        while True:
            item = q.dequeue()
            if item is None:
                return
            with lock:
                seen.append(item.task_id)
            q.finish_dispatch(item)

    consumers = [threading.Thread(target=consume) for _ in range(4)]
    for t in consumers:
        t.start()

    def produce(n):
        for i in range(100):
            q.enqueue(task(f"p{n}-{i}", TaskPriority(500 if i % 2 else 100)))

    producers = [threading.Thread(target=produce, args=(n,)) for n in range(4)]
    for t in producers:
        t.start()
    for t in producers:
        t.join()
    while q.size:
        time.sleep(0.01)
    q.stop()
    for t in consumers:
        t.join(5)

    assert len(seen) == 400 and len(set(seen)) == 400


# --- removing and cancelling ---------------------------------------------------------------------------------


def test_remove_task_takes_one_queued_task_out():
    q = PriorityTaskQueue()
    for name in "abc":
        q.enqueue(task(name))

    assert q.remove_task("b").task_id == "b"
    assert q.remove_task("b") is None and q.remove_task("zzz") is None
    assert drain(q) == ["a", "c"]


def test_remove_by_context_leaves_other_contexts_alone():
    q = PriorityTaskQueue()
    q.enqueue(task("a1", context="A"))
    q.enqueue(task("b1", context="B"))
    q.enqueue(task("a2", context="A"))

    removed = q.remove_by_context("A")

    assert sorted(t.task_id for t in removed) == ["a1", "a2"]
    assert drain(q) == ["b1"]
    assert q.remove_by_context("nobody") == []


def test_pending_count_covers_queued_and_in_transit_tasks():
    q = PriorityTaskQueue()
    q.enqueue(task("a", context="A"))
    q.enqueue(task("a2", context="A"))
    q.enqueue(task("b", context="B"))
    assert q.pending_count("A") == 2

    first = q.dequeue()  # "a": handed to the dispatcher, not yet to the executor
    assert q.pending_count("A") == 2  # still pending: it belongs to neither side yet

    q.finish_dispatch(first)
    assert (
        q.pending_count("A") == 1
        and q.pending_count("B") == 1
        and q.pending_count("C") == 0
    )


def test_cancelling_a_context_removes_queued_tasks_and_flags_those_in_transit():
    """A task the dispatcher has taken but not yet submitted must not slip through."""
    q = PriorityTaskQueue()
    q.enqueue(task("moving", context="A"))
    q.enqueue(task("waiting", context="A"))
    q.enqueue(task("other", context="B"))
    moving = q.dequeue()

    removed, flagged = q.cancel_context("A")

    assert [t.task_id for t in removed] == ["waiting"] and flagged == 1
    assert q.is_cancelled(moving) is True
    assert drain(q) == ["other"]
    q.finish_dispatch(moving)
    assert q.is_cancelled(moving) is False and q.pending_count("A") == 0


def test_cancelling_twice_flags_a_task_once():
    q = PriorityTaskQueue()
    q.enqueue(task("moving", context="A"))
    q.dequeue()

    assert q.cancel_context("A")[1] == 1
    assert q.cancel_context("A")[1] == 0


def test_clear_empties_the_queue_and_flags_what_is_in_transit():
    q = PriorityTaskQueue()
    for name in "abc":
        q.enqueue(task(name))
    moving = q.dequeue()

    cleared = q.clear()

    assert sorted(t.task_id for t in cleared) == ["b", "c"] and q.size == 0
    assert q.is_cancelled(moving)


# --- starvation -----------------------------------------------------------------------------------------------------


def aged(queue, name, seconds):
    """Make a queued task look as if it had been waiting for ``seconds``."""
    for item in queue._heap:
        if item.task_id == name:
            item.waiting_since -= seconds


def test_a_task_that_waited_too_long_moves_up_one_level():
    q = PriorityTaskQueue(starvation_threshold=10)
    q.enqueue(task("old", TaskPriority.LOW))
    time.sleep(0.01)
    q.enqueue(task("young", TaskPriority.NORMAL))
    aged(q, "old", 30)

    first = q.dequeue()

    # LOW -> NORMAL, and having waited longer than "young" it goes ahead of it
    assert first.task_id == "old" and first.priority is TaskPriority.NORMAL


def test_promotion_is_one_level_per_threshold_not_one_per_dequeue():
    """The waiting clock was never restarted, so every dequeue promoted the task again and a
    LOW task reached CRITICAL after three of them."""
    q = PriorityTaskQueue(starvation_threshold=10)
    q.enqueue(task("old", TaskPriority.LOW))
    aged(q, "old", 30)
    for i in range(3):
        q.enqueue(task(f"high{i}", TaskPriority.HIGH))

    order = [q.dequeue().task_id for _ in range(4)]

    assert order == [
        "high0",
        "high1",
        "high2",
        "old",
    ]  # one level (NORMAL): behind HIGH


def test_a_task_is_promoted_again_after_waiting_another_threshold():
    q = PriorityTaskQueue(starvation_threshold=10)
    q.enqueue(task("old", TaskPriority.LOW))
    q.enqueue(task("filler", TaskPriority.CRITICAL))
    aged(q, "old", 30)
    q.dequeue()  # filler out, old LOW -> NORMAL
    aged(q, "old", 30)

    q.enqueue(task("filler2", TaskPriority.CRITICAL))
    q.dequeue()  # old NORMAL -> HIGH

    assert [t.priority for t in q._heap if t.task_id == "old"] == [TaskPriority.HIGH]


def test_starvation_promotion_can_be_switched_off():
    q = PriorityTaskQueue(starvation_threshold=0)
    q.enqueue(task("old", TaskPriority.LOW))
    q.enqueue(task("new", TaskPriority.NORMAL))
    aged(q, "old", 10_000)

    assert q.dequeue().task_id == "new"


def test_a_critical_task_is_not_promoted_further():
    q = PriorityTaskQueue(starvation_threshold=1)
    q.enqueue(task("c", TaskPriority.CRITICAL))
    aged(q, "c", 100)
    assert q.dequeue().priority is TaskPriority.CRITICAL
