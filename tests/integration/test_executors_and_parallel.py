"""Flows and executors: single, parallel (`||`) and the executor choice, end to end."""

import asyncio
import os
import threading

import pytest

from tests.integration import events
from tests.integration.support import Trace, contents, make_event, results_of, run, step
from volnux.executors import shared


@pytest.mark.asyncio
async def test_a_thread_executor_runs_the_event_off_the_main_thread():
    seen = {}

    async def process(self, *args, **kwargs):
        seen["thread"] = threading.current_thread().name
        return True, "ok"

    a = make_event("T", process, executor="thread")

    _, error = await run(a.__name__)

    assert error is None and seen["thread"] != threading.main_thread().name


@pytest.mark.asyncio
async def test_the_default_executor_runs_on_the_engines_loop():
    seen = {}

    async def process(self, *args, **kwargs):
        seen["loop"] = asyncio.get_running_loop()
        return True, "ok"

    a = make_event("D", process, executor="default")
    loop = asyncio.get_running_loop()

    _, error = await run(a.__name__)

    assert error is None and seen["loop"] is loop


@pytest.mark.asyncio
async def test_a_process_executor_runs_the_event_in_another_process():
    pipeline, error = await run(events.IntegrationProcessEcho.__name__)

    assert error is None
    assert contents(pipeline)[0] != os.getpid()


@pytest.mark.asyncio
async def test_threaded_events_share_one_bounded_pool_across_runs():
    seen = []

    async def process(self, *args, **kwargs):
        seen.append(threading.current_thread().name)
        return True, "ok"

    a = make_event("S", process, executor="thread")
    await run(a.__name__)
    first_pool = shared.get_shared_pool(
        __import__("volnux.executors", fromlist=["x"]).ThreadPoolExecutor
    )
    await run(a.__name__)
    second_pool = shared.get_shared_pool(
        __import__("volnux.executors", fromlist=["x"]).ThreadPoolExecutor
    )

    assert first_pool is second_pool and first_pool is not None


@pytest.mark.asyncio
async def test_parallel_events_run_together_and_the_next_task_waits_for_all():
    trace = Trace()
    a = make_event("PA", step(trace, "a", delay=0.15), executor="thread")
    b = make_event("PB", step(trace, "b", delay=0.15), executor="thread")
    after = make_event("After", step(trace, "after"))

    pipeline, error = await run(
        f'({a.__name__} || {b.__name__}[executor = "thread"]) -> {after.__name__}'
    )

    assert error is None
    log = trace.log
    assert log.index(("start", "a")) < log.index(("end", "b"))  # overlapped
    assert log.index(("start", "b")) < log.index(("end", "a"))
    assert log.index(("start", "after")) > max(
        log.index(("end", "a")), log.index(("end", "b"))
    )
    assert sorted(r.content for r in results_of(pipeline)[:2]) == ["a", "b"]


@pytest.mark.asyncio
async def test_parallel_events_on_the_process_pool_and_a_join_that_reads_both():
    pipeline, error = await run(
        f"({events.ParallelLeft.__name__} || {events.ParallelRight.__name__}) "
        f"-> {events.ParallelJoin.__name__}"
    )

    assert error is None
    assert contents(pipeline) == [["left", "right"]]
