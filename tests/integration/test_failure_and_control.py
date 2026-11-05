"""Retries, runaway workflows, stopping, and sinks, through the real engine."""

import asyncio

import pytest

from tests.integration.support import (
    Trace,
    contents,
    make_event,
    make_pipeline,
    results_of,
    run,
    step,
)
from volnux.engine.base import EngineExecutionResult
from volnux.engine.default_engine import DefaultWorkflowEngine
from volnux.exceptions import EngineIterationLimitExceeded
from volnux.parser.operator import PipeType


@pytest.mark.asyncio
async def test_an_event_that_raises_is_retried_then_succeeds():
    attempts = []

    async def flaky(self, *args, **kwargs):
        attempts.append(1)
        if len(attempts) < 3:
            raise ValueError("transient")
        return True, "ok"

    a = make_event("Flaky", flaky)

    pipeline, error = await run(f"{a.__name__}[retry_attempts = 4]")

    assert error is None
    assert len(attempts) == 3 and contents(pipeline) == ["ok"]


@pytest.mark.asyncio
async def test_an_event_that_keeps_raising_leaves_an_error_result():
    async def broken(self, *args, **kwargs):
        raise ValueError("always")

    a = make_event("Broken", broken)

    pipeline, error = await run(f"{a.__name__}[retry_attempts = 2]")

    assert error is None
    [result] = results_of(pipeline)
    assert result.error is True


@pytest.mark.asyncio
async def test_a_cyclic_workflow_is_stopped_at_the_iteration_cap():
    """A task whose success points back at itself used to run for ever."""
    trace = Trace()
    a = make_event("Loop", step(trace, "loop"))
    pipeline = make_pipeline(a.__name__)()
    start = pipeline.get_pipeline_state().start
    start.condition_node.add_descriptor(1, PipeType.POINTER, start)  # success -> itself

    engine = DefaultWorkflowEngine(max_iterations=12)
    result = await asyncio.wait_for(engine.execute(start, pipeline), 20)

    assert result.status == EngineExecutionResult.FAILED
    assert isinstance(result.error, EngineIterationLimitExceeded)
    assert trace.count(("start", "loop")) == 12  # the 13th execution was refused


@pytest.mark.asyncio
async def test_the_iteration_cap_can_be_switched_off():
    trace = Trace()
    a = make_event("Once", step(trace, "once"))
    pipeline = make_pipeline(f"{a.__name__} -> {a.__name__}")()

    engine = DefaultWorkflowEngine(max_iterations=0)
    result = await engine.execute(pipeline.get_pipeline_state().start, pipeline)

    assert result.status == EngineExecutionResult.COMPLETED


@pytest.mark.asyncio
async def test_stopping_between_tasks_keeps_the_rest_from_running():
    trace = Trace()
    holder = {}

    async def first(self, *args, **kwargs):
        trace(("start", "first"))
        await holder["pipeline"].stop()
        return True, "first"

    a = make_event("First", first)
    b = make_event("Second", step(trace, "second"))
    pipeline = make_pipeline(f"{a.__name__} -> {b.__name__}")()
    holder["pipeline"] = pipeline

    await asyncio.wait_for(pipeline.start(), 20)

    assert [n for k, n in trace.log if k == "start"] == ["first"]


@pytest.mark.asyncio
@pytest.mark.parametrize("ok, taken", [(True, "yes"), (False, "no")])
async def test_the_task_after_a_conditional_runs_once_after_the_branch(ok, taken):
    """The join after `A (failure -> X, success -> Y) -> D` is a deferred (sink) task."""
    trace = Trace()
    a = make_event("Main", step(trace, "main", ok=ok))
    no = make_event("No", step(trace, "no"))
    yes = make_event("Yes", step(trace, "yes"))
    join = make_event("Join", step(trace, "join"))

    _, error = await run(
        f"{a.__name__} (failure -> {no.__name__}, success -> {yes.__name__}) -> {join.__name__}"
    )

    assert error is None
    assert [n for k, n in trace.log if k == "start"] == ["main", taken, "join"]


@pytest.mark.asyncio
async def test_a_sink_queued_twice_runs_once():
    engine = DefaultWorkflowEngine()
    task = make_pipeline(make_event("Cleanup", step(Trace(), "c")).__name__)()
    task = task.get_pipeline_state().start

    engine._queue_sink(task)
    engine._queue_sink(task)

    assert len(engine.sink_queue) == 1
