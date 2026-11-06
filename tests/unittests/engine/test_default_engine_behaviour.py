"""DefaultWorkflowEngine: behaviours the call-shape tests in test_default_engine.py miss.

Each test here fails on the engine as it was before the review fixes (see the
docstrings for what the old code did).
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from volnux.engine import run as run_module
from volnux.engine.base import (
    EngineExecutionResult,
    EngineResult,
    SubgraphErrorStrategy,
    TaskNode,
)
from volnux.engine.default_engine import DefaultWorkflowEngine
from volnux.exceptions import EngineIterationLimitExceeded, TaskCancelledError
from volnux.parser.code_gen import ExecutableASTGenerator
from volnux.parser.grammar import pointy_parser
from volnux.parser.operator import PipeType
from volnux.task import PipelineTask, PipelineTaskGrouping

from .test_default_engine import _make_task


def compile_pointy(source):
    generator = ExecutableASTGenerator(PipelineTask, PipelineTaskGrouping)
    generator.visit_program(pointy_parser(source))
    return generator.generate()


# --- one engine per run ------------------------------------------------------------


@pytest.mark.asyncio
async def test_every_run_gets_an_engine_of_its_own():
    """run_workflow used to share one module-level engine, whose queues and counters
    belong to a single run."""
    engines = []

    class Spy(DefaultWorkflowEngine):
        async def execute(self, root_task, pipeline):
            engines.append(self)
            return EngineResult(status=EngineExecutionResult.COMPLETED)

    with patch.object(run_module, "DefaultWorkflowEngine", Spy):
        await run_module.run_workflow(MagicMock(), MagicMock())
        await run_module.run_workflow(MagicMock(), MagicMock())

    assert len(engines) == 2 and engines[0] is not engines[1]
    assert all(e is not run_module._default_engine for e in engines)


@pytest.mark.asyncio
async def test_a_failed_run_raises_its_error():
    boom = RuntimeError("boom")

    class Failing(DefaultWorkflowEngine):
        async def execute(self, root_task, pipeline):
            return EngineResult(status=EngineExecutionResult.FAILED, error=boom)

    with patch.object(run_module, "DefaultWorkflowEngine", Failing):
        with pytest.raises(RuntimeError, match="boom"):
            await run_module.run_workflow(MagicMock(), MagicMock())


# --- sinks ---------------------------------------------------------------------------


def test_a_sink_is_queued_once_however_often_it_is_offered():
    engine = DefaultWorkflowEngine()
    sink = _make_task()
    sink.get_id.return_value = "sink-1"

    engine._queue_sink(sink)
    engine._queue_sink(sink)
    engine._queue_sink(None)

    assert list(engine.sink_queue) == [sink]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome, drained",
    [
        (EngineExecutionResult.COMPLETED, True),
        (EngineExecutionResult.FAILED, True),
        (EngineExecutionResult.TERMINATED_EARLY, True),
        (EngineExecutionResult.SUSPENDED, False),  # saved with the run, for the resume
    ],
)
async def test_sinks_run_however_the_traversal_ended_except_a_suspension(
    outcome, drained
):
    """Sinks were only drained when the queue emptied normally, so a failure skipped
    the cleanup they exist for."""
    engine = DefaultWorkflowEngine()
    engine._traverse = AsyncMock(return_value=EngineResult(status=outcome))
    engine._drain_sink_nodes = AsyncMock()

    result = await engine._run_loop(MagicMock(), [TaskNode(_make_task())])

    assert result.status == outcome
    assert engine._drain_sink_nodes.await_count == (1 if drained else 0)


@pytest.mark.asyncio
async def test_a_failing_sink_fails_a_completed_run_but_not_one_that_already_failed():
    pipeline = MagicMock()

    done = DefaultWorkflowEngine()
    done._traverse = AsyncMock(
        return_value=EngineResult(status=EngineExecutionResult.COMPLETED)
    )
    done._drain_sink_nodes = AsyncMock(side_effect=RuntimeError("sink broke"))
    result = await done._run_loop(pipeline, [TaskNode(_make_task())])
    assert result.status == EngineExecutionResult.FAILED and "sink broke" in str(
        result.error
    )

    failed = DefaultWorkflowEngine()
    original = EngineResult(
        status=EngineExecutionResult.FAILED, error=ValueError("first")
    )
    failed._traverse = AsyncMock(return_value=original)
    sink = _make_task()
    sink.get_id.return_value = "s"
    failed._queue_sink(sink)
    with patch(
        "volnux.engine.default_engine.ExecutionContext.create_context",
        AsyncMock(side_effect=RuntimeError("sink broke")),
    ):
        result = await failed._run_loop(pipeline, [])
    assert result is original  # the sink's error is logged, not allowed to replace it


@pytest.mark.asyncio
async def test_the_queues_are_reset_for_each_run():
    engine = DefaultWorkflowEngine()
    stale = _make_task()
    stale.get_id.return_value = "stale"
    engine._queue_sink(stale)
    engine._traverse = AsyncMock(
        return_value=EngineResult(status=EngineExecutionResult.COMPLETED)
    )
    engine._drain_sink_nodes = AsyncMock()

    await engine._run_loop(MagicMock(), [TaskNode(_make_task())])

    assert list(engine.sink_queue) == [] and engine._sink_ids == set()


# --- sub-engines -----------------------------------------------------------------------


def fake_engine_factory(engine, behaviours):
    """Make spawn_sub_engine hand out children that behave as ``behaviours`` say."""
    children = []

    async def spawn(
        root_task, pipeline, error_strategy=SubgraphErrorStrategy.TREAT_AS_FAILURE
    ):
        child = MagicMock()
        child.tasks_processed = 1
        child.final_context = None
        child._engine_id = "c%d" % len(children)
        behaviour = behaviours[len(children)]
        child.execute = behaviour
        children.append(child)
        engine.child_engines.append(child)
        return child

    engine.spawn_sub_engine = spawn
    return children


@pytest.mark.asyncio
async def test_a_chain_cancelled_from_outside_is_a_failed_chain_not_a_crash():
    """gather() hands a CancelledError back as a value; it is a BaseException, so it
    was taken for a result and the next line read `.status` from it."""
    engine = DefaultWorkflowEngine()

    async def ok(task, pipeline):
        return EngineResult(status=EngineExecutionResult.COMPLETED, tasks_processed=1)

    async def cancelled(task, pipeline):
        raise asyncio.CancelledError()

    fake_engine_factory(engine, [ok, cancelled])

    results = await engine.execute_subgraphs([_make_task(), _make_task()], MagicMock())

    assert [r.status for r in results] == [
        EngineExecutionResult.COMPLETED,
        EngineExecutionResult.FAILED,
    ]
    assert isinstance(results[1].error, TaskCancelledError)


@pytest.mark.asyncio
async def test_finished_children_are_dropped_and_their_counts_merged():
    """child_engines only ever grew (each child holds its whole context graph)."""
    engine = DefaultWorkflowEngine()

    async def ok(task, pipeline):
        return EngineResult(status=EngineExecutionResult.COMPLETED, tasks_processed=1)

    fake_engine_factory(engine, [ok, ok, ok])

    await engine.execute_subgraphs([_make_task()] * 2, MagicMock())
    assert engine.child_engines == [] and engine.tasks_processed == 2

    fake_engine_factory(engine, [ok])
    await engine.execute_subgraph(_make_task(), MagicMock())
    assert engine.child_engines == [] and engine.tasks_processed == 3


@pytest.mark.asyncio
async def test_max_concurrency_zero_means_no_limit():
    """`max_concurrency or default` turned an explicit 0 into the default."""
    engine = DefaultWorkflowEngine()
    running = peak = 0

    async def slow(task, pipeline):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.02)
        running -= 1
        return EngineResult(status=EngineExecutionResult.COMPLETED)

    fake_engine_factory(engine, [slow] * 4)

    with patch.object(
        DefaultWorkflowEngine, "_cooperative_limit", staticmethod(lambda: 1)
    ):
        await engine.execute_subgraphs(
            [_make_task()] * 4, MagicMock(), max_concurrency=0
        )

    assert peak == 4


@pytest.mark.asyncio
async def test_a_group_with_a_suspended_chain_is_not_retried():
    """A suspended chain looked 'not successful', so the retry loop ran it again and
    asked the outside world the same question twice."""
    engine = DefaultWorkflowEngine()
    group = MagicMock()
    group.options.retry_attempts = 3
    group.options.max_concurrency = None
    group.chains = [_make_task(), _make_task()]
    calls = []

    async def execute_subgraphs(chains, pipeline, **kwargs):
        calls.append(1)
        return [
            EngineResult(status=EngineExecutionResult.SUSPENDED),
            EngineResult(status=EngineExecutionResult.FAILED),
        ]

    engine.execute_subgraphs = execute_subgraphs

    await engine._run_group_chains(group, MagicMock())

    assert len(calls) == 1


def test_a_suspended_chain_is_waiting_not_failed():
    assert not DefaultWorkflowEngine._chain_failed(
        EngineResult(status=EngineExecutionResult.SUSPENDED, final_context=MagicMock())
    )
    assert DefaultWorkflowEngine._chain_failed(
        EngineResult(status=EngineExecutionResult.FAILED)
    )


# --- runaway workflows -------------------------------------------------------------------


def test_the_iteration_limit_comes_from_the_argument_then_the_setting():
    assert DefaultWorkflowEngine(max_iterations=7)._iteration_limit() == 7
    assert DefaultWorkflowEngine(max_iterations=0)._iteration_limit() == 0
    assert DefaultWorkflowEngine()._iteration_limit() == 10000  # the shipped default


@pytest.mark.asyncio
async def test_a_task_that_leads_back_to_itself_stops_at_the_limit():
    task = _make_task()
    task.condition_node.on_success_event = task
    engine = DefaultWorkflowEngine(max_iterations=5)
    ctx = MagicMock()
    ctx.dispatch = AsyncMock()
    ctx.get_switch_request.return_value = None
    ctx.is_multitask.return_value = False
    engine._build_context = AsyncMock(return_value=ctx)
    engine._should_terminate = MagicMock(return_value=False)
    engine._checkpoint_before_task = AsyncMock()
    engine._checkpoint_after_task = AsyncMock()

    result = await asyncio.wait_for(engine.execute(task, MagicMock()), 5)

    assert result.status == EngineExecutionResult.FAILED
    assert isinstance(result.error, EngineIterationLimitExceeded)
    assert ctx.dispatch.await_count == 5


# --- walking the pointers -------------------------------------------------------------------


def chained(pipe, *tasks, close_loop=False):
    for here, there in zip(tasks, tasks[1:]):
        here.condition_node.on_success_pipe = pipe
        here.condition_node.on_success_event = there
    if close_loop:
        tasks[-1].condition_node.on_success_pipe = pipe
        tasks[-1].condition_node.on_success_event = tasks[0]
    return tasks


def test_a_parallel_group_that_loops_back_is_cut_at_the_loop():
    """These walkers followed the pointers with no memory and spun for ever."""
    a, b, c = _make_task(), _make_task(), _make_task()
    chained(PipeType.PARALLELISM, a, b, c, close_loop=True)
    engine = DefaultWorkflowEngine()

    assert DefaultWorkflowEngine._parallel_group(a) == [a, b, c]
    for task in (a, b, c):
        task.is_parallel_execution_node = True
    assert engine._detect_parallel_tasks(a) == {a, b, c}


def test_a_stream_segment_that_loops_back_ends_at_the_loop():
    a, b = _make_task(), _make_task()
    chained(PipeType.PIPE_POINTER, a, b, close_loop=True)
    engine = DefaultWorkflowEngine()
    with patch.object(DefaultWorkflowEngine, "_streams", staticmethod(lambda t: True)):
        segment = engine._detect_stream_segment(a)

    assert segment == [a, b]


# --- suspension -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_storage_failure_does_not_turn_a_suspension_into_a_failure():
    """The save ran inside the per-task try: its error became FAILED (strict) or let
    the loop go on past a suspended task (not strict)."""
    engine = DefaultWorkflowEngine()
    engine._save_suspension_state = AsyncMock(side_effect=OSError("disk full"))

    await engine._persist_suspension(
        MagicMock(), TaskNode(_make_task())
    )  # must not raise

    engine._save_suspension_state.assert_awaited_once()


# --- the task model ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "source, conditional",
    [
        ("A -> B", False),
        ("A (success -> B)", False),
        ("A (failure -> X)", True),  # used to be treated as plain A: X never ran
        ("A (failure -> X, success -> Y)", True),
        ("A (0 -> X, 1 -> Y, 2 -> Z)", True),
    ],
)
def test_a_task_is_conditional_when_the_result_decides_what_runs(source, conditional):
    assert compile_pointy(source).is_conditional is conditional
