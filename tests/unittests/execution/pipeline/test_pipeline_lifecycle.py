"""Pipeline: running, reporting how a run ended, stop/pause/shutdown, and inspection."""

import asyncio
import io
import logging
import pickle
from unittest.mock import AsyncMock, MagicMock

import pytest

from volnux import Pipeline
from volnux.exceptions import EventDoesNotExist, EventDone
from volnux.execution.pipeline.fields import InputDataField
from volnux.execution.status import ExecutionStatus
from volnux.signal import signals

from .conftest import unique


@pytest.fixture
def fired():
    """Every pipeline signal fired, as ``(name, kwargs)``."""
    log = []
    names = [
        "pipeline_pre_init",
        "pipeline_post_init",
        "pipeline_execution_start",
        "pipeline_execution_end",
        "pipeline_stop",
        "pipeline_shutdown",
    ]
    keep = []
    for name in names:

        def make(name):
            def listener(sender=None, signal=None, **kwargs):
                log.append((name, kwargs))

            return listener

        fn = make(name)
        keep.append(fn)
        getattr(signals, name).connect(None, fn)
    yield log
    for name, fn in zip(names, keep):
        getattr(signals, name).disconnect(None, fn)


def names(fired, pipeline=None):
    return [
        n
        for n, kw in fired
        if pipeline is None
        or kw.get("pipeline") is pipeline
        or kw.get("execution_context") is not None
    ]


@pytest.fixture
def simple(pipeline_class, event_class):
    """A -> B, with one string field."""
    a = event_class(name="A")
    b = event_class(name="B")
    cls = pipeline_class(
        f"{a.__name__} -> {b.__name__}",
        label=InputDataField(data_type=str, required=False),
    )
    cls.events = (a, b)
    return cls


# --- construction ----------------------------------------------------------------------------


def test_construction_fires_pre_and_post_init_and_starts_resting(simple, fired):
    pipeline = simple(label="x")

    assert [
        n for n, _ in fired if n in ("pipeline_pre_init", "pipeline_post_init")
    ] == [
        "pipeline_pre_init",
        "pipeline_post_init",
    ]
    assert pipeline.label == "x"
    assert pipeline.execution_context is None
    assert pipeline._stop_requested is False and pipeline._run_gate is None


def test_a_rejected_construction_fires_no_signal(simple, fired):
    with pytest.raises(TypeError):
        simple(unknown=1)
    assert not [n for n, _ in fired if n == "pipeline_pre_init"]


def test_identity_cache_key_equality_and_hash(simple):
    one, two = simple(), simple()

    assert one.id and one.id != two.id
    assert one.get_cache_key() == f"pipeline_{type(one).__name__}_{one.id}"
    assert one == one and one != two and one != "not a pipeline"
    assert len({one, two, one}) == 2


# --- start --------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_runs_the_workflow_and_returns_the_context_chain(simple):
    pipeline = simple()

    context = await pipeline.start()

    assert context is pipeline.execution_context
    assert [r.event_name for c in context for r in c.get_results()] == [
        e.__name__ for e in simple.events
    ]
    assert context.get_latest_context().status == ExecutionStatus.COMPLETED


@pytest.mark.asyncio
async def test_start_fires_start_then_end(simple, fired):
    pipeline = simple()

    await pipeline.start()

    run = [n for n, _ in fired if n.startswith("pipeline_execution")]
    assert run == ["pipeline_execution_start", "pipeline_execution_end"]


@pytest.mark.asyncio
async def test_a_pipeline_runs_once_unless_forced(simple, fired):
    pipeline = simple()
    await pipeline.start()
    first_context = pipeline.execution_context

    with pytest.raises(EventDone, match="force_rerun"):
        await pipeline.start()
    assert pipeline.execution_context is first_context
    # the refused start was not announced: it has no matching end signal
    assert [n for n, _ in fired].count("pipeline_execution_start") == 1

    again = await pipeline.start(force_rerun=True)
    assert again is not first_context and again is pipeline.execution_context


@pytest.mark.asyncio
async def test_a_failing_run_raises_and_reports_no_end(
    pipeline_class, event_class, fired
):
    ev = event_class()
    cls = pipeline_class(ev.__name__)
    pipeline = cls()
    with pytest.MonkeyPatch.context() as mp:
        import volnux.engine as engine

        async def boom(root, pipeline):
            raise RuntimeError("engine broke")

        mp.setattr(engine, "run_workflow", boom)
        with pytest.raises(RuntimeError, match="engine broke"):
            await pipeline.start()

    assert "pipeline_execution_end" not in [n for n, _ in fired]


@pytest.mark.asyncio
async def test_start_clears_a_previous_stop_request(simple):
    pipeline = simple()
    await pipeline.stop()
    assert pipeline._stop_requested is True

    await pipeline.start()

    assert pipeline._stop_requested is False
    assert (
        pipeline.execution_context.get_latest_context().status
        == ExecutionStatus.COMPLETED
    )


# --- report_outcome -----------------------------------------------------------------------------


def with_status(pipeline, status):
    context = MagicMock()
    context.status = status
    chain = MagicMock()
    chain.get_latest_context.return_value = context
    pipeline.execution_context = chain
    return chain


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status, signal",
    [
        (ExecutionStatus.CANCELLED, "pipeline_stop"),
        (ExecutionStatus.ABORTED, "pipeline_shutdown"),
        (ExecutionStatus.COMPLETED, "pipeline_execution_end"),
        (ExecutionStatus.FAILED, "pipeline_execution_end"),
    ],
)
async def test_the_signal_reflects_how_the_run_ended(simple, fired, status, signal):
    pipeline = simple()
    chain = with_status(pipeline, status)

    assert await pipeline.report_outcome() is chain

    assert [n for n, _ in fired if n.startswith("pipeline_") and "init" not in n] == [
        signal
    ]


@pytest.mark.asyncio
async def test_a_suspended_run_is_not_reported_as_finished(simple, fired):
    pipeline = simple()
    chain = with_status(pipeline, ExecutionStatus.SUSPENDED)

    assert await pipeline.report_outcome() is chain

    assert [n for n, _ in fired if "init" not in n] == []


@pytest.mark.asyncio
async def test_a_run_with_no_context_still_reports_its_end(simple, fired):
    pipeline = simple()

    assert await pipeline.report_outcome() is None

    assert [n for n, _ in fired if "init" not in n] == ["pipeline_execution_end"]


# --- shutdown, stop, pause, resume --------------------------------------------------------------


@pytest.mark.asyncio
async def test_shutdown_aborts_the_latest_context_and_announces_it(simple, fired):
    pipeline = simple()
    chain = with_status(pipeline, ExecutionStatus.COMPLETED)
    latest = chain.get_latest_context.return_value
    latest.abort = AsyncMock()

    await pipeline.shutdown()

    latest.abort.assert_awaited_once()
    assert [n for n, _ in fired if "init" not in n] == ["pipeline_shutdown"]


@pytest.mark.asyncio
async def test_shutdown_accepts_a_plain_abort(simple):
    pipeline = simple()
    chain = with_status(pipeline, ExecutionStatus.COMPLETED)
    latest = chain.get_latest_context.return_value
    latest.abort = MagicMock(return_value=None)

    await pipeline.shutdown()

    latest.abort.assert_called_once()


@pytest.mark.asyncio
async def test_shutdown_and_stop_before_any_run_do_nothing_harmful(simple, fired):
    pipeline = simple()

    await pipeline.shutdown()
    await pipeline.stop()

    assert pipeline._stop_requested is True
    assert [n for n, _ in fired if "init" not in n] == []  # no run to announce


@pytest.mark.asyncio
async def test_stop_cancels_the_latest_context_announces_and_releases_a_pause(
    simple, fired
):
    pipeline = simple()
    chain = with_status(pipeline, ExecutionStatus.RUNNING)
    latest = chain.get_latest_context.return_value
    latest.cancel_async = AsyncMock()
    latest.pause_tasks = AsyncMock(return_value=0)
    await pipeline.pause()
    gate = pipeline._run_gate
    assert gate is not None and not gate.is_set()

    await pipeline.stop()

    latest.cancel_async.assert_awaited_once()
    assert gate.is_set() and pipeline._run_gate is None
    assert [n for n, _ in fired if "init" not in n] == ["pipeline_stop"]


@pytest.mark.asyncio
async def test_stop_falls_back_to_a_plain_cancel(simple):
    pipeline = simple()
    chain = with_status(pipeline, ExecutionStatus.RUNNING)
    latest = chain.get_latest_context.return_value
    del latest.cancel_async
    latest.cancel = MagicMock(return_value=None)

    await pipeline.stop()

    latest.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_pause_and_resume_hold_and_release_the_gate(simple):
    pipeline = simple()
    chain = with_status(pipeline, ExecutionStatus.RUNNING)
    latest = chain.get_latest_context.return_value
    latest.pause_tasks = AsyncMock(return_value=2)
    latest.resume_tasks = AsyncMock(return_value=2)

    assert await pipeline.pause() == 2
    gate = pipeline._run_gate
    assert isinstance(gate, asyncio.Event) and not gate.is_set()
    assert await pipeline.pause() == 2 and pipeline._run_gate is gate  # one gate

    assert await pipeline.resume() == 2
    assert gate.is_set() and pipeline._run_gate is None


@pytest.mark.asyncio
async def test_pause_and_resume_with_nothing_running_count_nothing(simple):
    pipeline = simple()

    assert await pipeline.pause() == 0
    assert pipeline._run_gate is not None  # held, so the run will wait when it starts
    assert await pipeline.resume() == 0
    assert pipeline._run_gate is None


# --- inspecting the graph and the run -------------------------------------------------------------


def test_get_task_by_id_finds_every_task_and_rejects_others(simple):
    pipeline = simple()
    start = simple.get_pipeline_state().start
    second = start.condition_node.on_success_event

    assert pipeline.get_task_by_id(start.get_id()) is start
    assert pipeline.get_task_by_id(second.get_id()) is second
    with pytest.raises(EventDoesNotExist) as caught:
        pipeline.get_task_by_id("no-such-task")
    assert "no-such-task" in str(caught.value)


def test_the_tree_has_one_node_per_task_in_order(simple):
    tree = simple().get_pipeline_tree()
    a, b = simple.events

    tags = [tree[n].tag for n in tree.expand_tree(mode=1)]
    assert tags == [a.__name__, b.__name__]
    assert tree.depth() == 1


def test_the_tree_marks_branches_and_sinks(pipeline_class, event_class):
    a, no, yes, join = (event_class(name=n) for n in ("A", "No", "Yes", "Join"))
    cls = pipeline_class(
        f"{a.__name__} (failure -> {no.__name__}, success -> {yes.__name__}) -> {join.__name__}"
    )

    tree = cls().get_pipeline_tree()
    tags = {tree[n].tag for n in tree.expand_tree()}

    assert f"{a.__name__} (?)" in tags
    assert f"{no.__name__} (No)" in tags and f"{yes.__name__} (Yes)" in tags
    assert f"{join.__name__} (Sink)" in tags


def test_the_ascii_graph_lists_the_tasks_and_goes_to_a_stream_too(simple):
    pipeline = simple()
    stream = io.StringIO()

    text = pipeline.draw_ascii_graph(stream)

    assert stream.getvalue() == text
    for event in simple.events:
        assert event.__name__ in text


def test_without_graphviz_drawing_an_image_warns_instead_of_failing(
    simple, monkeypatch, caplog
):
    import volnux.execution.pipeline.pipeline as module

    monkeypatch.setattr(module, "graphviz", None)
    with caplog.at_level(logging.WARNING, logger=module.logger.name):
        simple().draw_graphviz_image()

    assert "graphviz is not installed" in caplog.text


def test_the_first_failed_context_is_found(simple):
    pipeline = simple()
    assert pipeline.get_first_error_execution_node() is None  # nothing ran

    ok_1, bad, ok_2 = MagicMock(), MagicMock(), MagicMock()
    ok_1.execution_failed.return_value = False
    bad.execution_failed.return_value = True
    ok_2.execution_failed.return_value = False
    ok_1.next_context, bad.next_context, ok_2.next_context = bad, ok_2, None
    pipeline.execution_context = ok_1

    assert pipeline.get_first_error_execution_node() is bad

    bad.execution_failed.return_value = False
    assert pipeline.get_first_error_execution_node() is None


@pytest.mark.asyncio
async def test_a_pipeline_that_ran_has_no_failed_node(simple):
    pipeline = simple()
    await pipeline.start()
    assert pipeline.get_first_error_execution_node() is None


# --- restoring and pickling ----------------------------------------------------------------------


def test_load_class_by_id_restores_the_fields_and_the_id(pipeline_class, event_class):
    cls = pipeline_class(
        event_class().__name__,
        a=InputDataField(data_type=str, required=False),
        b=InputDataField(data_type=int, required=False),
    )
    original = cls(a="x", b=3)

    restored = cls.load_class_by_id(original.get_cache_key())

    assert (restored.a, restored.b) == ("x", 3)
    assert restored.id == original.get_cache_key()


def test_load_class_by_id_of_an_unknown_key_gives_a_default_instance(
    pipeline_class, event_class
):
    cls = pipeline_class(
        event_class().__name__, a=InputDataField(data_type=str, required=False)
    )

    fresh = cls.load_class_by_id("pipeline_never_seen")

    assert fresh.a is None


def test_a_pipeline_survives_pickling_with_its_fields(pipeline_class, event_class):
    ev = event_class()
    cls = pipeline_class(ev.__name__, a=InputDataField(data_type=str, required=False))
    # pickled by reference: the class must be importable
    import sys

    module = sys.modules[__name__]
    cls.__module__ = __name__
    cls.__qualname__ = cls.__name__
    setattr(module, cls.__name__, cls)
    try:
        pipeline = cls(a="payload")
        clone = pickle.loads(pickle.dumps(pipeline))
    finally:
        delattr(module, cls.__name__)

    assert clone.a == "payload" and clone.id == pipeline.id
    assert clone.execution_context is None


def test_load_class_by_id_restores_the_framework_metadata_too(
    pipeline_class, event_class
):
    """The metadata is cached beside the fields; passed to the constructor as if it
    were one, it was an unexpected argument."""
    cls = pipeline_class(
        event_class().__name__, a=InputDataField(data_type=str, required=False)
    )
    original = cls(a="x")
    original.set_internal("trigger", {"name": "nightly"})

    restored = cls.load_class_by_id(original.get_cache_key())

    assert restored.a == "x"
    assert restored.get_internal("trigger") == {"name": "nightly"}
    restored.get_internal("trigger")["name"] = "changed"
    assert original.get_internal("trigger") == {"name": "nightly"}  # not shared
