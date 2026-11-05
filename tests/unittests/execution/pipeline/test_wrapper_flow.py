"""PipelineWrapper: the order of its messages, its states, and its failure paths."""

import logging
import queue
from unittest.mock import Mock

import pytest

from volnux import EventBase, Pipeline
from volnux.execution.pipeline.wrapper import PipelineExecutionState, PipelineWrapper

from .conftest import unique


@pytest.fixture
def make_wrapper(pipeline_class, event_class):
    def build(process=None, signals=(), import_fn=None, q=None):
        ev = event_class(process)
        pipeline = pipeline_class(ev.__name__)()
        q = q if q is not None else queue.Queue()
        wrapper = PipelineWrapper(
            pipeline,
            focus_on_signals=list(signals),
            signals_queue=q,
            import_string_fn=import_fn or (lambda path: Mock()),
            logger=Mock(spec=logging.Logger),
            wrapper_id="w1",
        )
        return wrapper, q

    return build


def messages(q):
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except queue.Empty:
            return out


def kinds(q):
    return [
        m["event_type"] for m in messages(q) if m["message_type"] == "wrapper_lifecycle"
    ]


def test_a_good_run_announces_started_start_end_finished_in_that_order(make_wrapper):
    wrapper, q = make_wrapper()

    pipeline, exception = wrapper.run()

    assert exception is None and pipeline is wrapper.pipeline
    assert kinds(q) == [
        "wrapper_started",
        "pipeline_execution_start",
        "pipeline_execution_end",
        "wrapper_finished",
    ]
    assert wrapper.execution_state is PipelineExecutionState.FINISHED


def test_the_state_moves_through_the_run(make_wrapper):
    seen = []

    async def process(self, *a, **k):
        seen.append("during")
        return True, "ok"

    wrapper, _ = make_wrapper(process)
    assert wrapper.execution_state is PipelineExecutionState.INITIALIZED

    wrapper.run()

    assert seen == ["during"]
    assert (
        wrapper.start_time
        and wrapper.end_time
        and wrapper.end_time >= wrapper.start_time
    )


def test_a_failed_run_stays_failed_and_says_so_in_its_last_message(make_wrapper):
    wrapper, q = make_wrapper()
    wrapper.pipeline.start = Mock(side_effect=RuntimeError("engine down"))

    _, exception = wrapper.run()
    sent = messages(q)

    assert isinstance(exception, RuntimeError)
    assert (
        wrapper.execution_state is PipelineExecutionState.FAILED
    )  # not overwritten by FINISHED
    last = sent[-1]
    assert last["event_type"] == "wrapper_finished"
    assert last["final_state"] == "failed" and last["had_exception"] is True
    failed = [m for m in sent if m.get("event_type") == "wrapper_failed"]
    assert len(failed) == 1 and "engine down" in failed[0]["error_message"]
    assert "RuntimeError" in failed[0]["traceback"]


def test_when_the_signal_setup_fails_the_pipeline_is_not_run(make_wrapper):
    wrapper, q = make_wrapper()
    wrapper._connect_signals = Mock(side_effect=RuntimeError("cannot connect"))
    wrapper.pipeline.start = Mock()

    _, exception = wrapper.run()

    assert isinstance(exception, RuntimeError)
    wrapper.pipeline.start.assert_not_called()
    sent = kinds(q)
    assert "wrapper_failed" in sent and "pipeline_execution_start" not in sent
    assert wrapper.execution_state is PipelineExecutionState.FAILED


def test_signals_are_connected_for_the_run_and_disconnected_after(make_wrapper):
    signal = Mock()
    wrapper, _ = make_wrapper(signals=["a.b"], import_fn=lambda path: signal)

    wrapper.run()

    signal.connect.assert_called_once()
    signal.disconnect.assert_called_once()
    assert wrapper._connected_signals == []


def test_a_failure_to_connect_one_signal_leaves_the_others(make_wrapper):
    good, bad = Mock(), Mock()
    bad.connect.side_effect = RuntimeError("nope")
    wrapper, _ = make_wrapper(
        signals=["bad", "good"], import_fn=lambda p: bad if p == "bad" else good
    )

    wrapper._connect_signals()

    assert [s for s, _ in wrapper._connected_signals] == [good]


def test_a_handler_forwards_the_signal_with_context(make_wrapper):
    signal = Mock()
    wrapper, q = make_wrapper(signals=["a.b"], import_fn=lambda path: signal)
    wrapper._connect_signals()
    handler = signal.connect.call_args.kwargs["listener"]

    handler(sender="S", value=1)
    [message] = messages(q)

    assert message["message_type"] == "pipeline_signal"
    assert message["wrapper_id"] == "w1"
    assert message["kwargs"]["signal"] is signal and message["kwargs"]["value"] == 1
    assert message["kwargs"]["pipeline_id"] == wrapper.pipeline.id


def test_no_signals_requested_connects_nothing(make_wrapper):
    wrapper, _ = make_wrapper()
    wrapper._connect_signals()
    assert wrapper._connected_signals == []


def test_wrapper_ids_default_to_unique_values_that_hide_the_pid(
    pipeline_class, event_class
):
    pipeline = pipeline_class(event_class().__name__)()
    make = lambda: PipelineWrapper(
        pipeline,
        focus_on_signals=[],
        signals_queue=queue.Queue(),
        import_string_fn=lambda p: None,
        logger=None,
    )

    a, b = make(), make()

    assert a.wrapper_id != b.wrapper_id and a.wrapper_id.startswith("wrapper-")


def test_a_wrapper_without_a_logger_still_runs(pipeline_class, event_class):
    pipeline = pipeline_class(event_class().__name__)()
    q = queue.Queue()
    wrapper = PipelineWrapper(
        pipeline,
        focus_on_signals=["x"],
        signals_queue=q,
        import_string_fn=lambda p: (_ for _ in ()).throw(ImportError(p)),
        logger=None,
    )

    _, exception = wrapper.run()

    assert exception is None
