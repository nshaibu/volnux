"""PipelineWrapper's messages to the parent: their shape, and the round trip of a
signal from a worker back to a listener in the parent process."""

import logging
import queue
import traceback
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from volnux.exceptions import PipelineExecutionError
from volnux.execution.pipeline.batch import _BatchProcessingMonitor
from volnux.execution.pipeline.wrapper import (
    ExecutionStatusFailed,
    PipelineExecutionState,
    PipelineWrapper,
    _PROTOCOL_KEYS,
)
from volnux.import_utils import import_string
from volnux.signal import signals
from volnux.execution.status import ExecutionStatus


@pytest.fixture
def make_wrapper(pipeline_class, event_class):
    def build(process=None, signals_to_forward=(), logger=True, q=None, pipeline=None):
        if pipeline is None:
            pipeline = pipeline_class(event_class(process).__name__)()
        q = q if q is not None else queue.Queue()
        wrapper = PipelineWrapper(
            pipeline,
            focus_on_signals=list(signals_to_forward),
            signals_queue=q,
            import_string_fn=import_string,
            logger=Mock(spec=logging.Logger) if logger else None,
            wrapper_id="w-proto",
        )
        return wrapper, q

    return build


def drain(q):
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except queue.Empty:
            return out


def lifecycle(q, kind=None):
    items = [m for m in drain(q) if m["message_type"] == "wrapper_lifecycle"]
    return items if kind is None else [m for m in items if m["event_type"] == kind]


# --- shape of lifecycle messages ---------------------------------------------------------


def test_every_lifecycle_message_carries_the_protocol_fields(make_wrapper):
    wrapper, q = make_wrapper()
    wrapper._send_wrapper_lifecycle_event("custom_event")
    [message] = drain(q)

    assert message["message_type"] == "wrapper_lifecycle"
    assert message["event_type"] == "custom_event"
    assert message["wrapper_id"] == "w-proto"
    assert message["pipeline_id"] == wrapper.pipeline.id
    assert isinstance(message["process_id"], int) and message["timestamp"] > 0
    assert message["execution_state"] == "initialized" and message["version"] == "1.0"


def test_extra_data_is_merged_but_cannot_replace_protocol_keys(make_wrapper):
    wrapper, q = make_wrapper()
    hostile = {key: "HIJACKED" for key in _PROTOCOL_KEYS}
    hostile["payload"] = 42

    wrapper._send_wrapper_lifecycle_event("e", hostile)
    [message] = drain(q)

    assert message["payload"] == 42
    assert message["wrapper_id"] == "w-proto" and message["event_type"] == "e"
    assert "HIJACKED" not in message.values()


def test_the_state_reported_is_the_state_at_the_time(make_wrapper):
    wrapper, q = make_wrapper()
    wrapper.execution_state = PipelineExecutionState.RUNNING
    wrapper._send_wrapper_lifecycle_event("e")
    assert drain(q)[0]["execution_state"] == "running"


def test_a_queue_that_rejects_a_message_is_logged_not_raised(make_wrapper):
    q = Mock()
    q.put_nowait.side_effect = queue.Full()
    wrapper, _ = make_wrapper(q=q)

    wrapper._send_wrapper_lifecycle_event("e")

    wrapper._logger.error.assert_called_once()


def test_without_a_logger_a_rejected_message_is_silently_dropped(make_wrapper):
    q = Mock()
    q.put_nowait.side_effect = queue.Full()
    wrapper, _ = make_wrapper(q=q, logger=False)
    wrapper._send_wrapper_lifecycle_event("e")


# --- the end of a run ----------------------------------------------------------------------------


def test_the_end_message_of_a_success_has_no_error_and_a_status(make_wrapper):
    wrapper, q = make_wrapper()
    wrapper.run()

    [end] = lifecycle(q, "pipeline_execution_end")

    assert end["success"] is True and end["status"] == "completed"
    assert "error_message" not in end and "execution_context" not in end


def test_the_start_message_carries_the_pipeline_for_diagnostics(make_wrapper):
    wrapper, q = make_wrapper()
    wrapper.run()
    [start] = lifecycle(q, "pipeline_execution_start")
    assert start["pipeline"] is wrapper.pipeline


def _context(status, failed, first_error_tasks=("TaskA", "TaskB")):
    latest = Mock()
    latest.status = status
    latest.execution_failed.return_value = failed
    chain = Mock()
    chain.get_latest_context.return_value = latest
    node = Mock()
    node.get_task_profiles.return_value = [
        Mock(get_event_name=Mock(return_value=n)) for n in first_error_tasks
    ]
    return chain, node


@pytest.mark.parametrize(
    "status",
    [ExecutionStatus.FAILED, ExecutionStatus.CANCELLED, ExecutionStatus.ABORTED],
)
def test_a_run_that_ended_badly_names_the_failed_tasks_and_status(make_wrapper, status):
    wrapper, q = make_wrapper()
    chain, node = _context(status, failed=True)
    wrapper.pipeline.start = AsyncMock(return_value=chain)
    wrapper.pipeline.get_first_error_execution_node = Mock(return_value=node)

    _, exception = wrapper.run()
    [end] = lifecycle(q, "pipeline_execution_end")

    assert isinstance(exception, PipelineExecutionError)
    assert "TaskA, TaskB" in str(exception) and status.value in str(exception)
    assert exception.params == {"failed_task": "TaskA, TaskB", "status": status.value}
    assert end["success"] is False and end["failed_task"] == "TaskA, TaskB"
    assert wrapper.execution_state is PipelineExecutionState.FAILED


def test_a_failed_context_whose_tasks_cannot_be_listed_is_still_reported(make_wrapper):
    wrapper, q = make_wrapper()
    chain, _ = _context(ExecutionStatus.FAILED, failed=True)
    wrapper.pipeline.start = AsyncMock(return_value=chain)
    wrapper.pipeline.get_first_error_execution_node = Mock(
        return_value=object()
    )  # no get_task_profiles

    _, exception = wrapper.run()

    assert "unknown" in str(exception)


def test_a_run_with_no_context_counts_as_a_success_with_no_status(make_wrapper):
    wrapper, q = make_wrapper()
    wrapper.pipeline.start = AsyncMock(return_value=None)

    _, exception = wrapper.run()
    [end] = lifecycle(q, "pipeline_execution_end")

    assert exception is None and end["success"] is True and end["status"] is None


def test_an_exception_in_the_pipeline_gives_both_an_end_and_a_wrapper_failed_message(
    make_wrapper,
):
    wrapper, q = make_wrapper()
    wrapper.pipeline.start = AsyncMock(side_effect=ValueError("kaput"))

    _, exception = wrapper.run()
    sent = lifecycle(q)
    end = [m for m in sent if m["event_type"] == "pipeline_execution_end"][0]
    failed = [m for m in sent if m["event_type"] == "wrapper_failed"][0]

    assert isinstance(exception, ValueError)
    assert end["success"] is False and end["status"] == ExecutionStatusFailed
    assert failed["error_type"] == "ValueError" and "kaput" in failed["error_message"]
    assert failed["pipeline_state"]["pipeline_class"] == type(wrapper.pipeline).__name__
    assert failed["pipeline_state"]["execution_context_exists"] is False


def test_the_wrapper_finished_message_always_comes_last_with_the_duration(make_wrapper):
    wrapper, q = make_wrapper()
    wrapper.pipeline.start = AsyncMock(side_effect=RuntimeError("x"))
    wrapper.run()

    last = lifecycle(q)[-1]

    assert last["event_type"] == "wrapper_finished"
    assert last["execution_duration"] >= 0 and last["had_exception"] is True


# --- helpers ----------------------------------------------------------------------------------------------


def test_the_traceback_is_serialised_with_its_frames():
    try:
        raise KeyError("deep")
    except KeyError as exc:
        text = PipelineWrapper._format_exception_traceback(exc)
    assert "KeyError" in text and "test_wrapper_protocol" in text


def test_a_traceback_that_cannot_be_formatted_falls_back():
    class Awful(Exception):
        def __str__(self):
            raise RuntimeError("no str")

    with patch.object(traceback, "format_exception", side_effect=RuntimeError("fmt")):
        text = PipelineWrapper._format_exception_traceback(ValueError("v"))
    assert "Failed to format traceback" in text


def test_pipeline_state_info_reports_the_pipeline_or_an_error(make_wrapper):
    wrapper, _ = make_wrapper()
    info = wrapper._get_pipeline_state_info()
    assert info["pipeline_id"] == wrapper.pipeline.id
    assert info["pipeline_class"] == type(wrapper.pipeline).__name__

    wrapper.pipeline = SimpleNamespace()  # no execution_context attribute
    assert wrapper._get_pipeline_state_info() == {
        "error": "Failed to gather pipeline state info"
    }


# --- signals, worker to parent -------------------------------------------------------------------------------


def test_a_real_signal_fired_in_the_pipeline_is_forwarded_with_its_import_path(
    make_wrapper,
):
    wrapper, q = make_wrapper(
        signals_to_forward=["volnux.signal.signals.pipeline_execution_end"]
    )

    wrapper.run()
    forwarded = [m for m in drain(q) if m["message_type"] == "pipeline_signal"]

    assert forwarded, "the end signal of the run should have been forwarded"
    signal = forwarded[0]["kwargs"]["signal"]
    assert signal is signals.pipeline_execution_end
    assert (
        signal.__instance_import_str__ == "volnux.signal.signals.pipeline_execution_end"
    )
    assert forwarded[0]["wrapper_id"] == "w-proto"


def test_listeners_are_removed_when_the_run_is_over(make_wrapper):
    path = "volnux.signal.signals.pipeline_execution_end"
    before = (
        len(signals.pipeline_execution_end._listeners)
        if hasattr(signals.pipeline_execution_end, "_listeners")
        else None
    )
    wrapper, q = make_wrapper(signals_to_forward=[path])

    wrapper.run()
    drain(q)
    wrapper2, q2 = make_wrapper()  # a second run on a clean slate
    wrapper2.run()

    assert not any(m["message_type"] == "pipeline_signal" for m in drain(q2))
    assert wrapper._connected_signals == []
    if before is not None:
        assert len(signals.pipeline_execution_end._listeners) == before


def test_a_forwarded_signal_is_re_emitted_in_the_parent_by_the_monitor(make_wrapper):
    """The full path: worker handler -> queue message -> construct_signal -> a listener.

    It never worked: the handler added `timestamp` and `wrapper_id` to the signal's
    arguments, and a signal rejects arguments it does not declare."""
    wrapper, q = make_wrapper(
        signals_to_forward=["volnux.signal.signals.pipeline_execution_end"]
    )
    received = []

    def listener(sender=None, signal=None, **kwargs):
        received.append(kwargs)

    signals.pipeline_execution_end.connect(None, listener)
    try:
        wrapper.run()
        messages = [m for m in drain(q) if m["message_type"] == "pipeline_signal"]
        received.clear()  # the worker-side emit also reached this process's listener
        for message in messages:
            _BatchProcessingMonitor.construct_signal(message)
    finally:
        signals.pipeline_execution_end.disconnect(None, listener)

    assert received, "the parent's listener should hear the forwarded signal"
    assert received[0]["pipeline_id"] == wrapper.pipeline.id
