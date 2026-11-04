"""PipelineWrapper: runs one pipeline in a worker and reports through a queue."""

import logging
import queue
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from volnux import EventBase
from volnux.exceptions import PipelineExecutionError
from volnux.execution.context import ExecutionContext
from volnux.execution.pipeline import Pipeline
from volnux.execution.pipeline.fields import InputDataField
from volnux.execution.pipeline.wrapper import PipelineExecutionState, PipelineWrapper
from volnux.execution.status import ExecutionStatus


class WrapFetch(EventBase):
    def process(self, *args, **kwargs):
        return True, "fetched"


class WrapSave(EventBase):
    def process(self, *args, **kwargs):
        return True, "saved"


class WrapPipeline(Pipeline):
    numbers = InputDataField()

    class Meta:
        pointy = "WrapFetch -> WrapSave"


def drain(q):
    items = []
    while True:
        try:
            items.append(q.get_nowait())
        except queue.Empty:
            return items


def lifecycle(q):
    return [m for m in drain(q) if m.get("message_type") == "wrapper_lifecycle"]


@pytest.fixture
def signals_queue():
    return queue.Queue()


@pytest.fixture
def wrapper(signals_queue):
    return PipelineWrapper(
        WrapPipeline(numbers=[1, 2, 3]),
        focus_on_signals=[],
        signals_queue=signals_queue,
        import_string_fn=lambda path: Mock(),
        logger=Mock(spec=logging.Logger),
        wrapper_id="test_wrapper",
    )


def test_initialization(wrapper):
    assert wrapper.execution_state == PipelineExecutionState.INITIALIZED
    assert wrapper.start_time is None and wrapper.end_time is None
    assert wrapper.exception is None
    assert wrapper.wrapper_id == "test_wrapper"


def test_a_real_pipeline_run_is_reported_as_a_success(wrapper, signals_queue):
    """Used to fail: the wrapper called ExecutionContext methods that did not
    exist, so every successful run ended in the except path."""
    pipeline, exception = wrapper.run()

    assert exception is None
    assert pipeline is wrapper.pipeline
    assert wrapper.execution_state == PipelineExecutionState.FINISHED

    events = lifecycle(signals_queue)
    assert [e["event_type"] for e in events] == [
        "wrapper_started",
        "pipeline_execution_start",
        "pipeline_execution_end",
        "wrapper_finished",
    ]
    end = events[2]
    assert end["success"] is True
    assert not any(e["event_type"] == "wrapper_failed" for e in events)


def test_the_end_event_carries_a_summary_not_the_execution_context(
    wrapper, signals_queue
):
    wrapper.run()
    end = [
        e
        for e in lifecycle(signals_queue)
        if e["event_type"] == "pipeline_execution_end"
    ][0]
    assert "execution_context" not in end
    assert end["status"] == ExecutionStatus.COMPLETED.value


@pytest.mark.parametrize(
    "status,failed",
    [
        (ExecutionStatus.COMPLETED, False),
        (ExecutionStatus.RUNNING, False),
        (ExecutionStatus.PAUSED, False),
        (ExecutionStatus.FAILED, True),
        (ExecutionStatus.ABORTED, True),
        (ExecutionStatus.CANCELLED, True),
    ],
)
def test_execution_failed_reflects_the_context_status(status, failed):
    context = SimpleNamespace(status=status)
    assert ExecutionContext.execution_failed(context) is failed


def _context_with_status(status):
    context = Mock()
    context.get_latest_context.return_value = context
    context.execution_failed.return_value = status != ExecutionStatus.COMPLETED
    context.status = status
    return context


def test_a_failed_context_is_returned_as_an_exception_and_reported(
    wrapper, signals_queue
):
    """A failed run used to come back as (pipeline, None) - indistinguishable
    from success to the batch."""
    failed = _context_with_status(ExecutionStatus.FAILED)
    with patch.object(
        wrapper.pipeline, "get_first_error_execution_node", return_value=None
    ), patch.object(wrapper.pipeline, "start", AsyncMock(return_value=failed)):
        pipeline, exception = wrapper.run()

    assert isinstance(exception, PipelineExecutionError)
    assert wrapper.execution_state == PipelineExecutionState.FAILED

    events = lifecycle(signals_queue)
    end = [e for e in events if e["event_type"] == "pipeline_execution_end"][0]
    assert end["success"] is False
    assert end["status"] == ExecutionStatus.FAILED.value
    assert events[-1]["event_type"] == "wrapper_finished"
    assert events[-1]["final_state"] == PipelineExecutionState.FAILED.value


def test_an_exception_from_start_is_returned_and_reported(wrapper, signals_queue):
    boom = ValueError("Test error")
    with patch.object(wrapper.pipeline, "start", AsyncMock(side_effect=boom)):
        pipeline, exception = wrapper.run()

    assert exception is boom
    assert pipeline is wrapper.pipeline
    assert wrapper.execution_state == PipelineExecutionState.FAILED

    events = lifecycle(signals_queue)
    types = [e["event_type"] for e in events]
    assert types.count("wrapper_failed") == 1
    assert (
        "Test error"
        in [e for e in events if e["event_type"] == "wrapper_failed"][0][
            "error_message"
        ]
    )
    end = [e for e in events if e["event_type"] == "pipeline_execution_end"][0]
    assert end["success"] is False  # the end event is always sent


def test_execution_timing_is_reported(wrapper, signals_queue):
    wrapper.run()
    assert wrapper.end_time >= wrapper.start_time
    finished = [
        e for e in lifecycle(signals_queue) if e["event_type"] == "wrapper_finished"
    ][0]
    assert finished["execution_duration"] >= 0
    assert finished["had_exception"] is False


def test_signals_are_forwarded_with_their_signal_reference(signals_queue):
    fake_signal = Mock()
    wrapper = PipelineWrapper(
        WrapPipeline(numbers=[1]),
        focus_on_signals=["some.signal"],
        signals_queue=signals_queue,
        import_string_fn=lambda path: fake_signal,
        logger=Mock(),
        wrapper_id="w",
    )
    wrapper._connect_signals()
    assert len(wrapper._connected_signals) == 1

    handler = fake_signal.connect.call_args.kwargs["listener"]
    handler(sender=WrapPipeline, pipeline=wrapper.pipeline)

    message = signals_queue.get_nowait()
    assert message["message_type"] == "pipeline_signal"
    assert message["kwargs"]["signal"] is fake_signal
    assert message["wrapper_id"] == "w" and message["version"] == "1"

    wrapper._disconnect_signals()
    fake_signal.disconnect.assert_called_once()
    assert wrapper._connected_signals == []


@pytest.mark.parametrize("error", [ImportError, AttributeError, TypeError])
def test_signal_import_errors_are_logged_not_raised(signals_queue, error):
    def fail(path):
        raise error("nope")

    wrapper = PipelineWrapper(
        WrapPipeline(numbers=[1]),
        focus_on_signals=["invalid.signal.path"],
        signals_queue=signals_queue,
        import_string_fn=fail,
        logger=Mock(),
    )
    wrapper._connect_signals()
    wrapper._logger.warning.assert_called_once()
    assert "Signal import failed" in wrapper._logger.warning.call_args[0][0]


def test_a_full_or_broken_queue_never_stops_the_worker(wrapper):
    wrapper.signals_queue = Mock()
    wrapper.signals_queue.put_nowait.side_effect = queue.Full()
    wrapper._send_wrapper_lifecycle_event("anything")  # must not raise
    wrapper._logger.error.assert_called_once()
    assert "Failed to send wrapper lifecycle event" in (
        wrapper._logger.error.call_args[0][0]
    )


def test_additional_data_cannot_overwrite_protocol_keys(wrapper, signals_queue):
    wrapper._send_wrapper_lifecycle_event(
        "custom", {"wrapper_id": "evil", "event_type": "evil", "extra": 1}
    )
    message = signals_queue.get_nowait()
    assert message["wrapper_id"] == "test_wrapper"
    assert message["event_type"] == "custom"
    assert message["extra"] == 1


def test_state_info_and_traceback_helpers(wrapper):
    info = wrapper._get_pipeline_state_info()
    assert {"pipeline_id", "pipeline_class", "execution_context_exists"} <= set(info)
    try:
        raise ValueError("Test error")
    except ValueError as exc:
        text = wrapper._format_exception_traceback(exc)
    assert "ValueError" in text and "Test error" in text
