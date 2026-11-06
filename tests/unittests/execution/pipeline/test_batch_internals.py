"""BatchPipeline and its monitor: the parts that need no worker processes."""

import asyncio
import logging
import queue
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from tests.unittests.batch_support import EchoBatch, EchoPipeline, ScalarBatch
from volnux.exceptions import PipelineConfigurationError
from volnux.execution.pipeline.batch import (
    BatchPipelineStatus,
    PipelineExecutionMetrics,
    _BatchProcessingMonitor,
    _BatchResult,
)
from volnux.signal.signals import (
    batch_pipeline_finished,
    batch_pipeline_started,
    pipeline_metrics_updated,
)


# --- metrics -----------------------------------------------------------------------------


def test_an_empty_run_has_zero_rates():
    m = PipelineExecutionMetrics()
    assert (
        m.success_rate,
        m.completion_rate,
        m.average_duration,
        m.total_duration,
    ) == (0, 0, 0, 0)


def test_rates_and_durations():
    m = PipelineExecutionMetrics(
        total_pipelines=10, completed=6, failed=2, execution_durations=[1.0, 3.0]
    )
    assert m.success_rate == 75.0  # of those that finished
    assert m.completion_rate == 80.0  # of those expected
    assert m.average_duration == 2.0


def test_total_duration_is_the_span_or_the_time_so_far():
    assert (
        PipelineExecutionMetrics(start_time=10.0, end_time=14.5).total_duration == 4.5
    )
    running = PipelineExecutionMetrics(start_time=time.time() - 2)
    assert 1.5 < running.total_duration < 5


def test_the_result_pair_unpacks():
    pipeline, exception = _BatchResult("p", None)
    assert (pipeline, exception) == ("p", None)


# --- the batch's own state -----------------------------------------------------------------


def test_a_new_batch_is_pending_and_has_run_nothing():
    batch = EchoBatch(items=[1, 2])
    assert batch.status is BatchPipelineStatus.PENDING
    assert batch._configured_pipelines_count == 0 and batch.signals_queue is None
    assert list(batch.get_fields()) == list(EchoPipeline.get_fields())


@pytest.mark.parametrize(
    "max_in_flight, workers, expected",
    [(None, 4, 8), (3, 4, 3), (0, 4, 8), (None, 0, 1)],
)
def test_the_window_defaults_to_twice_the_workers_and_is_at_least_one(
    max_in_flight, workers, expected
):
    batch = EchoBatch(items=[1])
    batch.max_in_flight = max_in_flight
    assert batch._get_window_size(workers) == expected


def test_a_per_instance_batch_size_overrides_the_fields():
    batch = EchoBatch(items=[1])
    field = dict(batch.get_fields())["items"]
    assert batch._get_effective_batch_size(field) == field.batch_size

    batch._instance_batch_sizes["items"] = 5

    assert batch._get_effective_batch_size(field) == 5
    assert EchoBatch(items=[1])._get_effective_batch_size(field) == field.batch_size


def test_non_batch_fields_are_passed_to_every_pipeline_from_the_batch():
    batch = EchoBatch(items=[1, 2], label="shared")
    assert batch._prepare_args_for_non_batch_fields() == {"label": "shared"}


def test_a_non_batch_field_the_caller_left_out_keeps_the_templates_default():
    """The batch passed None for it to every pipeline, overriding the default."""
    assert EchoBatch(items=[1])._prepare_args_for_non_batch_fields() == {"label": "x"}


def test_the_template_pipeline_is_built_with_a_copy_of_the_batchs_metadata():
    """It used update_internal, which merges a dict into a dict, and only caught
    KeyError: any other value raised out of the producer."""
    batch = EchoBatch(items=[1])
    batch.set_internal("trigger", {"name": "t"})
    batch.set_internal("note", "plain string")

    pipeline = batch._initialise_template(items=[1], label="a")

    assert pipeline.get_internal("trigger") == {"name": "t"}
    assert pipeline.get_internal("note") == "plain string"
    pipeline.get_internal("trigger")["name"] = "changed"
    assert batch.get_internal("trigger") == {"name": "t"}  # a copy, not shared


# --- stepping the batch iterators -------------------------------------------------------------


class Field:
    """Stands in for an InputDataField (hashable, like the real ones)."""

    def __init__(self, name, accepted=()):
        self.name, self._isinstance_types = name, accepted


def field(name, accepted=()):
    return Field(name, accepted)


def test_steps_continue_until_every_field_is_exhausted_with_none_for_the_short_ones():
    batch = EchoBatch(items=[1])
    long, short = field("long"), field("short")
    iterators = {long: iter([1, 2, 3]), short: iter(["a"])}

    steps = list(batch._execute_field_batch_processors(iterators))

    assert steps[:3] == [
        {"long": 1, "short": "a"},
        {"long": 2, "short": None},
        {"long": 3, "short": None},
    ]
    assert all(v is None for v in steps[-1].values())  # the final all-exhausted step


def test_every_step_is_a_fresh_dict():
    batch = EchoBatch(items=[1])
    a = field("a")
    steps = batch._execute_field_batch_processors({a: iter([1, 2])})

    first = next(steps)
    first["a"] = "tampered"

    assert next(steps) == {"a": 2}


def test_no_batch_fields_means_no_steps():
    assert list(EchoBatch(items=[1])._execute_field_batch_processors({})) == []


# --- waiting for a slot ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_producer_waits_while_the_window_is_full_and_goes_on_when_a_slot_frees():
    batch = EchoBatch(items=[1])
    batch._window_limit, batch._in_flight = 2, 2
    batch._slot_freed = asyncio.Event()

    waiter = asyncio.ensure_future(batch._wait_for_free_slot())
    await asyncio.sleep(0.02)
    assert not waiter.done()

    batch._in_flight = 1
    batch._slot_freed.set()
    await asyncio.wait_for(waiter, 1)


@pytest.mark.asyncio
async def test_an_open_window_does_not_wait():
    batch = EchoBatch(items=[1])
    batch._window_limit, batch._in_flight = 4, 3
    batch._slot_freed = asyncio.Event()

    await asyncio.wait_for(batch._wait_for_free_slot(), 1)


# --- the result of one future -----------------------------------------------------------------------


async def deliver(outcome):
    seen = []

    async def future():
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    await EchoBatch._handle_future(asyncio.ensure_future(future()), seen.append)
    return seen[0]


@pytest.mark.asyncio
async def test_a_wrapper_result_pair_is_forwarded():
    boom = ValueError("x")
    result = await deliver(("pipeline", boom))
    assert result.pipeline == "pipeline" and result.exception is boom


@pytest.mark.asyncio
async def test_a_bare_pipeline_result_counts_as_a_success():
    result = await deliver("just-a-pipeline")
    assert result.pipeline == "just-a-pipeline" and result.exception is None


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [TimeoutError("slow"), RuntimeError("worker died")])
async def test_a_future_that_raises_is_a_failed_result(error):
    result = await deliver(error)
    assert result.pipeline is None and result.exception is error


@pytest.mark.asyncio
async def test_a_sync_callback_is_fine_too():
    async def future():
        return ("p", None)

    got = []
    await EchoBatch._handle_future(
        asyncio.ensure_future(future()), lambda r: got.append(r)
    )
    assert got[0].pipeline == "p"


# --- run status ------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_batch_without_batch_fields_ends_finished():
    batch = ScalarBatch()
    summary = await batch.execute(lambda r: None)

    assert batch.status is BatchPipelineStatus.FINISHED
    assert summary == {"completed": 1, "failed": 0, "total": 1}


@pytest.mark.asyncio
async def test_an_empty_batch_ends_failed():
    batch = EchoBatch(items=[])
    with pytest.raises(PipelineConfigurationError):
        await batch.execute(lambda r: None)
    assert batch.status is BatchPipelineStatus.FAILED


@pytest.mark.asyncio
async def test_a_cancelled_batch_ends_cancelled():
    batch = ScalarBatch()
    started = asyncio.Event()

    async def slow_start(*a, **k):
        started.set()
        await asyncio.sleep(30)

    with patch.object(batch.get_pipeline_template(), "start", slow_start):
        task = asyncio.ensure_future(batch.execute(lambda r: None))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert batch.status is BatchPipelineStatus.CANCELLED


# --- close / context manager ---------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_close_is_safe_without_a_monitor_and_repeatedly():
    batch = EchoBatch(items=[1])
    await batch.close()
    await batch.close()
    await batch._stop_monitor()  # nothing to stop


@pytest.mark.asyncio
async def test_the_batch_is_an_async_context_manager_that_closes():
    batch = EchoBatch(items=[1])
    batch.close = MagicMock(side_effect=lambda: asyncio.sleep(0))

    async with batch as entered:
        assert entered is batch

    batch.close.assert_called_once()


# --- the monitor ------------------------------------------------------------------------------------------------


@pytest.fixture
def monitor():
    batch = EchoBatch(items=[1, 2])
    batch._configured_pipelines_count = 2
    m = _BatchProcessingMonitor(batch)
    yield m
    m.shutdown()


def test_the_monitor_listens_until_shut_down_and_shutdown_is_idempotent():
    finished = []

    def listener(sender, **kwargs):
        finished.append(kwargs["batch"])

    batch_pipeline_finished.connect(sender=None, listener=listener)
    try:
        m = _BatchProcessingMonitor(EchoBatch(items=[1]))
        assert m._signals_connected
        m.shutdown()
        m.shutdown()
        assert not m._signals_connected and m._shutdown_flag.is_set()
    finally:
        batch_pipeline_finished.disconnect(sender=None, listener=listener)

    assert len(finished) == 1


def test_metrics_can_be_switched_off():
    m = _BatchProcessingMonitor(EchoBatch(items=[1]), enable_metrics=False)
    try:
        m._record_started("w", None)
        m._record_finished("w", True, None)
        m._emit_batch_started()
        m._emit_batch_finished()
        m._emit_metrics_updated()
        assert m.metrics is None and m.get_current_metrics() is None
    finally:
        m.shutdown()


def test_a_start_event_after_the_finish_does_not_count_again(monitor):
    monitor._record_started("w", 1.0)
    monitor._record_finished("w", True, 2.0)
    monitor._record_started("w", 3.0)  # a late duplicate

    assert (
        monitor.metrics.started,
        monitor.metrics.completed,
        monitor.metrics.active,
    ) == (1, 1, 0)


def test_a_finish_without_a_start_is_counted_without_a_duration(monitor):
    monitor._record_finished("w", False, 5.0)
    assert monitor.metrics.failed == 1 and monitor.metrics.execution_durations == []


def test_metrics_updates_are_broadcast_with_the_batch_id(monitor):
    seen = []

    def listener(sender, **kwargs):
        seen.append(kwargs)

    pipeline_metrics_updated.connect(sender=None, listener=listener)
    try:
        monitor._record_started("w", 1.0)
    finally:
        pipeline_metrics_updated.disconnect(sender=None, listener=listener)

    assert (
        seen
        and seen[-1]["batch_id"] == monitor.batch.id
        and seen[-1]["active_count"] == 1
    )


def test_batch_started_sets_the_clock_and_the_expected_total(monitor):
    monitor.metrics.total_pipelines = 0
    monitor._on_batch_started(EchoBatch, total_pipelines=7)

    assert monitor.metrics.start_time and monitor.metrics.total_pipelines == 7


def test_batch_finished_sets_the_end_once(monitor):
    monitor._on_batch_finished(EchoBatch)
    first = monitor.metrics.end_time
    time.sleep(0.01)
    monitor._on_batch_finished(EchoBatch)

    assert first and monitor.metrics.end_time == first


@pytest.mark.parametrize(
    "sender, expected",
    [
        (None, "Unknown"),
        (EchoBatch, "EchoBatch"),
        (EchoBatch(items=[1]), "EchoBatch"),
        ("text", "str"),
    ],
)
def test_sender_names(sender, expected):
    assert _BatchProcessingMonitor._get_sender_name(sender) == expected


def test_a_forwarded_signal_is_re_emitted_in_this_process():
    reemitted = []
    fake_signal = MagicMock()
    fake_signal.__instance_import_str__ = "some.signal"
    parent = MagicMock()
    parent.emit.side_effect = lambda **kw: reemitted.append(kw)

    with patch("volnux.execution.pipeline.batch.import_string", return_value=parent):
        _BatchProcessingMonitor.construct_signal(
            {"kwargs": {"sender": EchoBatch, "signal": fake_signal, "value": 1}}
        )

    assert reemitted == [{"sender": EchoBatch, "value": 1}]


def test_a_signal_message_missing_its_sender_or_signal_is_ignored():
    with patch("volnux.execution.pipeline.batch.import_string") as imp:
        _BatchProcessingMonitor.construct_signal({"kwargs": {"value": 1}})
        _BatchProcessingMonitor.construct_signal({})
    imp.assert_not_called()


def test_the_monitor_thread_processes_messages_until_the_stop_sentinel():
    batch = EchoBatch(items=[1, 2])
    batch._configured_pipelines_count = 2
    batch._signals_queue = queue.Queue()
    m = _BatchProcessingMonitor(batch)
    base = {"message_type": "wrapper_lifecycle", "wrapper_id": "w", "pipeline_id": "p"}
    batch._signals_queue.put(
        {**base, "event_type": "pipeline_execution_start", "timestamp": 1.0}
    )
    batch._signals_queue.put(
        {"message_type": "garbage"}
    )  # a bad message must not stop it
    batch._signals_queue.put(
        {
            **base,
            "event_type": "pipeline_execution_end",
            "success": True,
            "timestamp": 2.0,
        }
    )
    batch._signals_queue.put(None)

    m.start()
    m.join(10)

    assert not m.is_alive()
    assert (m.metrics.started, m.metrics.completed) == (1, 1)
    assert m._shutdown_called


def test_the_monitor_survives_a_message_that_raises():
    batch = EchoBatch(items=[1])
    batch._signals_queue = queue.Queue()
    m = _BatchProcessingMonitor(batch)
    batch._signals_queue.put({"message_type": "wrapper_lifecycle", "event_type": 5})
    batch._signals_queue.put(None)
    m._handle_wrapper_lifecycle_event = MagicMock(side_effect=RuntimeError("bad"))

    m.start()
    m.join(10)

    assert not m.is_alive()


# --- signals from the workers reach listeners in this process -----------------------------


class ForwardingBatch(EchoBatch):
    listen_to_signals = ["volnux.signal.signals.pipeline_execution_end"]


@pytest.mark.asyncio
async def test_a_signal_fired_in_a_worker_process_reaches_a_listener_here():
    """The forwarded signal carried arguments no signal declares (`timestamp`,
    `wrapper_id`), so its re-emission here failed every time and nobody heard it."""
    from volnux.signal.signals import pipeline_execution_end

    heard = []

    def listener(sender=None, signal=None, **kwargs):
        heard.append((kwargs.get("pipeline_id"), kwargs.get("process_id")))

    batch = ForwardingBatch(items=[1, 2, 3, 4])
    pipeline_execution_end.connect(None, listener)
    try:
        summary = await batch.execute(lambda r: None)
    finally:
        pipeline_execution_end.disconnect(None, listener)

    import os

    from_workers = [h for h in heard if h[1] not in (None, os.getpid())]
    assert summary["completed"] == 2
    assert len(from_workers) == 2  # one per pipeline, each from its worker's process
