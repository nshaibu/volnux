"""BatchPipeline: stream Pipeline instances, each with its own data, into a
process pool."""

import asyncio
import multiprocessing as mp
import os
from unittest.mock import patch

import pytest

from volnux.exceptions import ImproperlyConfigured, PipelineConfigurationError
from volnux.execution.pipeline import BatchPipeline
from volnux.execution.pipeline.batch import BatchPipelineStatus, _BatchResult
from volnux.signal.signals import batch_pipeline_finished
from tests.unittests.batch_support import (
    EchoBatch,
    EchoPipeline,
    FileBatch,
    FailedPipelineBatch,
    RaisingWorkerBatch,
    ScalarBatch,
    SleepyBatch,
    TypedBatch,
)


async def run(batch, callback=None):
    results = []

    def collect(result):
        results.append(result)
        if callback is not None:
            return callback(result)

    summary = await batch.execute(collect)
    return summary, results


def chunks(results):
    return sorted(tuple(r.pipeline.items) for r in results if r.pipeline is not None)


# --------------------------------------------------------------------------
# Construction
# --------------------------------------------------------------------------


def test_a_batch_can_be_constructed_with_the_templates_fields():
    """Used to raise 'object.__init__() takes exactly one argument'."""
    batch = EchoBatch(items=[1, 2, 3, 4], label="hello")
    assert batch.items == [1, 2, 3, 4]
    assert batch.label == "hello"
    assert batch.status == BatchPipelineStatus.PENDING
    assert batch.id


def test_a_batch_requires_a_pipeline_template():
    class NoTemplate(BatchPipeline):
        pass

    with pytest.raises(ImproperlyConfigured):
        NoTemplate()


def test_the_template_must_be_a_pipeline_subclass():
    class Bad(BatchPipeline):
        pipeline_template = str

    with pytest.raises(ImproperlyConfigured):
        Bad(items=[1])


def test_unknown_or_missing_fields_are_rejected_like_the_template_does():
    with pytest.raises(TypeError):
        EchoBatch(unknown=1)
    with pytest.raises(TypeError):
        EchoBatch(items=[1], extra=2)


# --------------------------------------------------------------------------
# Batch processors and value conversion
# --------------------------------------------------------------------------


def test_the_default_list_processor_chunks_by_the_fields_batch_size():
    batch = EchoBatch(items=[1, 2, 3, 4, 5])
    batch._gather_and_init_field_batch_iterators()
    got = [
        kwargs["items"]
        for kwargs in batch._execute_field_batch_processors(batch._field_batch_op_map)
        if kwargs["items"] is not None
    ]
    assert got == [[1, 2], [3, 4], [5]]


def test_a_custom_field_batch_method_replaces_the_default():
    class Custom(EchoBatch):
        def items_batch(self, values, batch_size):
            for i in range(0, len(values), 3):
                yield values[i : i + 3]

    batch = Custom(items=list(range(7)))
    batch._gather_and_init_field_batch_iterators()
    got = [
        k["items"]
        for k in batch._execute_field_batch_processors(batch._field_batch_op_map)
        if k["items"] is not None
    ]
    assert [len(c) for c in got] == [3, 3, 1]


def test_a_custom_batch_method_with_the_wrong_signature_is_rejected():
    class Wrong(EchoBatch):
        def items_batch(self, data, batch_size):
            yield data

    with pytest.raises(ImproperlyConfigured):
        Wrong(items=[1])._gather_and_init_field_batch_iterators()


def test_a_custom_batch_method_may_return_any_iterable():
    """The validator accepts any iterable but the batch calls next() on it: a
    processor returning a plain list used to fail at run time."""

    class ListBatch(EchoBatch):
        def items_batch(self, values, batch_size):
            return [values[:2], values[2:]]

    batch = ListBatch(items=[1, 2, 3, 4, 5])
    batch._gather_and_init_field_batch_iterators()
    got = [
        k["items"]
        for k in batch._execute_field_batch_processors(batch._field_batch_op_map)
        if k["items"] is not None
    ]
    assert got == [[1, 2], [3, 4, 5]]


@pytest.mark.parametrize(
    "field_name,value,expected",
    [
        ("items", (1, 2), [1, 2]),  # tuple chunk -> the field's list
        ("items", [1, 2], [1, 2]),  # already acceptable: untouched
        ("items", None, None),
        ("ids", (1, 2), [1, 2]),  # parameterised List[int]
        ("label", "x", "x"),
    ],
)
def test_a_chunk_is_converted_to_the_fields_collection_type(
    field_name, value, expected
):
    """List[int] used to crash with 'Type List cannot be instantiated' and an
    untyped field with \"'object' object is not callable\"."""
    fields = {**dict(EchoPipeline.get_fields())}
    from tests.unittests.batch_support import TypedPipeline

    fields.update(dict(TypedPipeline.get_fields()))
    converted = BatchPipeline._convert_value_to_field_data_type(
        fields[field_name], value
    )
    assert converted == expected


def test_an_untyped_field_passes_values_through():
    from volnux.execution.pipeline.fields import InputDataField

    assert BatchPipeline._convert_value_to_field_data_type(
        InputDataField(), (1, 2)
    ) == (1, 2)


def test_bytes_are_never_stringified():
    from volnux.execution.pipeline.fields import InputDataField

    field = InputDataField(data_type=str)
    chunk = b"raw bytes"
    assert BatchPipeline._convert_value_to_field_data_type(field, chunk) == chunk


# --------------------------------------------------------------------------
# Execution
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_each_chunk_becomes_its_own_pipeline_with_its_own_data():
    batch = EchoBatch(items=[1, 2, 3, 4, 5], label="L")
    summary, results = await run(batch)

    assert summary == {"completed": 3, "failed": 0, "total": 3}
    assert chunks(results) == [(1, 2), (3, 4), (5,)]
    assert all(r.exception is None for r in results)
    assert {r.pipeline.label for r in results} == {"L"}  # non-batch fields shared
    assert batch.status == BatchPipelineStatus.FINISHED


@pytest.mark.asyncio
async def test_an_async_callback_is_awaited():
    seen = []

    async def callback(result):
        await asyncio.sleep(0)
        seen.append(result)

    batch = EchoBatch(items=[1, 2, 3])
    summary, _ = await run(batch, callback)
    assert len(seen) == 2 == summary["total"]


@pytest.mark.asyncio
async def test_a_failing_item_is_reported_and_the_rest_still_run():
    batch = TypedBatch(ids=[1, 2, "not-an-int", "x", 5, 6])
    summary, results = await run(batch)

    assert summary["total"] == 3
    assert summary["completed"] + summary["failed"] == 3
    assert summary["failed"] == 1
    failed = [r for r in results if r.exception is not None]
    assert failed and failed[0].pipeline is None
    assert isinstance(failed[0].exception, TypeError)


@pytest.mark.asyncio
async def test_a_worker_that_raises_is_a_failed_result():
    summary, results = await run(RaisingWorkerBatch(items=[1, 2, 3]))
    assert summary == {"completed": 0, "failed": 2, "total": 2}
    assert all(isinstance(r.exception, ValueError) for r in results)


@pytest.mark.asyncio
async def test_a_pipeline_that_ended_failed_is_a_failed_result():
    summary, results = await run(FailedPipelineBatch(items=[1, 2, 3]))
    assert summary["failed"] == 2 and summary["completed"] == 0
    assert all(r.pipeline is not None and r.exception is not None for r in results)


@pytest.mark.asyncio
async def test_a_batch_without_batch_fields_runs_one_pipeline_in_process():
    summary, results = await run(ScalarBatch(label="alone"))
    assert summary == {"completed": 1, "failed": 0, "total": 1}
    assert results[0].pipeline.label == "alone"


@pytest.mark.asyncio
async def test_an_empty_batch_is_a_configuration_error():
    batch = EchoBatch(items=[])
    with pytest.raises(PipelineConfigurationError):
        await run(batch)
    assert batch.status == BatchPipelineStatus.FAILED
    assert batch._configured_pipelines_count == 0


@pytest.mark.asyncio
async def test_on_result_must_be_callable():
    with pytest.raises(TypeError):
        await EchoBatch(items=[1]).execute("nope")


@pytest.mark.asyncio
async def test_a_raising_callback_does_not_lose_the_other_results(caplog):
    calls = []

    def callback(result):
        calls.append(result)
        raise RuntimeError("callback bug")

    batch = EchoBatch(items=[1, 2, 3, 4])
    with caplog.at_level("ERROR"):
        summary, _ = await run(batch, callback)
    assert len(calls) == summary["total"] == 2
    assert summary["completed"] == 2  # the batch's own tally is unaffected
    assert "on_result callback raised" in caplog.text  # and the error is not lost


@pytest.mark.asyncio
async def test_a_cancelled_pool_future_is_reported_as_a_failed_result():
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    future.cancel()
    seen = []
    await BatchPipeline._handle_future(future, seen.append)
    assert len(seen) == 1 and seen[0].pipeline is None
    assert isinstance(seen[0].exception, BaseException)


@pytest.mark.asyncio
async def test_cancelling_the_waiting_task_unwinds_without_reporting():
    future = asyncio.get_running_loop().create_future()  # never completes
    seen = []
    task = asyncio.ensure_future(
        BatchPipeline._handle_future(future, seen.append, unwinding=lambda: True)
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert seen == []


# --------------------------------------------------------------------------
# Streaming
# --------------------------------------------------------------------------


@pytest.fixture
def sleepy_env(monkeypatch):
    monkeypatch.setenv("VOLNUX_TEST_SLEEP", "0.3")


@pytest.mark.asyncio
async def test_pipelines_are_created_lazily_within_a_bounded_window(sleepy_env):
    batch = SleepyBatch(items=list(range(8)))
    first_result_seen_with = []

    def callback(result):
        if not first_result_seen_with:
            first_result_seen_with.append(batch._configured_pipelines_count)

    summary, _ = await run(batch, callback)

    assert summary == {"completed": 8, "failed": 0, "total": 8}
    # never more than the window in flight when a pipeline is created
    assert max(batch.seen_in_flight) < SleepyBatch.max_in_flight
    # streaming: the first result arrived before the last pipeline was created
    assert first_result_seen_with[0] < 8


@pytest.mark.asyncio
async def test_the_event_loop_stays_responsive_while_the_pool_works(sleepy_env):
    """Leaving the sync ``with ProcessPoolExecutor`` block used to freeze the
    loop until every worker had finished."""
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.05)
            ticks += 1

    task = asyncio.ensure_future(ticker())
    try:
        await run(SleepyBatch(items=list(range(6))))
    finally:
        task.cancel()
    assert ticks >= 10


@pytest.mark.asyncio
async def test_results_are_delivered_as_they_finish_not_at_the_end(sleepy_env):
    batch = SleepyBatch(items=list(range(6)))
    arrivals = []
    summary, _ = await run(
        batch, lambda r: arrivals.append(asyncio.get_event_loop().time())
    )
    assert summary["total"] == 6
    assert arrivals[-1] - arrivals[0] > 0.2  # spread out, not one burst


# --------------------------------------------------------------------------
# Memory pressure
# --------------------------------------------------------------------------


def _pressured(batch, percent):
    class FakeProcess:
        def memory_percent(self):
            return percent

    return patch("psutil.Process", return_value=FakeProcess())


def test_memory_pressure_shrinks_the_in_flight_window_by_20_percent():
    batch = SleepyBatch(items=[1])
    batch._window_limit = 10
    batch.max_memory_percent = 90.0
    with _pressured(batch, 95.0):
        batch.check_memory_usage()
    assert batch._window_limit == 8


def test_the_window_never_shrinks_below_one_and_is_throttled():
    batch = SleepyBatch(items=[1])
    batch._window_limit = 2
    batch._window_cooldown = 0.0
    with _pressured(batch, 99.0):
        for _ in range(10):
            batch.check_memory_usage()
    assert batch._window_limit == 1


def test_normal_memory_leaves_the_window_alone():
    batch = SleepyBatch(items=[1])
    batch._window_limit = 10
    with _pressured(batch, 50.0):
        batch.check_memory_usage()
    assert batch._window_limit == 10


def test_the_cooldown_stops_back_to_back_shrinking():
    batch = SleepyBatch(items=[1])
    batch._window_limit = 10
    batch._window_cooldown = 60.0
    with _pressured(batch, 99.0):
        batch.check_memory_usage()
        batch.check_memory_usage()
    assert batch._window_limit == 8


# --------------------------------------------------------------------------
# Metrics, signals, cleanup
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_monitor_counts_pipelines_not_batch_fields():
    batch = EchoBatch(items=list(range(10)))
    await run(batch)
    metrics = batch._monitor_thread.metrics
    assert metrics.total_pipelines == 5
    assert metrics.completed == 5 and metrics.failed == 0
    assert metrics.started == 5 and metrics.active == 0
    assert len(metrics.execution_durations) == 5


@pytest.mark.asyncio
async def test_the_monitor_counts_failed_pipelines_as_failed():
    batch = FailedPipelineBatch(items=[1, 2, 3, 4])
    # the stubbed worker sends no lifecycle events: nothing to count, no crash
    summary, _ = await run(batch)
    assert summary["failed"] == 2


@pytest.mark.asyncio
async def test_the_batch_finished_signal_fires_once():
    fired = []

    def listener(sender, **kwargs):
        fired.append(kwargs.get("batch"))

    batch_pipeline_finished.connect(sender=None, listener=listener)
    try:
        batch = EchoBatch(items=[1, 2, 3, 4])
        await run(batch)
    finally:
        batch_pipeline_finished.disconnect(sender=None, listener=listener)
    assert fired.count(batch) == 1


@pytest.mark.asyncio
async def test_listen_to_signals_is_resolved_when_the_batch_runs():
    assert EchoBatch.listen_to_signals is None
    batch = EchoBatch(items=[1, 2])
    await run(batch)
    assert batch._resolved_signals  # resolved at execute time, not import time


def _children():
    return {p.pid for p in mp.active_children()}


@pytest.mark.asyncio
async def test_no_processes_or_threads_are_left_behind():
    before = _children()
    batch = EchoBatch(items=list(range(6)))
    await run(batch)
    assert _children() - before == set()  # pool + manager are gone
    assert not batch._monitor_thread.is_alive()


@pytest.mark.asyncio
async def test_cancelling_the_batch_cleans_up(sleepy_env, monkeypatch):
    monkeypatch.setenv("VOLNUX_TEST_SLEEP", "2")
    before = _children()
    batch = SleepyBatch(items=list(range(10)))
    task = asyncio.ensure_future(run(batch))
    await asyncio.sleep(1.5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert batch.status == BatchPipelineStatus.CANCELLED
    for _ in range(100):  # workers may need a moment to wind down
        if not (_children() - before):
            break
        await asyncio.sleep(0.1)
    assert _children() - before == set()
    assert not batch._monitor_thread.is_alive()


# --------------------------------------------------------------------------
# File fields
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_file_field_streams_its_file_as_chunks_one_pipeline_each(tmp_path):
    """The file path used to be handed to a processor that needs an open stream,
    and a chunk could not be assigned to a file field anyway."""
    from volnux.execution.pipeline import FileChunk

    path = tmp_path / "book.txt"
    text = "0123456789abcdefghij0123"
    path.write_text(text)

    summary, results = await run(FileBatch(book=str(path)))

    assert summary == {"completed": 3, "failed": 0, "total": 3}
    got = sorted((r.pipeline.book for r in results), key=lambda c: c.index)
    assert all(isinstance(c, FileChunk) for c in got)
    assert "".join(c.data for c in got) == text
    assert {c.source for c in got} == {str(path)}


# --------------------------------------------------------------------------
# Monitor accounting (lifecycle events straight in)
# --------------------------------------------------------------------------


def _monitor():
    from volnux.execution.pipeline.batch import _BatchProcessingMonitor

    batch = EchoBatch(items=[1, 2])
    batch._configured_pipelines_count = 3
    monitor = _BatchProcessingMonitor(batch)
    return monitor


def _event(kind, wrapper="w1", ts=100.0, **extra):
    return {
        "message_type": "wrapper_lifecycle",
        "event_type": kind,
        "wrapper_id": wrapper,
        "pipeline_id": "p-" + wrapper,
        "timestamp": ts,
        **extra,
    }


def test_a_successful_run_is_counted_once_with_its_duration():
    monitor = _monitor()
    try:
        monitor._handle_wrapper_lifecycle_event(_event("pipeline_execution_start"))
        assert monitor.metrics.active == 1
        monitor._handle_wrapper_lifecycle_event(
            _event("pipeline_execution_end", ts=102.5, success=True)
        )
        monitor._handle_wrapper_lifecycle_event(_event("wrapper_finished", ts=102.6))
        m = monitor.metrics
        assert (m.started, m.completed, m.failed, m.active) == (1, 1, 0, 0)
        assert m.execution_durations == [2.5]
        assert m.total_pipelines == 3  # the pipelines created so far
    finally:
        monitor.shutdown()


def test_a_failed_worker_reporting_end_and_failure_is_counted_once():
    """A worker that raises sends an end event and a wrapper_failed event."""
    monitor = _monitor()
    try:
        monitor._handle_wrapper_lifecycle_event(_event("pipeline_execution_start"))
        monitor._handle_wrapper_lifecycle_event(
            _event("pipeline_execution_end", success=False)
        )
        monitor._handle_wrapper_lifecycle_event(
            _event("wrapper_failed", error_message="boom")
        )
        m = monitor.metrics
        assert (m.started, m.completed, m.failed, m.active) == (1, 0, 1, 0)
    finally:
        monitor.shutdown()


def test_a_wrapper_that_fails_before_starting_still_counts_as_one_failure():
    monitor = _monitor()
    try:
        monitor._handle_wrapper_lifecycle_event(
            _event("wrapper_failed", error_message="setup failed")
        )
        m = monitor.metrics
        assert (m.started, m.failed, m.active) == (1, 1, 0)
    finally:
        monitor.shutdown()


def test_concurrent_wrappers_are_tracked_separately():
    monitor = _monitor()
    try:
        for wrapper in ("a", "b", "c"):
            monitor._handle_wrapper_lifecycle_event(
                _event("pipeline_execution_start", wrapper=wrapper)
            )
        assert monitor.metrics.active == 3
        monitor._handle_wrapper_lifecycle_event(
            _event("pipeline_execution_end", wrapper="b", success=True)
        )
        assert monitor.metrics.active == 2
        assert monitor.metrics.success_rate == 100.0
    finally:
        monitor.shutdown()
