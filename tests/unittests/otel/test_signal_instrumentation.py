"""Pipeline.start's root span and the signal hooks."""

import asyncio
from types import SimpleNamespace

import pytest
from opentelemetry.trace import SpanKind, StatusCode

from volnux.execution.status import ExecutionStatus
from volnux.otel.instrumentations import signal as signal_module
from volnux.otel.instrumentations.signal import (
    SIGNAL_NAMES,
    SignalInstrumentation,
    connect_signal_instrumentation,
    disconnect_signal_instrumentation,
    instrument_pipeline_start,
    patch_pipeline,
)
from volnux.signal import signals


# ------------------------------------------------------------------ doubles
def fake_run(status=ExecutionStatus.COMPLETED, errors=(), contexts=1):
    contexts_list = [
        SimpleNamespace(
            status=status,
            results=[1, 2],
            errors=list(errors),
            metrics=SimpleNamespace(duration=0.5),
        )
        for _ in range(contexts)
    ]

    class Run:
        def get_latest_context(self):
            return contexts_list[-1]

        def __iter__(self):
            return iter(contexts_list)

        def __bool__(self):
            return True

    return Run()


class FakePipeline:
    id = "pipe-9"
    username = "ann"
    password = "hunter2"

    def __init__(self, run=None, error=None):
        self._run, self._error = run, error

    def get_fields(self):
        return [("username", None), ("password", None)]

    async def start(self, force_rerun=False):
        await asyncio.sleep(0.01)
        if self._error:
            raise self._error
        return self._run


Instrumented = type("Instrumented", (FakePipeline,), {})


def instrumented(pipeline_class=FakePipeline):
    pipeline_class.start = instrument_pipeline_start(pipeline_class.start)
    return pipeline_class


# ------------------------------------------------------------ the root span
class TestPipelineStart:
    def test_the_wrapper_is_a_coroutine_function(self):
        # (it was a plain function: calling it never awaited the pipeline)
        cls = type("P1", (FakePipeline,), {})
        instrumented(cls)
        assert asyncio.iscoroutinefunction(cls.start)

    @pytest.mark.asyncio
    async def test_a_root_server_span_that_lasts_the_run(self, otel):
        cls = instrumented(type("P2", (FakePipeline,), {}))
        run = fake_run()
        assert await cls(run).start() is run
        span = otel.span("workflow.P2")
        assert span.kind is SpanKind.SERVER and span.parent is None
        assert (span.end_time - span.start_time) >= 8_000_000
        assert span.status.status_code is StatusCode.OK

    @pytest.mark.asyncio
    async def test_attributes(self, otel):
        cls = instrumented(type("P3", (FakePipeline,), {}))
        await cls(fake_run(contexts=3)).start(force_rerun=True)
        attrs = otel.span("workflow.P3").attributes
        assert attrs["workflow.name"] == "P3"
        assert attrs["workflow.force_rerun"] is True
        assert attrs["workflow.status"] == "COMPLETED"
        assert attrs["workflow.results_count"] == 2
        assert attrs["workflow.context_count"] == 3
        assert attrs["workflow.total_duration_ms"] == pytest.approx(1500)
        assert attrs["pipeline.id"] == "pipe-9"

    @pytest.mark.asyncio
    async def test_force_rerun_can_be_positional(self, otel):
        cls = instrumented(type("P4", (FakePipeline,), {}))
        await cls(fake_run()).start(True)
        assert otel.span("workflow.P4").attributes["workflow.force_rerun"] is True

    @pytest.mark.asyncio
    async def test_secret_inputs_are_not_recorded(self, otel):
        cls = instrumented(type("P5", (FakePipeline,), {}))
        await cls(fake_run()).start()
        attrs = otel.span("workflow.P5").attributes
        assert attrs["pipeline.input.username"] == "ann"
        assert attrs["pipeline.input.password"] == "[redacted]"
        assert "hunter2" not in str(dict(attrs))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status, ok, event",
        [
            (ExecutionStatus.COMPLETED, True, "workflow_completed"),
            (ExecutionStatus.FAILED, False, "workflow_failed"),
            (ExecutionStatus.CANCELLED, False, "workflow_cancelled"),
            (ExecutionStatus.ABORTED, False, "workflow_aborted"),
        ],
    )
    async def test_the_outcome_is_the_span_status(self, otel, status, ok, event):
        cls = instrumented(type("P6", (FakePipeline,), {}))
        await cls(fake_run(status)).start()
        span = otel.span("workflow.P6")
        assert (span.status.status_code is StatusCode.OK) is ok
        assert event in [e.name for e in span.events]

    @pytest.mark.asyncio
    async def test_the_errors_of_a_failed_run_are_recorded(self, otel):
        cls = instrumented(type("P7", (FakePipeline,), {}))
        await cls(fake_run(ExecutionStatus.FAILED, [ValueError("one"), "two"])).start()
        span = otel.span("workflow.P7")
        messages = [
            e.attributes["exception.message"]
            for e in span.events
            if e.name == "exception"
        ]
        assert messages == ["one", "two"]
        assert span.status.description == "Workflow failed"

    @pytest.mark.asyncio
    async def test_a_start_that_raises(self, otel):
        cls = instrumented(type("P8", (FakePipeline,), {}))
        with pytest.raises(RuntimeError, match="boom"):
            await cls(error=RuntimeError("boom")).start()
        span = otel.span("workflow.P8")
        assert span.status.status_code is StatusCode.ERROR
        names = [e.name for e in span.events]
        assert "workflow_started" in names and "workflow_error" in names

    @pytest.mark.asyncio
    async def test_a_cancelled_start(self, otel):
        cls = instrumented(type("P9", (FakePipeline,), {}))
        task = asyncio.create_task(cls(fake_run()).start())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert otel.span("workflow.P9").status.status_code is StatusCode.ERROR

    @pytest.mark.asyncio
    async def test_no_execution_context(self, otel):
        cls = instrumented(type("P10", (FakePipeline,), {}))
        assert await cls(None).start() is None
        span = otel.span("workflow.P10")
        assert span.attributes["workflow.no_execution"] is True
        assert span.status.status_code is StatusCode.OK

    @pytest.mark.asyncio
    async def test_a_summary_that_fails_does_not_fail_the_run(self, otel):
        cls = instrumented(type("P11", (FakePipeline,), {}))

        class Odd:
            def get_latest_context(self):
                raise RuntimeError("odd")

        odd = Odd()
        assert await cls(odd).start() is odd

    @pytest.mark.asyncio
    async def test_without_a_tracer(self, no_otel):
        cls = instrumented(type("P12", (FakePipeline,), {}))
        run = fake_run()
        assert await cls(run).start() is run


class TestPatchPipeline:
    def test_patch_and_restore(self, otel):
        from volnux.execution.pipeline import Pipeline
        from volnux.otel.instrumentations._patching import unpatch_all

        original = Pipeline.start
        assert patch_pipeline() is True
        assert patch_pipeline() is False  # once
        assert asyncio.iscoroutinefunction(Pipeline.start)
        unpatch_all()
        assert Pipeline.start is original


# ---------------------------------------------------------------- the signals
SIGNAL_ARGUMENTS = {
    "pipeline_pre_init": dict(args=(1, 2), kwargs={"a": 1}),
    "pipeline_post_init": dict(pipeline=FakePipeline()),
    "pipeline_execution_start": dict(pipeline=FakePipeline()),
    "pipeline_execution_end": dict(execution_context=SimpleNamespace(id="c1")),
    "pipeline_stop": dict(
        pipeline=FakePipeline(), execution_context=SimpleNamespace(id="c1")
    ),
    "pipeline_shutdown": dict(pipeline=FakePipeline(), execution_context=None),
    "event_init": dict(event=SimpleNamespace(_task_id="t1"), init_kwargs={}),
    "event_execution_start": dict(
        event=object(), execution_context=SimpleNamespace(id="c1")
    ),
    "event_execution_end": dict(
        event=object(), execution_context=SimpleNamespace(id="c1")
    ),
    "event_execution_retry": dict(
        event=object(),
        execution_context=None,
        task_id="t1",
        backoff=1.5,
        retry_count=2,
        max_attempts=3,
    ),
    "event_execution_retry_done": dict(
        event=object(), execution_context=None, task_id="t1", max_attempts=3
    ),
    "event_execution_failed": dict(
        task_profiles=[SimpleNamespace(get_event_name=lambda: "A")],
        execution_context=SimpleNamespace(id="c1"),
        state=ExecutionStatus.FAILED,
    ),
    "event_execution_cancelled": dict(
        task_profiles=[SimpleNamespace(get_event_name=lambda: "A")],
        execution_context=SimpleNamespace(id="c1"),
        state=ExecutionStatus.CANCELLED,
    ),
    "event_execution_aborted": dict(
        task_profiles=[SimpleNamespace(get_event_name=lambda: "A")],
        execution_context=SimpleNamespace(id="c1"),
        state=ExecutionStatus.ABORTED,
    ),
    "batch_pipeline_started": dict(
        batch=SimpleNamespace(id=5), total_pipelines=3, timestamp="now"
    ),
    "batch_pipeline_finished": dict(
        batch=SimpleNamespace(id=5),
        metrics={},
        success_rate=0.5,
        total_duration=2.0,
        timestamp="now",
    ),
    "pipeline_metrics_updated": dict(
        batch_id=5, metrics={}, active_count=1, completion_rate=0.2, timestamp="now"
    ),
}


class TestSignalHooks:
    def test_there_is_a_hook_for_every_signal_volnux_has(self):
        assert set(SIGNAL_NAMES) == set(SIGNAL_ARGUMENTS)
        for name in SIGNAL_NAMES:
            assert hasattr(signals, name), name
            assert callable(getattr(SignalInstrumentation, f"on_{name}"))

    def test_there_are_seventeen(self):
        assert len(SIGNAL_NAMES) == 17

    @pytest.mark.parametrize("name", SIGNAL_NAMES)
    def test_an_emitted_signal_becomes_an_event_on_the_current_span(self, otel, name):
        connect_signal_instrumentation()
        try:
            with otel.tracer.start_as_current_span("host"):
                getattr(signals, name).emit(None, **SIGNAL_ARGUMENTS[name])
        finally:
            disconnect_signal_instrumentation()
        events = [e.name for e in otel.span("host").events]
        assert events == [f"signal.{name}"]

    def test_the_attributes(self, otel):
        with otel.tracer.start_as_current_span("host"):
            SignalInstrumentation.on_event_execution_retry(
                None, **SIGNAL_ARGUMENTS["event_execution_retry"]
            )
            SignalInstrumentation.on_event_execution_failed(
                None, **SIGNAL_ARGUMENTS["event_execution_failed"]
            )
        retry, failed = otel.span("host").events
        assert dict(retry.attributes) == {
            "event.class": "object",
            "task_id": "t1",
            "retry_count": 2,
            "max_attempts": 3,
            "backoff": 1.5,
        }
        assert dict(failed.attributes) == {
            "tasks": "A",
            "context.id": "c1",
            "state": "FAILED",
        }

    def test_values_that_are_none_are_left_out(self, otel):
        with otel.tracer.start_as_current_span("host"):
            SignalInstrumentation.on_pipeline_shutdown(
                None, pipeline=FakePipeline(), execution_context=None
            )
            SignalInstrumentation.on_event_init(None, event=object(), init_kwargs={})
        shutdown, init = otel.span("host").events
        assert "context.id" not in shutdown.attributes
        assert "event.task_id" not in init.attributes

    def test_without_a_recording_span_nothing_happens(self, otel):
        SignalInstrumentation.on_pipeline_execution_end(
            None, execution_context=SimpleNamespace(id="c")
        )
        assert otel.spans() == []

    def test_without_a_tracer_nothing_happens(self, no_otel):
        SignalInstrumentation.on_pipeline_execution_end(
            None, execution_context=SimpleNamespace(id="c")
        )

    def test_arguments_the_hook_does_not_expect_do_not_break_the_emitter(self, otel):
        with otel.tracer.start_as_current_span("host"):
            SignalInstrumentation.on_event_execution_start(None, unexpected=1)
        assert otel.span("host").events == ()

    def test_extra_arguments_the_signal_adds_are_accepted(self, otel):
        with otel.tracer.start_as_current_span("host"):
            SignalInstrumentation.on_pipeline_post_init(
                None, pipeline=FakePipeline(), signal=object(), process_id=1
            )
        assert len(otel.span("host").events) == 1


class TestConnecting:
    def test_connecting_twice_listens_once(self, otel):
        connect_signal_instrumentation()
        connect_signal_instrumentation()
        try:
            with otel.tracer.start_as_current_span("host"):
                signals.event_init.emit(None, **SIGNAL_ARGUMENTS["event_init"])
        finally:
            disconnect_signal_instrumentation()
        assert len(otel.span("host").events) == 1

    def test_disconnecting_stops_the_events(self, otel):
        connect_signal_instrumentation()
        disconnect_signal_instrumentation()
        with otel.tracer.start_as_current_span("host"):
            signals.event_init.emit(None, **SIGNAL_ARGUMENTS["event_init"])
        assert otel.span("host").events == ()

    def test_the_count_is_returned(self):
        assert connect_signal_instrumentation() == 17
        assert disconnect_signal_instrumentation() == 17

    def test_the_hooks_are_kept_alive(self):
        # (a signal keeps only a weak reference to its listener)
        import gc

        connect_signal_instrumentation()
        try:
            gc.collect()
            assert signals.event_init.emit(None, **SIGNAL_ARGUMENTS["event_init"]) != []
        finally:
            disconnect_signal_instrumentation()
