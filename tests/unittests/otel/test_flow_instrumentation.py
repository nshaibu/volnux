"""Flow instrumentation: spans that last as long as the work, and the flow metrics."""

import asyncio
from collections import deque
from types import SimpleNamespace

import pytest
from opentelemetry.trace import StatusCode

from volnux.flows.base import BaseFlow
from volnux.flows.parallel import ParallelFlow
from volnux.flows.single import SingleFlow
from volnux.flows.stream import StreamFlow
from volnux.otel.instrumentations import flow as flow_module
from volnux.otel.instrumentations._patching import is_patched, unpatch_all
from volnux.otel.instrumentations.flow import (
    EventResultError,
    FlowMetrics,
    instrument_flow_run,
    instrument_get_initialized_event,
    instrument_map_events_to_executor,
    instrument_submit_event_to_executor,
)

pytestmark = pytest.mark.asyncio


class MyEvent:
    _task_id = "task-1"
    retry_policy = SimpleNamespace(max_attempts=4)


class MyExecutor:
    max_workers = 8


def task(name="A", parallel=False):
    return SimpleNamespace(
        get_event_name=lambda: name,
        get_id=lambda: 1,
        is_conditional=False,
        is_parallel_execution_node=parallel,
        options=SimpleNamespace(retry_attempts=2),
    )


class FakeFlow:
    """What the instrumentation uses of a flow."""

    def __init__(self, tasks=(), future=None, run_error=None):
        self.task_profiles = deque(tasks)
        self.context = SimpleNamespace(id="ctx-1")
        self._future = future
        self._run_error = run_error

    async def run(self):
        if self._run_error:
            raise self._run_error
        return self._future

    async def _submit_event_to_executor(self, executor, event, event_call_kwargs):
        if self._run_error:
            raise self._run_error
        return self._future

    async def _map_events_to_executor(self, *args):
        return [1, 2, 3]

    def get_initialized_event(self, task_profile):
        if self._run_error:
            raise self._run_error
        return MyEvent(), {"a": 1}


def flow_class(name):
    cls = type(name, (FakeFlow,), {})
    cls.run = instrument_flow_run(cls.run)
    cls._submit_event_to_executor = instrument_submit_event_to_executor(
        cls._submit_event_to_executor
    )
    cls._map_events_to_executor = instrument_map_events_to_executor(
        cls._map_events_to_executor
    )
    cls.get_initialized_event = instrument_get_initialized_event(
        cls.get_initialized_event
    )
    return cls


async def pending_future():
    return asyncio.get_running_loop().create_future()


# ------------------------------------------------------------------ FlowMetrics
class TestFlowMetrics:
    async def test_initialize_creates_the_instruments_once(self, otel):
        FlowMetrics.initialize()
        first = FlowMetrics.flow_duration
        FlowMetrics.initialize()
        assert FlowMetrics.flow_duration is first and FlowMetrics._initialized

    async def test_concurrent_first_use_creates_them_once(self, otel):
        import threading

        created = []
        real = otel.meter_provider.get_meter

        def counting(*a, **k):
            created.append(1)
            return real(*a, **k)

        otel.meter_provider.get_meter = counting
        threads = [threading.Thread(target=FlowMetrics.initialize) for _ in range(8)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        assert len(created) == 1

    async def test_reset_forgets_the_instruments(self, otel):
        FlowMetrics.initialize()
        FlowMetrics.reset()
        assert not FlowMetrics._initialized and FlowMetrics.flow_duration is None

    async def test_recording_before_initialisation_does_nothing(self, otel):
        FlowMetrics.record_flow_duration(5, "F", "success")
        FlowMetrics.increment_error_count("E", "F")
        FlowMetrics.increment_parallel_tasks()
        assert otel.points("flow.duration") == []

    async def test_the_instruments_record_with_their_attributes(self, otel):
        FlowMetrics.initialize()
        FlowMetrics.record_flow_duration(12.5, "SingleFlow", "success")
        FlowMetrics.record_event_duration(3, "Ev", "error", "Thread")
        FlowMetrics.increment_task_count("Ev", "SingleFlow")
        FlowMetrics.increment_error_count("KeyError", "SingleFlow", "Ev")
        FlowMetrics.increment_error_count("KeyError", "SingleFlow")
        FlowMetrics.record_executor_usage("Thread", "SingleFlow")
        FlowMetrics.increment_parallel_tasks(3)
        FlowMetrics.decrement_parallel_tasks(1)

        (flow_point,) = otel.points("flow.duration")
        assert flow_point["attributes"] == {
            "flow.type": "SingleFlow",
            "flow.status": "success",
        }  # (no task count: it would be a label with one value per size)
        assert flow_point["sum"] == 12.5
        assert otel.total("event.duration", **{"event.status": "error"}) == 1
        assert otel.total("task.count", **{"task.type": "Ev"}) == 1
        assert otel.total("error.count", **{"event.name": "Ev"}) == 1
        assert otel.total("error.count") == 2
        assert otel.total("executor.usage", **{"executor.type": "Thread"}) == 1
        assert otel.total("parallel_tasks.active") == 2

    async def test_a_failing_provider_leaves_it_uninitialised(self, monkeypatch, otel):
        def broken():
            raise RuntimeError("no provider")

        monkeypatch.setattr(flow_module.metrics, "get_meter_provider", broken)
        FlowMetrics.reset()
        FlowMetrics.initialize()
        assert not FlowMetrics._initialized


# ----------------------------------------------------------------------- run()
class TestFlowRun:
    async def test_the_span_ends_when_the_future_does_not_when_run_returns(self, otel):
        future = await pending_future()
        flow = flow_class("RunFlow")(tasks=[task("A"), task("B")], future=future)
        assert await flow.run() is future
        assert otel.spans_named("flow.run.RunFlow") == []  # still open
        future.set_result("done")
        await asyncio.sleep(0)
        span = otel.span("flow.run.RunFlow")
        assert span.status.status_code is StatusCode.OK
        assert span.attributes["flow.type"] == "RunFlow"
        assert span.attributes["flow.task_count"] == 2
        assert span.attributes["flow.tasks"] == "A,B"
        assert span.attributes["flow.context_id"] == "ctx-1"
        names = [e.name for e in span.events]
        assert names == ["flow_execution_started", "flow_execution_completed"]

    async def test_the_duration_metric_is_the_works_duration(self, otel):
        future = await pending_future()
        flow = flow_class("TimedFlow")(future=future)
        await flow.run()
        await asyncio.sleep(0.05)
        future.set_result(None)
        await asyncio.sleep(0)
        (point,) = otel.points("flow.duration")
        assert point["sum"] >= 40  # ms: not the instant `run` took to return
        assert point["attributes"]["flow.status"] == "success"

    async def test_a_future_that_fails(self, otel):
        future = await pending_future()
        flow = flow_class("FailFlow")(future=future)
        await flow.run()
        future.set_exception(ValueError("work failed"))
        await asyncio.sleep(0)
        span = otel.span("flow.run.FailFlow")
        assert span.status.status_code is StatusCode.ERROR
        assert any(e.name == "exception" for e in span.events)
        assert otel.total("flow.duration", **{"flow.status": "error"}) == 1
        assert otel.total("error.count", **{"error.type": "ValueError"}) == 1

    async def test_a_cancelled_future(self, otel):
        future = await pending_future()
        flow = flow_class("CancelFlow")(future=future)
        await flow.run()
        future.cancel()
        await asyncio.sleep(0)
        span = otel.span("flow.run.CancelFlow")
        assert span.status.status_code is StatusCode.ERROR
        assert otel.total("flow.duration", **{"flow.status": "cancelled"}) == 1
        assert otel.total("error.count") == 0  # a cancellation is not an error count

    async def test_a_run_that_raises(self, otel):
        flow = flow_class("RaiseFlow")(run_error=RuntimeError("cannot submit"))
        with pytest.raises(RuntimeError, match="cannot submit"):
            await flow.run()
        assert otel.span("flow.run.RaiseFlow").status.status_code is StatusCode.ERROR
        assert otel.total("flow.duration", **{"flow.status": "error"}) == 1

    async def test_a_run_that_is_cancelled_while_submitting(self, otel):
        flow = flow_class("CancelSubmit")(run_error=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            await flow.run()
        assert otel.span("flow.run.CancelSubmit").status.status_code is StatusCode.ERROR
        assert otel.total("flow.duration", **{"flow.status": "cancelled"}) == 1

    async def test_the_parallel_gauge_returns_to_zero_on_every_path(self, otel):
        parallel = [task("A", parallel=True), task("B", parallel=True)]
        for name, setup in (
            ("ok", lambda f: f.set_result(1)),
            ("err", lambda f: f.set_exception(RuntimeError())),
            ("cancel", lambda f: f.cancel()),
        ):
            future = await pending_future()
            flow = flow_class(f"Gauge{name}")(tasks=parallel, future=future)
            await flow.run()
            assert otel.total("parallel_tasks.active") == 2, name
            setup(future)
            await asyncio.sleep(0)
            assert otel.total("parallel_tasks.active") == 0, name
        # and when run itself raises (the counter leaked on this path)
        flow = flow_class("GaugeRaise")(tasks=parallel, run_error=RuntimeError("x"))
        with pytest.raises(RuntimeError):
            await flow.run()
        assert otel.total("parallel_tasks.active") == 0

    async def test_no_parallel_tasks_no_gauge(self, otel):
        future = await pending_future()
        flow = flow_class("SeqFlow")(tasks=[task("A")], future=future)
        await flow.run()
        future.set_result(1)
        await asyncio.sleep(0)
        assert otel.points("parallel_tasks.active") == []
        assert "flow.parallel_tasks" not in otel.span("flow.run.SeqFlow").attributes

    async def test_a_result_that_is_not_a_future_ends_the_span_at_once(self, otel):
        flow = flow_class("PlainFlow")(future="not a future")
        assert await flow.run() == "not a future"
        assert otel.span("flow.run.PlainFlow").status.status_code is StatusCode.OK

    async def test_an_already_finished_future(self, otel):
        future = await pending_future()
        future.set_result(1)
        flow = flow_class("DoneFlow")(future=future)
        await flow.run()
        assert otel.span("flow.run.DoneFlow").status.status_code is StatusCode.OK

    async def test_without_a_tracer(self, no_otel):
        future = await pending_future()
        flow = flow_class("NoTracerFlow")(future=future)
        assert await flow.run() is future

    async def test_work_started_inside_run_nests_under_the_flow_span(self, otel):
        future = await pending_future()
        cls = flow_class("NestFlow")

        async def run_and_submit(self):
            await self._submit_event_to_executor(MyExecutor(), MyEvent(), {})
            return future

        cls.run = instrument_flow_run(run_and_submit)
        inner_future = await pending_future()
        flow = cls(future=inner_future)
        await flow.run()
        inner_future.set_result("e")
        future.set_result("f")
        await asyncio.sleep(0)
        event = otel.span("event.execute.MyEvent")
        assert event.parent.span_id == otel.span("flow.run.NestFlow").context.span_id


# ------------------------------------------------------------------ submit event
class TestSubmitEvent:
    async def submit(self, otel, future, name="SubFlow"):
        flow = flow_class(name)(future=future)
        returned = await flow._submit_event_to_executor(MyExecutor(), MyEvent(), {})
        assert returned is future
        return flow

    async def test_the_span_lasts_until_the_events_future(self, otel):
        future = await pending_future()
        await self.submit(otel, future)
        assert otel.spans_named("event.execute.MyEvent") == []
        future.set_result(SimpleNamespace(error=False))
        await asyncio.sleep(0)
        span = otel.span("event.execute.MyEvent")
        assert span.status.status_code is StatusCode.OK
        assert span.attributes["event.name"] == "MyEvent"
        assert span.attributes["event.task_id"] == "task-1"
        assert span.attributes["executor.type"] == "MyExecutor"
        assert span.attributes["executor.max_workers"] == 8
        assert span.attributes["event.retry.max_attempts"] == 4
        assert span.attributes["context.id"] == "ctx-1"

    async def test_a_future_that_fails_is_an_error_span(self, otel):
        future = await pending_future()
        await self.submit(otel, future)
        future.set_exception(OSError("disk"))
        await asyncio.sleep(0)
        span = otel.span("event.execute.MyEvent")
        assert span.status.status_code is StatusCode.ERROR
        assert span.events[-1].attributes is not None
        assert otel.total("error.count", **{"error.type": "OSError"}) == 1
        assert otel.total("event.duration", **{"event.status": "error"}) == 1

    async def test_an_error_result_is_an_error_span(self, otel):
        # (an event reports failure as a result, not by raising)
        future = await pending_future()
        await self.submit(otel, future)
        future.set_result(SimpleNamespace(error=True, content="bad input"))
        await asyncio.sleep(0)
        span = otel.span("event.execute.MyEvent")
        assert span.status.status_code is StatusCode.ERROR
        assert "bad input" in span.status.description
        assert (
            otel.total("error.count", **{"error.type": EventResultError.__name__}) == 1
        )

    async def test_a_cancelled_future(self, otel):
        # (calling .result() on it raised CancelledError inside the callback)
        future = await pending_future()
        await self.submit(otel, future)
        future.cancel()
        await asyncio.sleep(0)
        assert otel.span("event.execute.MyEvent").status.status_code is StatusCode.ERROR
        assert otel.total("event.duration", **{"event.status": "cancelled"}) == 1

    async def test_a_submission_that_raises(self, otel):
        flow = flow_class("RaiseSub")(run_error=RuntimeError("no executor"))
        with pytest.raises(RuntimeError):
            await flow._submit_event_to_executor(MyExecutor(), MyEvent(), {})
        assert otel.span("event.execute.MyEvent").status.status_code is StatusCode.ERROR

    async def test_metrics_per_submission(self, otel):
        future = await pending_future()
        await self.submit(otel, future)
        future.set_result(SimpleNamespace(error=False))
        await asyncio.sleep(0)
        assert otel.total("task.count", **{"task.type": "MyEvent"}) == 1
        assert otel.total("executor.usage", **{"executor.type": "MyExecutor"}) == 1
        (point,) = otel.points("event.duration")
        assert point["attributes"]["event.status"] == "success"

    async def test_the_arguments_are_passed_on(self, otel):
        seen = {}

        class Flow(FakeFlow):
            async def _submit_event_to_executor(self, executor, event, kwargs):
                seen.update(executor=executor, event=event, kwargs=kwargs)
                return await pending_future()

        Flow._submit_event_to_executor = instrument_submit_event_to_executor(
            Flow._submit_event_to_executor
        )
        executor, event = MyExecutor(), MyEvent()
        await Flow()._submit_event_to_executor(executor, event, {"k": 1})
        assert seen == {"executor": executor, "event": event, "kwargs": {"k": 1}}

    async def test_without_a_tracer(self, no_otel):
        future = await pending_future()
        flow = flow_class("NoTracerSub")(future=future)
        assert (
            await flow._submit_event_to_executor(MyExecutor(), MyEvent(), {}) is future
        )


# ----------------------------------------------------- initialize and map events
class TestInitializeEvent:
    async def test_a_sync_method_stays_sync(self, otel):
        assert not asyncio.iscoroutinefunction(
            flow_class("InitFlow").get_initialized_event
        )

    async def test_the_span_and_its_attributes(self, otel):
        flow = flow_class("InitFlow2")()
        event, call_args = flow.get_initialized_event(task("Work"))
        assert call_args == {"a": 1} and isinstance(event, MyEvent)
        span = otel.span("flow.initialize_event")
        assert span.attributes["event.name"] == "Work"
        assert span.attributes["task.name"] == "Work"
        assert span.attributes["event.retry_attempts"] == 2
        assert span.attributes["event.class"] == "MyEvent"

    async def test_it_is_not_counted_as_a_task(self, otel):
        # (it is called again on cache hits: task.count counts submissions)
        flow = flow_class("InitFlow3")()
        flow.get_initialized_event(task())
        flow.get_initialized_event(task())
        assert otel.points("task.count") == []

    async def test_a_failure_is_an_error_span_and_counted(self, otel):
        flow = flow_class("InitFlow4")(run_error=KeyError("missing"))
        with pytest.raises(KeyError):
            flow.get_initialized_event(task("Work"))
        assert otel.span("flow.initialize_event").status.status_code is StatusCode.ERROR
        assert (
            otel.total(
                "error.count", **{"error.type": "KeyError", "event.name": "Work"}
            )
            == 1
        )

    async def test_a_task_without_retry_options(self, otel):
        profile = task()
        profile.options = None
        flow_class("InitFlow5")().get_initialized_event(profile)
        assert (
            "event.retry_attempts" not in otel.span("flow.initialize_event").attributes
        )


class TestMapEvents:
    async def test_a_span_with_the_number_of_futures(self, otel):
        flow = flow_class("MapFlow")(tasks=[task(), task()])
        assert await flow._map_events_to_executor() == [1, 2, 3]
        span = otel.span("flow.map_events_to_executor")
        assert span.attributes["flow.futures"] == 3
        assert span.attributes["flow.task_count"] == 2


# ---------------------------------------------------------------------- patching
class TestPatching:
    async def test_the_real_concrete_flows_are_patched(self, otel):
        # (it imported SingleTaskFlow / MultiTaskFlow, which do not exist, and its
        # hasattr guard saw the base class's marker: nothing was ever patched)
        flow_module.patch_all_flow_components()
        for cls in (SingleFlow, ParallelFlow, StreamFlow):
            assert is_patched(cls, "run"), cls.__name__
        for name in (
            "get_initialized_event",
            "_submit_event_to_executor",
            "_map_events_to_executor",
        ):
            assert is_patched(BaseFlow, name)

    async def test_the_abstract_base_run_is_left_alone(self, otel):
        flow_module.patch_all_flow_components()
        assert not is_patched(BaseFlow, "run")

    async def test_patching_twice_does_nothing_more(self, otel):
        flow_module.patch_all_flow_components()
        assert flow_module.patch_flow_class() == 0
        assert flow_module.patch_concrete_flow_classes(SingleFlow, ParallelFlow) == 0

    async def test_a_class_that_does_not_define_run_is_skipped(self, otel):
        class Inherits(SingleFlow):
            pass

        assert flow_module.patch_concrete_flow_classes(Inherits) == 0

    async def test_unpatching_restores_the_flows(self, otel):
        before = (
            SingleFlow.__dict__["run"],
            BaseFlow.__dict__["_submit_event_to_executor"],
        )
        flow_module.patch_all_flow_components()
        unpatch_all()
        assert (
            SingleFlow.__dict__["run"],
            BaseFlow.__dict__["_submit_event_to_executor"],
        ) == before
