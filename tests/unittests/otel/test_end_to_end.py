"""A real pipeline run with the instrumentation on: the spans, their hierarchy and
the metrics, with nothing mocked."""

import pytest
from opentelemetry.trace import StatusCode

from tests.integration.support import Trace, make_event, run, step
from volnux.executors import shared
from volnux.otel.instrumentations.context_coordinator import (
    patch_all_execution_components,
)
from volnux.otel.instrumentations.engine import patch_engine
from volnux.otel.instrumentations.flow import patch_all_flow_components
from volnux.otel.instrumentations.signal import (
    connect_signal_instrumentation,
    disconnect_signal_instrumentation,
    patch_all_pipeline_components,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
def instrumented(otel):
    patch_all_execution_components()
    patch_all_pipeline_components()
    patch_all_flow_components()
    patch_engine()
    yield otel
    disconnect_signal_instrumentation()
    shared.shutdown_shared_pools()


def parent_name(otel, span):
    by_id = {s.context.span_id: s for s in otel.spans()}
    parent = by_id.get(span.parent.span_id) if span.parent else None
    return parent.name if parent else None


def workflow_span(otel):
    roots = [s for s in otel.spans() if s.name.startswith("workflow.")]
    assert len(roots) == 1
    return roots[0]


async def test_a_run_still_works_with_the_instrumentation_on(instrumented):
    # (Pipeline.start used to be wrapped by a plain function: every start raised)
    trace = Trace()
    a = make_event("A", step(trace, "a"))
    pipeline, error = await run(a.__name__)
    assert error is None
    assert [e for kind, e in trace.log if kind == "start"] == ["a"]


async def test_the_span_hierarchy(instrumented):
    otel = instrumented
    trace = Trace()
    a = make_event("A", step(trace, "a"))
    b = make_event("B", step(trace, "b"))
    pipeline, error = await run(f"{a.__name__} -> {b.__name__}")
    assert error is None

    root = workflow_span(otel)
    assert root.parent is None
    assert root.status.status_code is StatusCode.OK
    assert root.attributes["workflow.status"] == "COMPLETED"
    assert root.attributes["workflow.context_count"] == 2

    engine = otel.span("engine.execute")
    assert parent_name(otel, engine) == root.name

    dispatches = otel.spans_named("context.dispatch")
    assert len(dispatches) == 2
    # the first context hangs under the engine, the next under the previous one
    assert sorted(parent_name(otel, d) for d in dispatches) == [
        "context.dispatch",
        "engine.execute",
    ]

    for name in (a.__name__, b.__name__):
        event = otel.span(f"event.execute.{name}")
        assert parent_name(otel, event) == "flow.run.SingleFlow"
        assert event.status.status_code is StatusCode.OK

    for flow in otel.spans_named("flow.run.SingleFlow"):
        assert parent_name(otel, flow) == "context.dispatch"
    for coordinator in otel.spans_named("coordinator.execute_async"):
        assert parent_name(otel, coordinator) == "context.dispatch"

    # one trace
    assert len({s.context.trace_id for s in otel.spans()}) == 1


async def test_every_span_is_ended(instrumented):
    otel = instrumented
    a = make_event("A", step(Trace(), "a"))
    await run(a.__name__)
    assert all(s.end_time is not None for s in otel.spans())


async def test_a_failing_event_makes_an_error_span(instrumented):
    otel = instrumented

    async def boom(self, *args, **kwargs):
        raise RuntimeError("event failed")

    bad = make_event("Bad", boom)
    pipeline, error = await run(bad.__name__)

    event = otel.span(f"event.execute.{bad.__name__}")
    assert event.status.status_code is StatusCode.ERROR
    # (an event that fails is a result the workflow routes on, not a failed
    # workflow: the context completes)
    root = workflow_span(otel)
    assert root.status.status_code is StatusCode.OK
    assert root.attributes["workflow.results_count"] == 1


async def test_the_event_span_lasts_as_long_as_the_event(instrumented):
    # (it used to end when the event was *submitted*)
    otel = instrumented
    slow = make_event("Slow", step(Trace(), "s", delay=0.1))
    await run(slow.__name__)
    event = otel.span(f"event.execute.{slow.__name__}")
    assert (event.end_time - event.start_time) >= 80_000_000  # ns


async def test_flow_and_event_metrics(instrumented):
    otel = instrumented
    a = make_event("A", step(Trace(), "a"))
    await run(a.__name__)
    assert otel.total("flow.duration", **{"flow.type": "SingleFlow"}) >= 1
    assert otel.total("event.duration", **{"event.name": a.__name__}) >= 1
    assert otel.total("task.count", **{"task.type": a.__name__}) == 1
    assert otel.total("executor.usage") == 1


async def test_a_failing_event_counts_an_error(instrumented):
    otel = instrumented

    async def boom(self, *args, **kwargs):
        raise RuntimeError("x")

    bad = make_event("Bad", boom)
    await run(bad.__name__)
    assert otel.total("error.count", **{"event.name": bad.__name__}) >= 1


async def test_signals_are_events_on_the_current_span(instrumented):
    otel = instrumented
    a = make_event("A", step(Trace(), "a"))
    await run(a.__name__)
    names = {e.name for s in otel.spans() for e in s.events}
    assert "signal.pipeline_execution_start" in names
    assert "signal.event_execution_start" in names
    assert "workflow_started" in names and "workflow_completed" in names


async def test_the_signal_hooks_can_be_connected_again_without_duplicates(instrumented):
    otel = instrumented
    connect_signal_instrumentation()
    connect_signal_instrumentation()
    a = make_event("A", step(Trace(), "a"))
    await run(a.__name__)
    starts = [
        e
        for s in otel.spans()
        for e in s.events
        if e.name == "signal.pipeline_execution_start"
    ]
    assert len(starts) == 1


async def test_patching_again_does_not_double_the_spans(instrumented):
    otel = instrumented
    patch_all_execution_components()
    patch_all_pipeline_components()
    patch_all_flow_components()
    patch_engine()
    a = make_event("A", step(Trace(), "a"))
    await run(a.__name__)
    assert len(otel.spans_named("engine.execute")) == 1
    assert len(otel.spans_named("flow.run.SingleFlow")) == 1
    assert len(otel.spans_named("coordinator.execute_async")) == 1


async def test_without_a_tracer_nothing_is_recorded(otel, no_otel):
    patch_all_execution_components()
    patch_all_pipeline_components()
    patch_all_flow_components()
    patch_engine()
    try:
        a = make_event("A", step(Trace(), "a"))
        pipeline, error = await run(a.__name__)
        assert error is None
        assert otel.spans() == []
    finally:
        disconnect_signal_instrumentation()
        shared.shutdown_shared_pools()


async def test_unpatching_restores_the_classes(otel):
    import inspect

    from volnux.execution.context import ExecutionContext
    from volnux.execution.pipeline import Pipeline
    from volnux.flows.single import SingleFlow
    from volnux.otel.instrumentations._patching import unpatch_all

    originals = (Pipeline.start, ExecutionContext.dispatch, SingleFlow.run)
    patch_all_execution_components()
    patch_all_pipeline_components()
    patch_all_flow_components()
    assert (Pipeline.start, ExecutionContext.dispatch, SingleFlow.run) != originals
    assert inspect.iscoroutinefunction(Pipeline.start)  # the kind is kept
    disconnect_signal_instrumentation()
    unpatch_all()
    assert (Pipeline.start, ExecutionContext.dispatch, SingleFlow.run) == originals


async def test_cancel_async_still_takes_propagate(instrumented):
    # (the wrapper took only self: the coordinator's cancel_async(propagate=False)
    # raised TypeError)
    import inspect

    from volnux.execution.context import ExecutionContext

    assert "args" in str(inspect.signature(ExecutionContext.cancel_async)) or (
        "propagate" in inspect.signature(ExecutionContext.cancel_async).parameters
    )
