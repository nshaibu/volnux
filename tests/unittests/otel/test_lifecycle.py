"""initialize_otel -> run -> shutdown_otel -> initialize_otel again, for real."""

import asyncio
import inspect

import pytest
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

import volnux.otel as otel
from tests.integration.support import Trace, make_event, run, step
from volnux.executors import shared
from volnux.execution.context import ExecutionContext
from volnux.execution.pipeline import Pipeline
from volnux.otel.instrumentations import _patching
from volnux.otel.tracer_setup import VolnuxTracer, get_tracer

pytestmark = pytest.mark.asyncio


class _NoLogging:
    def instrument(self, **kwargs):
        pass


@pytest.fixture
def real_otel(monkeypatch):
    """The real initialize_otel, without the process-wide side effects (the global
    tracer provider, the logging format)."""
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.trace.set_tracer_provider", lambda p: None
    )
    monkeypatch.setattr("volnux.otel.tracer_setup.LoggingInstrumentor", _NoLogging)
    VolnuxTracer.reset()
    exporters = []
    original = VolnuxTracer._setup_exporters

    def with_memory_exporter(self):
        exporter = InMemorySpanExporter()
        self._provider.add_span_processor(SimpleSpanProcessor(exporter))
        exporters.append(exporter)

    monkeypatch.setattr(VolnuxTracer, "_setup_exporters", with_memory_exporter)
    yield exporters
    if otel.VolnuxObservability.is_initialized():
        otel.shutdown_otel()
    VolnuxTracer.reset()
    shared.shutdown_shared_pools()


async def test_initialize_run_shutdown(real_otel):
    originals = (Pipeline.start, ExecutionContext.dispatch)
    tracer = otel.initialize_otel("lifecycle", enable_metrics=False)
    assert otel.VolnuxObservability.is_initialized()
    assert get_tracer() is tracer.get_tracer()
    assert (Pipeline.start, ExecutionContext.dispatch) != originals

    a = make_event("A", step(Trace(), "a"))
    pipeline, error = await run(a.__name__)
    assert error is None
    names = {s.name for s in real_otel[0].get_finished_spans()}
    assert {"engine.execute", "context.dispatch", "flow.run.SingleFlow"} <= names
    assert any(n.startswith("workflow.") for n in names)

    otel.shutdown_otel()
    # nothing is traced after the shutdown, and the classes are as they were
    assert get_tracer() is None
    assert (Pipeline.start, ExecutionContext.dispatch) == originals
    assert inspect.iscoroutinefunction(Pipeline.start)
    assert _patching.patched_count() == 0


async def test_a_run_after_shutdown_records_nothing(real_otel):
    otel.initialize_otel("lifecycle", enable_metrics=False)
    otel.shutdown_otel()
    before = len(real_otel[0].get_finished_spans())
    a = make_event("A", step(Trace(), "a"))
    pipeline, error = await run(a.__name__)
    assert error is None
    assert len(real_otel[0].get_finished_spans()) == before


async def test_initialising_again_after_a_shutdown_uses_the_new_configuration(
    real_otel,
):
    # (the stale, shut-down tracer was returned, with its old configuration)
    first = otel.initialize_otel("first", sample_rate=1.0, enable_metrics=False)
    assert first.config.service_name == "first"
    otel.shutdown_otel()

    second = otel.initialize_otel("second", sample_rate=0.5, enable_metrics=False)
    assert second is not first
    assert second.config.service_name == "second" and second.config.sample_rate == 0.5
    assert VolnuxTracer.get_instance() is second

    a = make_event("A", step(Trace(), "a"))
    await run(a.__name__)
    # the new tracer works (sampling at 0.5 may drop it, so check it is the live one)
    assert get_tracer() is second.get_tracer()


async def test_initialising_twice_while_running_is_a_no_op(real_otel):
    first = otel.initialize_otel("one", enable_metrics=False)
    again = otel.initialize_otel("two", enable_metrics=False)
    assert again is first and first.config.service_name == "one"
    assert _patching.patched_count() > 0
    count = _patching.patched_count()
    otel.initialize_otel("three", enable_metrics=False)
    assert _patching.patched_count() == count  # nothing wrapped twice


async def test_the_framework_components_are_patched_too(real_otel):
    from volnux.engine.workflows.trigger.engine import TriggerEngine
    from volnux.otel.instrumentations._patching import is_patched

    otel.initialize_otel("lifecycle", enable_metrics=False)
    assert is_patched(TriggerEngine, "_handle_activation")
    otel.shutdown_otel()
    assert not is_patched(TriggerEngine, "_handle_activation")


async def test_the_trace_propagation_is_installed_and_removed(real_otel):
    from volnux import context

    otel.initialize_otel("lifecycle", enable_metrics=False)
    assert context._trace_propagation is not None
    otel.shutdown_otel()
    assert context._trace_propagation is None


async def test_the_metrics_collector_is_created(real_otel, monkeypatch):
    from volnux.otel.metrics import get_metrics_collector

    created = []
    monkeypatch.setattr(
        "volnux.otel.OTLPMetricExporter",
        lambda endpoint: created.append(endpoint) or _Exporter(),
    )
    monkeypatch.setattr("volnux.otel.otel_metrics.set_meter_provider", lambda p: None)
    otel.initialize_otel(
        "lifecycle", backends=["otlp"], config={"otlp": {"endpoint": "http://c:4317"}}
    )
    assert created == ["http://c:4317"]
    assert get_metrics_collector() is not None
    otel.shutdown_otel()
    assert get_metrics_collector() is None


class _Exporter:
    """A metric exporter that exports nothing."""

    from opentelemetry.sdk.metrics.export import AggregationTemporality

    _preferred_temporality = {}
    _preferred_aggregation = {}

    def export(self, *a, **k):
        return None

    def force_flush(self, *a, **k):
        return True

    def shutdown(self, *a, **k):
        return None
