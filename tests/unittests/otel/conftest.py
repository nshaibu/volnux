"""Fixtures for the OpenTelemetry tests: real spans and metrics, in memory.

``otel`` installs a tracer whose spans go to an ``InMemorySpanExporter`` and a meter
provider that a ``InMemoryMetricReader`` reads, then removes them (and every patch
made through ``patch_attr``) after the test, so no test sees another's state.
"""

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from volnux import context as volnux_context_module
from volnux.otel import VolnuxObservability
from volnux.otel.instrumentations import _patching
from volnux.otel.instrumentations.flow import FlowMetrics
from volnux.otel.metrics import VolnuxMetricsCollector
from volnux.otel.tracer_setup import VolnuxTracer


class _TracerHolder:
    """What ``VolnuxTracer.get_instance()`` returns."""

    def __init__(self, tracer):
        self._tracer = tracer

    def get_tracer(self):
        return self._tracer


class OTel(SimpleNamespace):
    """The test's telemetry: ``spans()``, ``span(name)``, ``points(metric)``."""

    def spans(self) -> List[Any]:
        return list(self.exporter.get_finished_spans())

    def names(self) -> List[str]:
        return [span.name for span in self.spans()]

    def span(self, name: str):
        found = [span for span in self.spans() if span.name == name]
        assert found, f"no span {name!r}; there are {self.names()}"
        assert len(found) == 1, f"{len(found)} spans named {name!r}"
        return found[0]

    def spans_named(self, name: str) -> List[Any]:
        return [span for span in self.spans() if span.name == name]

    def points(self, metric: str) -> List[Dict[str, Any]]:
        """The data points of ``metric``: ``{"attributes", "value" | "count"+"sum"}``."""
        data = self.reader.get_metrics_data()
        found = []
        for resource_metrics in data.resource_metrics if data else ():
            for scope in resource_metrics.scope_metrics:
                for item in scope.metrics:
                    if item.name != metric:
                        continue
                    for point in item.data.data_points:
                        entry = {"attributes": dict(point.attributes)}
                        if hasattr(point, "value"):
                            entry["value"] = point.value
                        else:
                            entry["count"] = point.count
                            entry["sum"] = point.sum
                        found.append(entry)
        return found

    def total(self, metric: str, **attributes: str) -> float:
        """The sum of ``metric`` over the points that match ``attributes``."""
        total = 0.0
        for point in self.points(metric):
            if all(point["attributes"].get(k) == v for k, v in attributes.items()):
                total += point.get("value", point.get("count", 0))
        return total


@pytest.fixture
def otel(monkeypatch):
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("volnux-test")

    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])

    previous_instance = VolnuxTracer._instance
    VolnuxTracer._instance = _TracerHolder(tracer)

    # metrics: the flow metrics read the global provider; the collector gets a meter
    monkeypatch.setattr(
        "volnux.otel.instrumentations.flow.metrics.get_meter_provider",
        lambda: meter_provider,
    )
    FlowMetrics.reset()
    collector = VolnuxMetricsCollector(meter_provider.get_meter("volnux-test"))
    collector._setup_instruments()
    previous_collector = (
        VolnuxMetricsCollector._instance,
        VolnuxMetricsCollector._initialized,
    )
    VolnuxMetricsCollector._instance = collector
    VolnuxMetricsCollector._initialized = True

    yield OTel(
        exporter=exporter,
        provider=provider,
        tracer=tracer,
        reader=reader,
        meter_provider=meter_provider,
        collector=collector,
    )

    _patching.unpatch_all()
    FlowMetrics.reset()
    VolnuxTracer._instance = previous_instance
    (
        VolnuxMetricsCollector._instance,
        VolnuxMetricsCollector._initialized,
    ) = previous_collector
    volnux_context_module.register_trace_propagation(None)
    exporter.clear()
    provider.shutdown()
    meter_provider.shutdown()


@pytest.fixture
def no_otel(monkeypatch):
    """Telemetry as it is before ``initialize_otel``: no tracer, no collector."""
    previous = VolnuxTracer._instance
    VolnuxTracer._instance = None
    previous_collector = (
        VolnuxMetricsCollector._instance,
        VolnuxMetricsCollector._initialized,
    )
    VolnuxMetricsCollector._instance = None
    VolnuxMetricsCollector._initialized = False
    yield
    VolnuxTracer._instance = previous
    (
        VolnuxMetricsCollector._instance,
        VolnuxMetricsCollector._initialized,
    ) = previous_collector


@pytest.fixture(autouse=True)
def clean_observability_state():
    """No test starts or ends with observability marked initialised, or patched."""
    VolnuxObservability._initialized = False
    VolnuxObservability._tracer_instance = None
    VolnuxObservability._backends = []
    VolnuxObservability._meter_provider = None
    yield
    _patching.unpatch_all()
    VolnuxObservability._initialized = False
    VolnuxObservability._tracer_instance = None
    VolnuxObservability._backends = []
    VolnuxObservability._meter_provider = None
