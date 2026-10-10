import pytest

import volnux.otel.metrics as metrics_mod


class DummyCounter:
    def __init__(self):
        self.calls = []

    def add(self, value, attributes=None):
        self.calls.append(("add", value, attributes or {}))


class DummyHistogram:
    def __init__(self):
        self.calls = []

    def record(self, value, attributes=None):
        self.calls.append(("record", value, attributes or {}))


class DummyMeter:
    def __init__(self):
        self.created = []

    def create_counter(self, name, description, unit):
        instrument = DummyCounter()
        self.created.append(("counter", name, description, unit, instrument))
        return instrument

    def create_histogram(self, name, description, unit):
        instrument = DummyHistogram()
        self.created.append(("histogram", name, description, unit, instrument))
        return instrument

    def create_up_down_counter(self, name, description, unit):
        instrument = DummyCounter()
        self.created.append(("up_down_counter", name, description, unit, instrument))
        return instrument


@pytest.fixture(autouse=True)
def reset_metrics_collector():
    metrics_mod.VolnuxMetricsCollector.shutdown()
    yield
    metrics_mod.VolnuxMetricsCollector.shutdown()


def test_initialize_returns_singleton(monkeypatch):
    dummy_meter = DummyMeter()
    monkeypatch.setattr(
        metrics_mod.metrics, "get_meter", lambda name, version=None: dummy_meter
    )

    collector = metrics_mod.VolnuxMetricsCollector.initialize(
        service_name="svc",
        service_version="1.2.3",
    )

    assert collector is metrics_mod.VolnuxMetricsCollector.get_instance()
    assert metrics_mod.VolnuxMetricsCollector._initialized is True
    assert metrics_mod.VolnuxMetricsCollector._meter is dummy_meter


def test_initialize_returns_existing_instance_when_already_initialized(monkeypatch):
    dummy_meter = DummyMeter()
    monkeypatch.setattr(
        metrics_mod.metrics, "get_meter", lambda name, version=None: dummy_meter
    )

    first = metrics_mod.VolnuxMetricsCollector.initialize("svc", "1.0.0")
    second = metrics_mod.VolnuxMetricsCollector.initialize("svc", "1.0.0")

    assert first is second
    assert metrics_mod.VolnuxMetricsCollector.get_instance() is first


def test_setup_instruments_creates_expected_metric_instruments(monkeypatch):
    dummy_meter = DummyMeter()
    collector = metrics_mod.VolnuxMetricsCollector(dummy_meter)

    collector._setup_instruments()

    created_names = {entry[1] for entry in dummy_meter.created}

    assert metrics_mod.VolnuxMetricsRegistry.WORKFLOW_COUNT.name in created_names
    assert metrics_mod.VolnuxMetricsRegistry.WORKFLOW_DURATION.name in created_names
    assert metrics_mod.VolnuxMetricsRegistry.TASK_QUEUE_SIZE.name in created_names
    assert metrics_mod.VolnuxMetricsRegistry.ENGINE_QUEUE_LENGTH.name in created_names
    assert metrics_mod.VolnuxMetricsRegistry.CPU_USAGE.name in created_names
    assert metrics_mod.VolnuxMetricsRegistry.MEMORY_USAGE.name in created_names


def test_setup_instruments_no_meter_logs_warning(monkeypatch):
    warnings = []
    monkeypatch.setattr(
        metrics_mod.logger, "warning", lambda msg, *args, **kwargs: warnings.append(msg)
    )

    collector = metrics_mod.VolnuxMetricsCollector(None)
    collector._setup_instruments()

    assert any("No meter available" in msg for msg in warnings)


def test_record_metric_adds_to_counter(monkeypatch):
    dummy_meter = DummyMeter()
    collector = metrics_mod.VolnuxMetricsCollector(dummy_meter)
    collector._setup_instruments()

    collector.record_metric(
        metrics_mod.VolnuxMetricsRegistry.WORKFLOW_COUNT,
        1,
        attributes={"pipeline.name": "p1"},
    )

    instrument = collector._instruments[
        metrics_mod.VolnuxMetricsRegistry.WORKFLOW_COUNT.name
    ]
    assert instrument.calls == [("add", 1, {"pipeline.name": "p1"})]


def test_record_metric_adds_to_histogram(monkeypatch):
    dummy_meter = DummyMeter()
    collector = metrics_mod.VolnuxMetricsCollector(dummy_meter)
    collector._setup_instruments()

    collector.record_metric(
        metrics_mod.VolnuxMetricsRegistry.WORKFLOW_DURATION,
        123.4,
        attributes={"pipeline.name": "p1"},
    )

    instrument = collector._instruments[
        metrics_mod.VolnuxMetricsRegistry.WORKFLOW_DURATION.name
    ]
    assert instrument.calls == [("record", 123.4, {"pipeline.name": "p1"})]


def test_record_metric_skips_missing_instrument(monkeypatch):
    debug_calls = []
    monkeypatch.setattr(
        metrics_mod.logger,
        "debug",
        lambda msg, *args, **kwargs: debug_calls.append(msg),
    )

    collector = metrics_mod.VolnuxMetricsCollector(None)
    collector.record_metric(
        metrics_mod.MetricDefinition(
            name="missing.metric",
            description="missing",
            unit="1",
            metric_type=metrics_mod.MetricType.COUNTER,
        ),
        1,
    )

    assert any("Instrument not found" in msg for msg in debug_calls)


def test_record_engine_metrics_records_snapshot(monkeypatch):
    dummy_meter = DummyMeter()
    collector = metrics_mod.VolnuxMetricsCollector(dummy_meter)
    collector._setup_instruments()

    snapshot = {
        "task_queue_length": 7,
        "target_workers": 3,
        "actual_workers": 2,
        "parallel_tasks": 4,
        "cpu_usage_cores": 1.5,
        "memory_usage_gb": 2.25,
    }

    collector.record_engine_metrics(snapshot, pipeline_name="pipeline-a")

    assert collector._instruments[
        metrics_mod.VolnuxMetricsRegistry.ENGINE_QUEUE_LENGTH.name
    ].calls[-1] == (
        "add",
        7.0,
        {"pipeline.name": "pipeline-a"},
    )
    assert collector._instruments[
        metrics_mod.VolnuxMetricsRegistry.ENGINE_TARGET_WORKERS.name
    ].calls[-1] == (
        "add",
        3.0,
        {"pipeline.name": "pipeline-a"},
    )
    assert collector._instruments[
        metrics_mod.VolnuxMetricsRegistry.CPU_USAGE.name
    ].calls[-1] == (
        "record",
        1.5,
        {"pipeline.name": "pipeline-a"},
    )


def test_record_system_utilization_records_cpu_and_memory(monkeypatch):
    dummy_meter = DummyMeter()
    collector = metrics_mod.VolnuxMetricsCollector(dummy_meter)
    collector._setup_instruments()

    collector.record_system_utilization(
        cpu_usage_cores=1.25,
        memory_usage_mb=512.0,
        pipeline_name="pipeline-a",
    )

    assert collector._instruments[
        metrics_mod.VolnuxMetricsRegistry.CPU_USAGE.name
    ].calls[-1] == (
        "record",
        1.25,
        {"pipeline.name": "pipeline-a"},
    )
    assert collector._instruments[
        metrics_mod.VolnuxMetricsRegistry.MEMORY_USAGE.name
    ].calls[-1] == (
        "record",
        512.0,
        {"pipeline.name": "pipeline-a"},
    )


def test_shutdown_resets_state(monkeypatch):
    dummy_meter = DummyMeter()
    monkeypatch.setattr(
        metrics_mod.metrics, "get_meter", lambda name, version=None: dummy_meter
    )

    metrics_mod.VolnuxMetricsCollector.initialize("svc", "1.0.0")
    assert metrics_mod.VolnuxMetricsCollector.get_instance() is not None

    metrics_mod.VolnuxMetricsCollector.shutdown()

    assert metrics_mod.VolnuxMetricsCollector.get_instance() is None
    assert metrics_mod.VolnuxMetricsCollector._meter is None
    assert metrics_mod.VolnuxMetricsCollector._instruments == {}
    assert metrics_mod.VolnuxMetricsCollector._initialized is False


def test_initialize_metrics_delegates_to_collector(monkeypatch):
    called = {}

    def fake_initialize(service_name, service_version="1.0.0"):
        called["service_name"] = service_name
        called["service_version"] = service_version
        return "collector"

    monkeypatch.setattr(
        metrics_mod.VolnuxMetricsCollector,
        "initialize",
        classmethod(
            lambda cls, service_name, service_version="1.0.0": fake_initialize(
                service_name, service_version
            )
        ),
    )

    result = metrics_mod.initialize_metrics("svc", service_version="2.0.0")

    assert result == "collector"
    assert called == {"service_name": "svc", "service_version": "2.0.0"}
