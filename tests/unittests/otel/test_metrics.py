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


# --- levels, units, the new definitions ------------------------------------------
import pytest as _pytest  # noqa: E402


class TestLevels:
    """Queue lengths and worker counts are levels: the series must read the level,
    not the sum of every sample."""

    def test_recording_the_same_level_twice_does_not_double_it(self, otel):
        definition = metrics_mod.VolnuxMetricsRegistry.ENGINE_QUEUE_LENGTH
        otel.collector.record_level(definition, 5, {"pipeline.name": "p"})
        otel.collector.record_level(definition, 5, {"pipeline.name": "p"})
        assert otel.total("engine.queue_length") == 5

    def test_a_falling_level_goes_down(self, otel):
        definition = metrics_mod.VolnuxMetricsRegistry.ENGINE_QUEUE_LENGTH
        for value in (5, 9, 2):
            otel.collector.record_level(definition, value, {"pipeline.name": "p"})
        assert otel.total("engine.queue_length") == 2

    def test_each_series_has_its_own_level(self, otel):
        definition = metrics_mod.VolnuxMetricsRegistry.ENGINE_QUEUE_LENGTH
        otel.collector.record_level(definition, 4, {"pipeline.name": "a"})
        otel.collector.record_level(definition, 7, {"pipeline.name": "b"})
        assert otel.total("engine.queue_length", **{"pipeline.name": "a"}) == 4
        assert otel.total("engine.queue_length", **{"pipeline.name": "b"}) == 7

    def test_engine_metrics_are_levels(self, otel):
        snapshot = {
            "task_queue_length": 3,
            "target_workers": 8,
            "actual_workers": 4,
            "parallel_tasks": 2,
        }
        for _ in range(3):
            otel.collector.record_engine_metrics(snapshot, "pipe")
        assert otel.total("engine.queue_length") == 3
        assert otel.total("engine.target_workers") == 8
        assert otel.total("engine.actual_workers") == 4
        assert otel.total("engine.parallel_tasks") == 2

    def test_parallel_tasks_is_a_level_too(self, otel):
        otel.collector.record_parallel_tasks(6, "pipe")
        otel.collector.record_parallel_tasks(6, "pipe")
        assert otel.total("engine.parallel_tasks") == 6

    def test_memory_is_in_mb_whichever_way_it_is_reported(self, otel):
        # (record_engine_metrics reported GB into an instrument whose unit is MB)
        otel.collector.record_engine_metrics({"memory_usage_gb": 2}, "pipe")
        otel.collector.record_system_utilization(1.0, 512, "other")
        sums = {
            p["attributes"]["pipeline.name"]: p["sum"]
            for p in otel.points("resource.memory_usage")
        }
        assert sums == {"pipe": 2048.0, "other": 512.0}

    def test_a_level_of_zero_from_nothing_records_nothing(self, otel):
        otel.collector.record_level(
            metrics_mod.VolnuxMetricsRegistry.ENGINE_QUEUE_LENGTH, 0, {}
        )
        assert otel.points("engine.queue_length") == []


class TestRegistry:
    NEW = (
        ("TRIGGER_ACTIVATIONS", "trigger.activations", "counter"),
        ("WORKFLOW_RUN_DURATION", "workflow.run.duration", "histogram"),
        ("WORKFLOW_RUN_COUNT", "workflow.run.count", "counter"),
        ("REGISTRY_LOAD_DURATION", "registry.load.duration", "histogram"),
        ("CHECKPOINT_DURATION", "checkpoint.duration", "histogram"),
        ("CHECKPOINT_COUNT", "checkpoint.operations", "counter"),
        ("SAGA_DURATION", "saga.duration", "histogram"),
        ("SAGA_COUNT", "saga.executions", "counter"),
        ("MESSAGING_DURATION", "messaging.duration", "histogram"),
        ("MESSAGING_OPERATIONS", "messaging.operations", "counter"),
    )

    @_pytest.mark.parametrize("attribute, name, kind", NEW)
    def test_the_definitions(self, attribute, name, kind):
        definition = getattr(metrics_mod.VolnuxMetricsRegistry, attribute)
        assert definition.name == name and definition.metric_type.value == kind

    def test_names_are_unique(self):
        names = [
            d.name
            for d in vars(metrics_mod.VolnuxMetricsRegistry).values()
            if isinstance(d, metrics_mod.MetricDefinition)
        ]
        assert len(names) == len(set(names))

    def test_a_definition_is_created_for_every_entry(self, otel):
        names = {
            d.name
            for d in vars(metrics_mod.VolnuxMetricsRegistry).values()
            if isinstance(d, metrics_mod.MetricDefinition)
        }
        assert names <= set(otel.collector._instruments)

    def test_parallel_tasks_is_an_up_down_counter(self):
        assert (
            metrics_mod.VolnuxMetricsRegistry.ENGINE_PARALLEL_TASKS.metric_type
            is metrics_mod.MetricType.UP_DOWN_COUNTER
        )


class TestRecordMetric:
    def test_a_counter_adds_and_a_histogram_records(self, otel):
        otel.collector.increment_workflow_count("p", "success")
        otel.collector.record_workflow_duration(40.0, "p", "success")
        assert otel.total("workflow.executions", **{"pipeline.name": "p"}) == 1
        assert otel.points("workflow.duration")[0]["sum"] == 40.0

    def test_a_failing_instrument_is_swallowed(self, otel):
        class Broken:
            def add(self, *a, **k):
                raise RuntimeError("x")

        otel.collector._instruments["workflow.executions"] = Broken()
        otel.collector.increment_workflow_count("p", "success")  # no raise

    def test_an_unknown_instrument_does_nothing(self, otel):
        definition = metrics_mod.MetricDefinition(
            "no.such", "d", "1", metrics_mod.MetricType.COUNTER
        )
        otel.collector.record_metric(definition, 1)

    def test_initialize_creates_the_collector_once_under_concurrency(self, monkeypatch):
        import threading

        metrics_mod.VolnuxMetricsCollector.shutdown()
        created = []
        real = metrics_mod.VolnuxMetricsCollector._setup_instruments

        def counting(self):
            created.append(1)
            real(self)

        monkeypatch.setattr(
            metrics_mod.VolnuxMetricsCollector, "_setup_instruments", counting
        )
        threads = [
            threading.Thread(
                target=metrics_mod.VolnuxMetricsCollector.initialize, args=("svc",)
            )
            for _ in range(8)
        ]
        [t.start() for t in threads]
        [t.join() for t in threads]
        try:
            assert len(created) == 1
        finally:
            metrics_mod.VolnuxMetricsCollector.shutdown()
