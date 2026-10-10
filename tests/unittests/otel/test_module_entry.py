import importlib
from types import SimpleNamespace
from unittest.mock import Mock, MagicMock

import pytest

import volnux.otel as otel
from volnux import __version__ as volnux_version


class DummyTracer:
    def __init__(self):
        self.shutdown_called = False

    def shutdown(self):
        self.shutdown_called = True


class DummyTracerConfig:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class DummyVolnuxTracer:
    last_config = None
    instance = DummyTracer()

    @classmethod
    def initialize(cls, config):
        cls.last_config = config
        return cls.instance


@pytest.fixture(autouse=True)
def reset_observability_state():
    otel.VolnuxObservability._initialized = False
    otel.VolnuxObservability._tracer_instance = None
    otel.VolnuxObservability._backends = []
    yield
    otel.VolnuxObservability._initialized = False
    otel.VolnuxObservability._tracer_instance = None
    otel.VolnuxObservability._backends = []


def test_is_initialized_false_by_default():
    assert otel.VolnuxObservability.is_initialized() is False


def test_get_backends_returns_copy():
    otel.VolnuxObservability._backends = [otel.ObservabilityBackend.DATADOG]
    backends = otel.VolnuxObservability.get_backends()

    assert backends == [otel.ObservabilityBackend.DATADOG]
    backends.append(otel.ObservabilityBackend.JAEGER)
    assert otel.VolnuxObservability.get_backends() == [
        otel.ObservabilityBackend.DATADOG
    ]


def test_initialize_otel_defaults_to_otlp_and_sets_framework_metadata(monkeypatch):
    monkeypatch.setattr(otel, "logger", Mock())
    monkeypatch.setattr("volnux.otel.VolnuxTracerConfig", DummyTracerConfig)
    monkeypatch.setattr("volnux.otel.VolnuxTracer", DummyVolnuxTracer)

    patched_components = []
    patched_flows = []
    metrics_calls = []

    monkeypatch.setattr(
        "volnux.otel.patch_all_execution_components",
        lambda: patched_components.append("execution"),
    )
    monkeypatch.setattr(
        "volnux.otel.patch_all_pipeline_components",
        lambda: patched_components.append("pipeline"),
    )
    monkeypatch.setattr(
        "volnux.otel.patch_all_flow_components",
        lambda: patched_flows.append("flow"),
    )
    monkeypatch.setattr(
        otel,
        "_setup_metrics",
        lambda service_name, backends, config, service_version: metrics_calls.append(
            (service_name, backends, config, service_version)
        ),
    )

    tracer = otel.initialize_otel(service_name="svc")

    assert tracer is DummyVolnuxTracer.instance
    assert otel.VolnuxObservability.is_initialized() is True
    assert otel.VolnuxObservability.get_tracer_instance() is DummyVolnuxTracer.instance
    assert otel.VolnuxObservability.get_backends() == [
        otel.ObservabilityBackend.GENERIC_OTLP
    ]

    cfg = DummyVolnuxTracer.last_config
    assert cfg.service_name == "svc"
    assert cfg.sample_rate == 1.0
    assert cfg.otlp_endpoint is None
    assert cfg.custom_attributes["framework"] == "volnux"
    assert cfg.custom_attributes["instrumentation.version"] == volnux_version

    assert patched_components == ["execution", "pipeline"]
    assert patched_flows == ["flow"]
    assert metrics_calls == [("svc", ["otlp"], {}, volnux_version)]


def test_initialize_otel_maps_backends_to_config(monkeypatch):
    monkeypatch.setattr(otel, "logger", Mock())
    monkeypatch.setattr("volnux.otel.VolnuxTracerConfig", DummyTracerConfig)
    monkeypatch.setattr("volnux.otel.VolnuxTracer", DummyVolnuxTracer)
    monkeypatch.setattr(otel, "_setup_metrics", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "volnux.otel.patch_all_execution_components",
        lambda: None,
    )
    monkeypatch.setattr(
        "volnux.otel.patch_all_pipeline_components",
        lambda: None,
    )
    monkeypatch.setattr(
        "volnux.otel.patch_all_flow_components",
        lambda: None,
    )

    tracer = otel.initialize_otel(
        service_name="svc",
        backends=["datadog", "tempo", "otlp", "jaeger"],
        config={
            "datadog": {"agent_url": "http://datadog:4317"},
            "tempo": {"endpoint": "http://tempo:4317"},
            "otlp": {"endpoint": "http://collector:4317"},
            "jaeger": {"endpoint": "http://jaeger:4317"},
        },
        enable_metrics=False,
        patch_components=False,
        patch_flow_components=False,
    )

    assert tracer is DummyVolnuxTracer.instance
    assert otel.VolnuxObservability.get_backends() == [
        otel.ObservabilityBackend.DATADOG,
        otel.ObservabilityBackend.TEMPO,
        otel.ObservabilityBackend.GENERIC_OTLP,
        otel.ObservabilityBackend.JAEGER,
    ]

    cfg = DummyVolnuxTracer.last_config
    assert cfg.datadog_agent_url == "http://datadog:4317"
    assert cfg.tempo_endpoint == "http://tempo:4317"
    assert cfg.otlp_endpoint == "http://jaeger:4317"


def test_initialize_otel_deduplicates_backends(monkeypatch):
    monkeypatch.setattr(otel, "logger", Mock())
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.VolnuxTracerConfig", DummyTracerConfig
    )
    monkeypatch.setattr("volnux.otel.tracer_setup.VolnuxTracer", DummyVolnuxTracer)
    monkeypatch.setattr(otel, "_setup_metrics", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "volnux.otel.instrumentations.context_coordinator.patch_all_execution_components",
        lambda: None,
    )
    monkeypatch.setattr(
        "volnux.otel.instrumentations.signal.patch_all_pipeline_components",
        lambda: None,
    )
    monkeypatch.setattr(
        "volnux.otel.instrumentations.flow.patch_all_flow_components",
        lambda: None,
    )

    otel.initialize_otel(
        service_name="svc",
        backends=["otlp", "otlp", "datadog", "datadog"],
        config={"otlp": {"endpoint": "http://collector:4317"}},
        enable_metrics=False,
        patch_components=False,
        patch_flow_components=False,
    )

    assert otel.VolnuxObservability.get_backends() == [
        otel.ObservabilityBackend.GENERIC_OTLP,
        otel.ObservabilityBackend.DATADOG,
    ]


def test_initialize_otel_validates_sample_rate(monkeypatch):
    monkeypatch.setattr(otel, "logger", Mock())

    with pytest.raises(ValueError, match="sample_rate must be between 0.0 and 1.0"):
        otel.initialize_otel(service_name="svc", sample_rate=1.5, enable_metrics=False)

    with pytest.raises(ValueError, match="sample_rate must be between 0.0 and 1.0"):
        otel.initialize_otel(
            service_name="svc", sample_rate=-0.01, enable_metrics=False
        )


def test_initialize_otel_returns_existing_tracer_when_already_initialized(monkeypatch):
    monkeypatch.setattr(otel, "logger", Mock())
    existing = DummyTracer()
    otel.VolnuxObservability._initialized = True
    otel.VolnuxObservability._tracer_instance = existing

    tracer = otel.initialize_otel(service_name="svc", enable_metrics=False)

    assert tracer is existing


def test_initialize_otel_raises_when_initialized_but_missing_tracer(monkeypatch):
    monkeypatch.setattr(otel, "logger", Mock())
    otel.VolnuxObservability._initialized = True
    otel.VolnuxObservability._tracer_instance = None

    with pytest.raises(RuntimeError, match="tracer instance is missing"):
        otel.initialize_otel(service_name="svc", enable_metrics=False)


def test_shutdown_otel_calls_tracer_shutdown(monkeypatch):
    monkeypatch.setattr(otel, "logger", Mock())
    tracer = DummyTracer()
    otel.VolnuxObservability._initialized = True
    otel.VolnuxObservability._tracer_instance = tracer
    otel.VolnuxObservability._backends = [otel.ObservabilityBackend.DATADOG]

    otel.shutdown_otel()

    assert tracer.shutdown_called is True
    assert otel.VolnuxObservability.is_initialized() is False
    assert otel.VolnuxObservability.get_tracer_instance() is None
    assert otel.VolnuxObservability.get_backends() == []


def test_shutdown_otel_noops_when_not_initialized(monkeypatch):
    logger_mock = Mock()
    monkeypatch.setattr(otel, "logger", logger_mock)

    otel.shutdown_otel()

    logger_mock.warning.assert_called_once()


def test_setup_metrics_skips_when_no_endpoint(monkeypatch):
    logger_mock = Mock()
    monkeypatch.setattr(otel, "logger", logger_mock)

    otel._setup_metrics("svc", ["otlp"], {})

    logger_mock.warning.assert_called_once()


def test_setup_metrics_configures_meter_provider(monkeypatch):
    metrics_module = Mock()
    fake_meter_provider = Mock()
    fake_reader = Mock()
    fake_exporter = Mock()

    metrics_module.set_meter_provider = MagicMock()

    monkeypatch.setattr(
        "volnux.otel.metrics", metrics_module
    )
    monkeypatch.setattr(
        "volnux.otel.OTLPMetricExporter",
        lambda endpoint: fake_exporter,
    )
    monkeypatch.setattr(
        "volnux.otel.PeriodicExportingMetricReader",
        lambda exporter, export_interval_millis: fake_reader,
    )
    monkeypatch.setattr(
        "volnux.otel.MeterProvider",
        lambda resource, metric_readers: fake_meter_provider,
    )
    monkeypatch.setattr(
        "opentelemetry.sdk.resources.Resource.create",
        lambda attrs: SimpleNamespace(attrs=attrs),
    )

    otel._setup_metrics(
        "svc",
        ["otlp"],
        {"otlp": {"endpoint": "http://collector:4317"}},
    )

    metrics_module.set_meter_provider.assert_called_once_with(fake_meter_provider)


def test_module_exports_do_not_include_missing_helpers():
    # These helpers are not defined in volnux/otel/__init__.py right now.
    assert not hasattr(otel, "quick_setup")
    assert not hasattr(otel, "get_current_trace_id")
    assert not hasattr(otel, "get_current_span_id")
    assert not hasattr(otel, "add_span_attribute")
    assert not hasattr(otel, "add_span_event")
