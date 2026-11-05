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
    otel.VolnuxObservability._meter_provider = None
    DummyVolnuxTracer.last_config = None
    yield
    otel.VolnuxObservability._initialized = False
    otel.VolnuxObservability._tracer_instance = None
    otel.VolnuxObservability._backends = []
    otel.VolnuxObservability._meter_provider = None


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


@pytest.fixture
def stubbed(monkeypatch):
    """initialize_otel with the tracer, the patching and the metrics replaced by
    recorders, so only its own logic is under test."""
    calls = []
    monkeypatch.setattr(otel, "logger", Mock())
    monkeypatch.setattr("volnux.otel.VolnuxTracerConfig", DummyTracerConfig)
    monkeypatch.setattr("volnux.otel.VolnuxTracer", DummyVolnuxTracer)
    for name in (
        "patch_all_execution_components",
        "patch_all_pipeline_components",
        "patch_all_flow_components",
        "patch_engine",
        "patch_framework_components",
        "install_trace_propagation",
    ):
        monkeypatch.setattr(f"volnux.otel.{name}", lambda name=name: calls.append(name))

    def setup_metrics(service_name, backends, config, service_version):
        calls.append(("metrics", service_name, backends, config, service_version))
        return True

    monkeypatch.setattr(otel, "_setup_metrics", setup_metrics)
    return calls


def test_initialize_otel_defaults_to_otlp_and_sets_framework_metadata(stubbed):
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
    assert not hasattr(cfg, "otlp_endpoint")  # no endpoint, nothing to export to
    assert cfg.custom_attributes["framework"] == "volnux"
    assert cfg.custom_attributes["instrumentation.version"] == volnux_version
    assert cfg.instrument_logging is True


def test_initialize_otel_patches_every_layer_and_installs_propagation(stubbed):
    otel.initialize_otel(service_name="svc")

    assert stubbed == [
        "install_trace_propagation",
        "patch_all_execution_components",
        "patch_all_pipeline_components",
        "patch_engine",
        "patch_framework_components",
        "patch_all_flow_components",
        ("metrics", "svc", ["otlp"], {}, volnux_version),
    ]


def test_the_patching_can_be_switched_off(stubbed):
    otel.initialize_otel(
        service_name="svc",
        patch_components=False,
        patch_flow_components=False,
        enable_metrics=False,
    )
    assert stubbed == ["install_trace_propagation"]


def test_logging_instrumentation_can_be_switched_off(stubbed):
    otel.initialize_otel(
        service_name="svc", enable_logging_instrumentation=False, enable_metrics=False
    )
    assert DummyVolnuxTracer.last_config.instrument_logging is False


def test_initialize_otel_maps_backends_to_config(stubbed):
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
    # (otlp and jaeger share one exporter: the first one configured is kept)
    assert cfg.otlp_endpoint == "http://collector:4317"


def test_a_backend_without_an_endpoint_does_not_erase_another_ones(stubbed):
    # (jaeger without an endpoint used to overwrite the otlp endpoint with None)
    otel.initialize_otel(
        service_name="svc",
        backends=["otlp", "jaeger", "datadog"],
        config={"otlp": {"endpoint": "http://collector:4317"}},
        enable_metrics=False,
        patch_components=False,
        patch_flow_components=False,
    )
    cfg = DummyVolnuxTracer.last_config
    assert cfg.otlp_endpoint == "http://collector:4317"
    assert not hasattr(cfg, "datadog_agent_url")


def test_grafana_uses_the_tempo_endpoint(stubbed):
    otel.initialize_otel(
        service_name="svc",
        backends=["grafana"],
        config={"grafana": {"endpoint": "http://grafana:4317"}},
        enable_metrics=False,
        patch_components=False,
        patch_flow_components=False,
    )
    assert DummyVolnuxTracer.last_config.tempo_endpoint == "http://grafana:4317"


def test_an_unknown_backend_is_refused_before_anything_is_set_up(stubbed):
    with pytest.raises(ValueError, match="Unknown observability backend 'zipkin'"):
        otel.initialize_otel(service_name="svc", backends=["otlp", "zipkin"])
    assert otel.VolnuxObservability.is_initialized() is False
    assert otel.VolnuxObservability.get_backends() == []
    assert stubbed == []


def test_backend_names_are_case_insensitive(stubbed):
    otel.initialize_otel(
        service_name="svc",
        backends=["OTLP", "Datadog"],
        enable_metrics=False,
        patch_components=False,
        patch_flow_components=False,
    )
    assert otel.VolnuxObservability.get_backends() == [
        otel.ObservabilityBackend.GENERIC_OTLP,
        otel.ObservabilityBackend.DATADOG,
    ]


def test_initialize_otel_deduplicates_backends(stubbed):
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


def test_the_sample_rate_is_checked_even_when_already_initialized(monkeypatch):
    monkeypatch.setattr(otel, "logger", Mock())
    otel.VolnuxObservability._initialized = True
    otel.VolnuxObservability._tracer_instance = DummyTracer()
    with pytest.raises(ValueError, match="sample_rate"):
        otel.initialize_otel(service_name="svc", sample_rate=2)


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


def test_shutdown_stops_the_instrumentation_before_the_tracer(monkeypatch):
    order = []
    monkeypatch.setattr(otel, "logger", Mock())
    monkeypatch.setattr(
        otel, "remove_trace_propagation", lambda: order.append("propagation")
    )
    monkeypatch.setattr(
        otel, "disconnect_signal_instrumentation", lambda: order.append("signals")
    )
    monkeypatch.setattr(otel, "unpatch_all", lambda: order.append("unpatch"))

    class Tracer:
        def shutdown(self):
            order.append("tracer")

    meter_provider = Mock()
    meter_provider.shutdown.side_effect = lambda: order.append("meters")
    otel.VolnuxObservability._initialized = True
    otel.VolnuxObservability._tracer_instance = Tracer()
    otel.VolnuxObservability._meter_provider = meter_provider

    otel.shutdown_otel()

    # nothing makes spans against a provider that is shutting down
    assert order == ["propagation", "signals", "unpatch", "tracer", "meters"]
    assert otel.VolnuxObservability._meter_provider is None


def test_shutdown_resets_the_metrics_state(monkeypatch):
    from volnux.otel.instrumentations.flow import FlowMetrics
    from volnux.otel.metrics import VolnuxMetricsCollector

    monkeypatch.setattr(otel, "logger", Mock())
    VolnuxMetricsCollector._instance = object()
    VolnuxMetricsCollector._initialized = True
    FlowMetrics._initialized = True
    otel.VolnuxObservability._initialized = True
    otel.VolnuxObservability._tracer_instance = DummyTracer()

    otel.shutdown_otel()

    assert VolnuxMetricsCollector.get_instance() is None
    assert FlowMetrics._initialized is False


def test_a_meter_provider_that_fails_to_shut_down_does_not_stop_the_rest(monkeypatch):
    logger_mock = Mock()
    monkeypatch.setattr(otel, "logger", logger_mock)
    meter_provider = Mock()
    meter_provider.shutdown.side_effect = RuntimeError("exporter down")
    otel.VolnuxObservability._initialized = True
    otel.VolnuxObservability._tracer_instance = DummyTracer()
    otel.VolnuxObservability._meter_provider = meter_provider

    otel.shutdown_otel()

    assert otel.VolnuxObservability.is_initialized() is False
    logger_mock.warning.assert_called()


def test_setup_metrics_skips_when_no_endpoint(monkeypatch):
    logger_mock = Mock()
    monkeypatch.setattr(otel, "logger", logger_mock)

    assert otel._setup_metrics("svc", ["otlp"], {}) is False

    logger_mock.warning.assert_called_once()


@pytest.fixture
def metrics_parts(monkeypatch):
    parts = SimpleNamespace(
        metrics=Mock(),
        provider=Mock(),
        exporter_endpoints=[],
        initialized=[],
        resets=[],
    )
    monkeypatch.setattr("volnux.otel.otel_metrics", parts.metrics)
    monkeypatch.setattr(
        "volnux.otel.OTLPMetricExporter",
        lambda endpoint: parts.exporter_endpoints.append(endpoint) or Mock(),
    )
    monkeypatch.setattr(
        "volnux.otel.PeriodicExportingMetricReader",
        lambda exporter, export_interval_millis: Mock(),
    )
    monkeypatch.setattr(
        "volnux.otel.MeterProvider", lambda resource, metric_readers: parts.provider
    )
    monkeypatch.setattr(
        "opentelemetry.sdk.resources.Resource.create",
        lambda attrs: SimpleNamespace(attrs=attrs),
    )
    monkeypatch.setattr(
        "volnux.otel.initialize_metrics",
        lambda name, version: parts.initialized.append((name, version)),
    )
    monkeypatch.setattr("volnux.otel.FlowMetrics.reset", lambda: parts.resets.append(1))
    return parts


def test_setup_metrics_configures_meter_provider(metrics_parts):
    assert otel._setup_metrics(
        "svc", ["otlp"], {"otlp": {"endpoint": "http://collector:4317"}}
    )
    metrics_parts.metrics.set_meter_provider.assert_called_once_with(
        metrics_parts.provider
    )
    assert otel.VolnuxObservability._meter_provider is metrics_parts.provider
    assert metrics_parts.exporter_endpoints == ["http://collector:4317"]


def test_setup_metrics_creates_the_instruments(metrics_parts):
    # (it only set the provider: get_metrics_collector() stayed None)
    otel._setup_metrics("svc", ["otlp"], {"otlp": {"endpoint": "http://c:4317"}}, "9.9")
    assert metrics_parts.initialized == [("svc", "9.9")]
    assert metrics_parts.resets == [1]  # flow metrics bind to the new provider


@pytest.mark.parametrize(
    "backends, config, expected",
    [
        (["datadog", "otlp"], {"otlp": {"endpoint": "http://o"}}, "http://o"),
        (["otlp", "datadog"], {"datadog": {"agent_url": "http://d"}}, "http://d"),
        (["jaeger"], {"jaeger": {"endpoint": "http://j"}}, "http://j"),
        (["tempo"], {"tempo": {"endpoint": "http://t"}}, "http://t"),
        (
            ["datadog", "otlp"],
            {"datadog": {"agent_url": "http://d"}, "otlp": {"endpoint": "http://o"}},
            "http://d",
        ),
    ],
)
def test_the_first_backend_that_has_an_endpoint_gets_the_metrics(
    metrics_parts, backends, config, expected
):
    # (the first recognised backend ended the search even without an endpoint)
    assert otel._setup_metrics("svc", backends, config)
    assert metrics_parts.exporter_endpoints == [expected]


def test_setup_metrics_never_raises(monkeypatch):
    monkeypatch.setattr(otel, "logger", Mock())

    def boom(endpoint):
        raise RuntimeError("bad endpoint")

    monkeypatch.setattr("volnux.otel.OTLPMetricExporter", boom)
    assert otel._setup_metrics("svc", ["otlp"], {"otlp": {"endpoint": "x"}}) is False


def test_module_exports_do_not_include_missing_helpers():
    # These helpers are not defined in volnux/otel/__init__.py right now.
    assert not hasattr(otel, "quick_setup")
    assert not hasattr(otel, "get_current_trace_id")
    assert not hasattr(otel, "get_current_span_id")
    assert not hasattr(otel, "add_span_attribute")
    assert not hasattr(otel, "add_span_event")
