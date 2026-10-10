import pytest

from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ALWAYS_ON, ParentBased

from volnux.otel.tracer_setup import VolnuxTracer, VolnuxTracerConfig, get_tracer


class DummySpanProcessor:
    def __init__(self, exporter):
        self.exporter = exporter


class DummyExporter:
    def __init__(self, endpoint, insecure=True):
        self.endpoint = endpoint
        self.insecure = insecure


class DummyProvider:
    def __init__(self, resource=None, sampler=None):
        self.resource = resource
        self.sampler = sampler
        self.span_processors = []
        self.shutdown_called = False

    def add_span_processor(self, processor):
        self.span_processors.append(processor)

    def shutdown(self):
        self.shutdown_called = True


class DummyTracer:
    pass


@pytest.fixture(autouse=True)
def reset_tracer_state(monkeypatch):
    monkeypatch.setattr(VolnuxTracer, "_instance", None)
    monkeypatch.setattr(VolnuxTracer, "_tracer", None)
    monkeypatch.setattr(VolnuxTracer, "_provider", None)
    monkeypatch.setattr(VolnuxTracer, "_logging_instrumented", False)
    monkeypatch.setattr(VolnuxTracer, "_global_provider_set", False)
    yield


def test_config_rejects_invalid_sample_rate():
    with pytest.raises(ValueError, match="sample_rate must be between 0.0 and 1.0"):
        VolnuxTracerConfig(sample_rate=1.5)

    with pytest.raises(ValueError, match="sample_rate must be between 0.0 and 1.0"):
        VolnuxTracerConfig(sample_rate=-0.1)


def test_config_accepts_valid_sample_rate():
    config = VolnuxTracerConfig(sample_rate=0.25)
    assert config.sample_rate == 0.25
    assert config.custom_attributes == {}


def test_build_sampler_returns_always_off(monkeypatch):
    monkeypatch.setattr(VolnuxTracer, "_setup_tracer", lambda self: None)
    tracer = VolnuxTracer(VolnuxTracerConfig(sample_rate=0.0))
    assert tracer._build_sampler() == ALWAYS_OFF


def test_build_sampler_returns_always_on(monkeypatch):
    monkeypatch.setattr(VolnuxTracer, "_setup_tracer", lambda self: None)
    tracer = VolnuxTracer(VolnuxTracerConfig(sample_rate=1.0))
    assert tracer._build_sampler() == ALWAYS_ON


def test_build_sampler_returns_parent_based_ratio(monkeypatch):
    monkeypatch.setattr(VolnuxTracer, "_setup_tracer", lambda self: None)
    tracer = VolnuxTracer(VolnuxTracerConfig(sample_rate=0.5))
    assert isinstance(tracer._build_sampler(), ParentBased)


def test_initialize_returns_singleton(monkeypatch):
    monkeypatch.setattr(VolnuxTracer, "_setup_tracer", lambda self: None)

    config = VolnuxTracerConfig(service_name="svc-a")
    tracer1 = VolnuxTracer.initialize(config)
    tracer2 = VolnuxTracer.initialize(VolnuxTracerConfig(service_name="svc-b"))

    assert tracer1 is tracer2
    assert tracer1.config.service_name == "svc-a"


def test_get_tracer_returns_none_when_not_initialized():
    assert get_tracer() is None


def test_get_tracer_returns_tracer_instance(monkeypatch):
    dummy = DummyTracer()
    instance = VolnuxTracer.__new__(VolnuxTracer)
    instance._tracer = dummy
    monkeypatch.setattr(VolnuxTracer, "_instance", instance)

    assert get_tracer() is dummy


def test_get_tracer_raises_when_internal_tracer_missing(monkeypatch):
    instance = VolnuxTracer.__new__(VolnuxTracer)
    instance._tracer = None
    monkeypatch.setattr(VolnuxTracer, "_instance", instance)

    with pytest.raises(RuntimeError, match="Tracer not initialized"):
        get_tracer()


def test_setup_tracer_creates_provider_and_sets_global_provider_once(monkeypatch):
    created_providers = []
    instrument_calls = []

    def fake_provider_factory(*args, **kwargs):
        provider = DummyProvider(*args, **kwargs)
        created_providers.append(provider)
        return provider

    def fake_set_tracer_provider(provider):
        instrument_calls.append(("set_provider", provider))

    def fake_get_tracer(*args, **kwargs):
        return DummyTracer()

    def fake_instrument(self, set_logging_format=True):
        instrument_calls.append(("logging", set_logging_format))

    monkeypatch.setattr(
        "volnux.otel.tracer_setup.TracerProvider", fake_provider_factory
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.trace.set_tracer_provider", fake_set_tracer_provider
    )
    monkeypatch.setattr("volnux.otel.tracer_setup.trace.get_tracer", fake_get_tracer)
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.LoggingInstrumentor.instrument", fake_instrument
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.OTLPSpanExporter",
        lambda endpoint, insecure=True: DummyExporter(endpoint, insecure),
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.BatchSpanProcessor",
        lambda exporter: DummySpanProcessor(exporter),
    )

    tracer = VolnuxTracer(
        VolnuxTracerConfig(
            service_name="my-service",
            service_version="2.3.4",
            environment="testing",
            otlp_endpoint="http://localhost:4317",
            custom_attributes={"team": "core"},
            sample_rate=0.5,
        )
    )

    assert tracer._provider is created_providers[0]
    assert tracer._provider.resource is not None
    assert tracer._provider.sampler is not None
    assert tracer._tracer is not None

    assert ("set_provider", tracer._provider) in instrument_calls
    assert ("logging", True) in instrument_calls


def test_setup_exporters_adds_unique_endpoints_only(monkeypatch):
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.trace.set_tracer_provider", lambda provider: None
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.trace.get_tracer",
        lambda *args, **kwargs: DummyTracer(),
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.LoggingInstrumentor.instrument",
        lambda self, set_logging_format=True: None,
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.OTLPSpanExporter",
        lambda endpoint, insecure=True: DummyExporter(endpoint, insecure),
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.BatchSpanProcessor",
        lambda exporter: DummySpanProcessor(exporter),
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.TracerProvider",
        lambda *args, **kwargs: DummyProvider(*args, **kwargs),
    )

    tracer = VolnuxTracer(
        VolnuxTracerConfig(
            otlp_endpoint="http://collector:4317",
            datadog_agent_url="http://collector:4317",
            tempo_endpoint="http://tempo:4317",
        )
    )

    processors = tracer._provider.span_processors
    assert len(processors) == 2
    assert processors[0].exporter.endpoint == "http://collector:4317"
    assert processors[1].exporter.endpoint == "http://tempo:4317"


def test_setup_exporters_warns_when_no_endpoints(monkeypatch):
    warnings = []

    monkeypatch.setattr(
        "volnux.otel.tracer_setup.trace.set_tracer_provider", lambda provider: None
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.trace.get_tracer",
        lambda *args, **kwargs: DummyTracer(),
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.LoggingInstrumentor.instrument",
        lambda self, set_logging_format=True: None,
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.logger.warning",
        lambda msg, *args, **kwargs: warnings.append(msg),
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.OTLPSpanExporter",
        lambda endpoint, insecure=True: DummyExporter(endpoint, insecure),
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.BatchSpanProcessor",
        lambda exporter: DummySpanProcessor(exporter),
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.TracerProvider",
        lambda *args, **kwargs: DummyProvider(*args, **kwargs),
    )

    tracer = VolnuxTracer(
        VolnuxTracerConfig(
            otlp_endpoint=None, datadog_agent_url=None, tempo_endpoint=None
        )
    )

    assert tracer._provider.span_processors == []
    assert any("No trace exporter configured" in warning for warning in warnings)


def test_shutdown_calls_provider_shutdown(monkeypatch):
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.trace.set_tracer_provider", lambda provider: None
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.trace.get_tracer",
        lambda *args, **kwargs: DummyTracer(),
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.LoggingInstrumentor.instrument",
        lambda self, set_logging_format=True: None,
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.OTLPSpanExporter",
        lambda endpoint, insecure=True: DummyExporter(endpoint, insecure),
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.BatchSpanProcessor",
        lambda exporter: DummySpanProcessor(exporter),
    )
    monkeypatch.setattr(
        "volnux.otel.tracer_setup.TracerProvider",
        lambda *args, **kwargs: DummyProvider(*args, **kwargs),
    )

    tracer = VolnuxTracer(VolnuxTracerConfig(otlp_endpoint="http://collector:4317"))
    provider = tracer._provider

    tracer.shutdown()

    assert provider.shutdown_called is True
