"""setup_otel: OpenTelemetry from the OTEL_* settings, for the servers."""

import logging

import pytest

from volnux.otel import bootstrap


@pytest.fixture
def settings(monkeypatch):
    """Control what ``setting()`` returns; record what initialize_otel gets."""
    values = {"OTEL_ENABLED": True}
    calls = {"init": [], "shutdown": 0}

    def setting(name, default=None):
        return values.get(name, default)

    monkeypatch.setattr(bootstrap, "setting", setting)

    def initialize_otel(service_name, **kwargs):
        calls["init"].append((service_name, kwargs))

    def shutdown_otel():
        calls["shutdown"] += 1

    monkeypatch.setattr("volnux.otel.initialize_otel", initialize_otel)
    monkeypatch.setattr("volnux.otel.shutdown_otel", shutdown_otel)
    monkeypatch.setattr(
        bootstrap, "_instrument_fastapi", lambda: calls.setdefault("fastapi", True)
    )
    return values, calls


def test_disabled_by_default(monkeypatch):
    monkeypatch.setattr(bootstrap, "setting", lambda name, default=None: default)
    shutdown = bootstrap.setup_otel()
    assert shutdown() is None


def test_disabled_does_not_initialise(settings):
    values, calls = settings
    values["OTEL_ENABLED"] = False
    bootstrap.setup_otel()()
    assert calls["init"] == []


def test_the_default_backend_is_grafana_on_the_default_endpoint(settings):
    values, calls = settings
    bootstrap.setup_otel(project_name="proj", version="1.2.3")
    ((service, kwargs),) = calls["init"]
    assert service == "proj"
    assert kwargs["service_version"] == "1.2.3"
    assert kwargs["backends"] == ["grafana"]
    assert kwargs["config"] == {"grafana": {"endpoint": "http://localhost:4317"}}


@pytest.mark.parametrize(
    "backend, extra, expected_backend, expected_config",
    [
        (
            "datadog",
            {"OTEL_DATADOG_URL": "http://dd:4317"},
            ["datadog"],
            {"datadog": {"agent_url": "http://dd:4317"}},
        ),
        (
            "datadog",
            {},
            ["datadog"],
            {"datadog": {"agent_url": "http://e:4317"}},
        ),
        (
            "grafana",
            {"OTEL_TEMPO_URL": "http://tempo:4317"},
            ["grafana"],
            {"grafana": {"endpoint": "http://tempo:4317"}},
        ),
        ("tempo", {}, ["tempo"], {"tempo": {"endpoint": "http://e:4317"}}),
        ("jaeger", {}, ["jaeger"], {"jaeger": {"endpoint": "http://e:4317"}}),
        ("otlp", {}, ["otlp"], {"otlp": {"endpoint": "http://e:4317"}}),
        ("anything-else", {}, ["otlp"], {"otlp": {"endpoint": "http://e:4317"}}),
        ("DATADOG", {}, ["datadog"], {"datadog": {"agent_url": "http://e:4317"}}),
    ],
)
def test_the_backend_setting_picks_the_backend_and_its_endpoint(
    settings, backend, extra, expected_backend, expected_config
):
    values, calls = settings
    values.update({"OTEL_BACKEND": backend, "OTEL_ENDPOINT": "http://e:4317", **extra})
    bootstrap.setup_otel()
    ((_, kwargs),) = calls["init"]
    assert kwargs["backends"] == expected_backend
    assert kwargs["config"] == expected_config


def test_the_service_name_setting_wins_over_the_project_name(settings):
    values, calls = settings
    values["OTEL_SERVICE_NAME"] = "from-setting"
    bootstrap.setup_otel(project_name="proj")
    assert calls["init"][0][0] == "from-setting"


@pytest.mark.parametrize(
    "dev_mode, debug, environment",
    [
        (False, False, "production"),
        (True, False, "development"),
        (False, True, "development"),
    ],
)
def test_the_environment(settings, dev_mode, debug, environment):
    values, calls = settings
    values["DEBUG"] = debug
    bootstrap.setup_otel(dev_mode=dev_mode)
    assert calls["init"][0][1]["environment"] == environment


def test_the_role_and_node_are_attributes(settings):
    values, calls = settings
    values["NODE_ID"] = "node-7"
    bootstrap.setup_otel(role="worker")
    attrs = calls["init"][0][1]["custom_attributes"]
    # (the worker used to be labelled "api")
    assert attrs == {"volnux.node.id": "node-7", "volnux.role": "worker"}


def test_the_sample_rate_setting(settings):
    values, calls = settings
    values["OTEL_SAMPLE_RATE"] = "0.25"
    bootstrap.setup_otel()
    assert calls["init"][0][1]["sample_rate"] == 0.25


def test_fastapi_is_instrumented(settings):
    values, calls = settings
    bootstrap.setup_otel()
    assert calls["fastapi"] is True


def test_the_shutdown_function_shuts_otel_down(settings):
    values, calls = settings
    shutdown = bootstrap.setup_otel()
    shutdown()
    assert calls["shutdown"] == 1


def test_a_failing_shutdown_is_swallowed(settings, monkeypatch):
    values, calls = settings
    shutdown = bootstrap.setup_otel()

    def broken():
        raise RuntimeError("exporter down")

    monkeypatch.setattr("volnux.otel.shutdown_otel", broken)
    shutdown()  # no raise


def test_a_failing_initialisation_never_stops_the_server(settings, monkeypatch, caplog):
    values, calls = settings

    def broken(service_name, **kwargs):
        raise RuntimeError("bad config")

    monkeypatch.setattr("volnux.otel.initialize_otel", broken)
    with caplog.at_level(logging.WARNING):
        shutdown = bootstrap.setup_otel()
    assert shutdown() is None
    assert "OTel setup failed" in caplog.text


def test_a_missing_otel_install_disables_tracing(settings, monkeypatch, caplog):
    import builtins

    real_import = builtins.__import__

    def refuse(name, *args, **kwargs):
        if name == "volnux.otel":
            raise ImportError("no opentelemetry")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", refuse)
    with caplog.at_level(logging.WARNING):
        shutdown = bootstrap.setup_otel()
    monkeypatch.setattr(builtins, "__import__", real_import)
    assert shutdown() is None
    assert "tracing disabled" in caplog.text


class TestServerHelpers:
    """Both servers' _setup_otel are one thin call (they were two long copies)."""

    @pytest.mark.parametrize("module", ["volnux.adapters.lifecycle.utils"])
    def test_they_delegate(self, monkeypatch, module):
        import importlib
        from types import SimpleNamespace

        mod = importlib.import_module(module)
        seen = {}
        monkeypatch.setattr(
            "volnux.otel.bootstrap.setup_otel",
            lambda **kwargs: seen.update(kwargs) or (lambda: None),
        )
        engine = SimpleNamespace(project_name="p", version="9")
        shutdown = mod._setup_otel(engine, dev_mode=True, role="worker")
        assert shutdown() is None
        assert seen == {
            "project_name": "p",
            "version": "9",
            "dev_mode": True,
            "role": "worker",
        }


class TestIdempotentSetup:
    @pytest.fixture
    def running(self, settings, monkeypatch):
        """Telemetry already running (started by initialise_workflows)."""
        import volnux.otel as otel_module

        monkeypatch.setattr(otel_module.VolnuxObservability, "_initialized", True)
        return settings

    def test_nothing_is_initialised_again(self, running):
        values, calls = running
        shutdown = bootstrap.setup_otel(role="worker")
        assert calls["init"] == []
        assert "fastapi" not in calls  # nor instrumented again
        shutdown()
        assert calls["shutdown"] == 1

    def test_the_shutdown_it_returns_works_when_it_did_not_start_it(self, running):
        values, calls = running
        bootstrap.setup_otel()()
        bootstrap.setup_otel()()
        assert calls["shutdown"] == 2

    def test_disabled_still_wins(self, running):
        values, calls = running
        values["OTEL_ENABLED"] = False
        bootstrap.setup_otel()()
        assert calls["shutdown"] == 0
