"""initialise_workflows: the entry point that wires a project, and its telemetry."""

import logging
import sys
import time
from unittest.mock import Mock

import pytest
from opentelemetry.trace import StatusCode

import volnux.engine.workflows.registry as registry_module
import volnux.observability as observability
import volnux.setup as setup
from volnux.conf import override_settings
from volnux.config import VolnuxConfig
from volnux.engine.workflows.registry import WorkflowRegistry
from volnux.engine.workflows.trigger import get_trigger_engine
from volnux.engine.workflows.trigger.executors.asyncio_executor import (
    AsyncTaskWorkflowExecutor,
)
from volnux.engine.workflows.source import RegistrySource
from tests.unittests.otel.conftest import no_otel, otel  # noqa: F401  (fixtures)
from volnux.otel import VolnuxObservability

pytestmark = pytest.mark.asyncio


class Source:
    def __init__(self, name, error=None):
        self.name = name
        self.source_type = RegistrySource.LOCAL
        self.location = name
        self._error = error

    async def load_workflow_config(self, registry, params):
        if self._error:
            raise self._error
        return Mock(error=False, content="")


@pytest.fixture(autouse=True)
def telemetry_state_is_put_back():
    VolnuxObservability._initialized = False
    VolnuxObservability._tracer_instance = None
    yield
    VolnuxObservability._initialized = False
    VolnuxObservability._tracer_instance = None


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A project directory with its own registry and no running trigger engine; the
    project's logging configuration would disable other tests' loggers."""
    registry = WorkflowRegistry(cache_dir=tmp_path / "cache")
    monkeypatch.setattr(registry_module, "_workflow_registry", registry)
    engine = get_trigger_engine()
    monkeypatch.setattr(engine, "workflow_executor", None)
    monkeypatch.setattr(engine, "_running", False)
    monkeypatch.setattr("logging.config.dictConfig", lambda *a, **k: None)
    monkeypatch.setattr(
        "volnux.reporting.ensure_pipeline_reporters_installed", lambda *a, **k: None
    )
    (tmp_path / "workflows").mkdir()
    return SimpleProject(tmp_path, registry, engine)


class SimpleProject:
    def __init__(self, path, registry, engine):
        self.path, self.registry, self.engine = path, registry, engine

    def add_workflow(self, name):
        (self.path / "workflows" / name).mkdir()


@pytest.fixture
def telemetry(otel, monkeypatch):
    """Telemetry already running (in memory): what initialise_workflows finds after
    a project started it itself, or after the real start."""
    VolnuxObservability._initialized = True
    VolnuxObservability._tracer_instance = object()
    return otel


# ----------------------------------------------------------------- the wiring
class TestWithARealRegistry:
    """(The tests used a mock registry that had the attribute the real one lacked:
    initialise_workflows raised AttributeError for every project.)"""

    async def test_an_empty_project(self, project):
        engine = await setup.initialise_workflows(project.path)
        assert engine is project.engine
        assert project.registry.is_ready()
        assert project.registry.workflow_count == 0
        assert isinstance(engine.workflow_executor, AsyncTaskWorkflowExecutor)
        assert engine.get_workflow_registry() is project.registry

    async def test_sources_are_discovered_and_loaded(self, project, monkeypatch):
        loaded = []

        async def load(self, registry, options=None):
            loaded.append(self.name)
            return Mock(error=False, content="")

        monkeypatch.setattr(
            "volnux.engine.workflows.source.WorkflowSource.load_workflow_config", load
        )
        project.add_workflow("alpha")
        project.add_workflow("beta")
        await setup.initialise_workflows(project.path)
        assert sorted(loaded) == ["alpha", "beta"]

    async def test_one_workflow_can_be_asked_for(self, project, monkeypatch):
        loaded = []

        async def load(self, registry, options=None):
            loaded.append(self.name)
            return Mock(error=False, content="")

        monkeypatch.setattr(
            "volnux.engine.workflows.source.WorkflowSource.load_workflow_config", load
        )
        project.add_workflow("alpha")
        project.add_workflow("beta")
        await setup.initialise_workflows(project.path, "beta")
        assert loaded == ["beta"]

    async def test_a_registry_that_is_ready_is_not_loaded_again(self, project):
        project.registry.make_ready()
        project.registry._workflow_local_sources["x"] = Source("x")
        await setup.initialise_workflows(project.path)
        assert project.registry.workflow_count == 0  # nothing was loaded

    async def test_an_executor_set_beforehand_is_kept(self, project):
        mine = AsyncTaskWorkflowExecutor(project.registry)
        project.engine.workflow_executor = mine
        await setup.initialise_workflows(project.path)
        assert project.engine.workflow_executor is mine

    async def test_the_executor_setting(self, project):
        from volnux.engine.workflows.trigger.executors.celery import (
            CeleryWorkflowExecutor,
        )

        path = "volnux.engine.workflows.trigger.executors.asyncio_executor.AsyncTaskWorkflowExecutor"
        with override_settings(DEFAULT_WORKFLOW_EXECUTOR=path):
            await setup.initialise_workflows(project.path)
        assert type(project.engine.workflow_executor) is AsyncTaskWorkflowExecutor

    async def test_a_setting_that_is_not_an_executor(self, project):
        with override_settings(DEFAULT_WORKFLOW_EXECUTOR="builtins.dict"):
            with pytest.raises(TypeError, match="WorkflowExecutor"):
                await setup.initialise_workflows(project.path)

    async def test_a_path_that_does_not_exist(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            await setup.initialise_workflows(tmp_path / "missing")

    async def test_a_running_trigger_engine_is_stopped_first(
        self, project, monkeypatch
    ):
        stopped = []

        async def stop():
            stopped.append(1)

        monkeypatch.setattr(project.engine, "_running", True)
        monkeypatch.setattr(project.engine, "stop", stop)
        await setup.initialise_workflows(project.path)
        assert stopped == [1]

    async def test_an_engine_that_cannot_be_stopped(self, project, monkeypatch):
        async def stop():
            raise OSError("stuck")

        monkeypatch.setattr(project.engine, "_running", True)
        monkeypatch.setattr(project.engine, "stop", stop)
        with pytest.raises(RuntimeError, match="Cannot reinitialize safely"):
            await setup.initialise_workflows(project.path)

    async def test_init_py_is_loaded_before_telemetry_starts(
        self, project, monkeypatch
    ):
        order = []
        (project.path / "init.py").write_text("# project init\n")

        async def load_from_file(self, path):
            order.append(("config", path.name))

        monkeypatch.setattr(VolnuxConfig, "load_from_file_async", load_from_file)
        monkeypatch.setattr(
            observability,
            "start_tracing_for_setup",
            lambda **kwargs: order.append(("telemetry", kwargs["role"])),
        )
        await setup.initialise_workflows(project.path)
        # (the OTEL_* settings may come from init.py)
        assert order == [("config", "init.py"), ("telemetry", "api")]


# ---------------------------------------------------------------- telemetry off
class TestTelemetryOff:
    async def test_nothing_of_otel_is_imported_or_recorded(self, project):
        with override_settings(OTEL_ENABLED=False):
            assert observability.start_tracing_for_setup() is False
            await setup.initialise_workflows(project.path)
        assert VolnuxObservability.is_initialized() is False

    async def test_the_phase_helpers_are_no_ops(self, project):
        with observability.setup_phase("anything", a=1) as span:
            assert span is None
        observability.record_setup_duration(0.1, "success")  # no raise
        observability.shutdown_observability()  # nothing to shut down

    async def test_setup_does_not_import_opentelemetry_when_off(self):
        import subprocess

        code = (
            "import sys; import volnux.setup, volnux.observability;"
            "from volnux.observability import start_tracing_for_setup;"
            "assert start_tracing_for_setup() is False;"
            "print('volnux.otel' in sys.modules)"
        )
        out = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        assert out.returncode == 0, out.stderr
        assert out.stdout.strip() == "False"


# ----------------------------------------------------------------- telemetry on
class TestTelemetryOn:
    async def test_the_root_span_and_the_phases(self, project, telemetry, monkeypatch):
        otel = telemetry
        project.add_workflow("alpha")

        async def load(self, registry, options=None):
            return Mock(error=False, content="")

        monkeypatch.setattr(
            "volnux.engine.workflows.source.WorkflowSource.load_workflow_config", load
        )
        with override_settings(OTEL_ENABLED=True, ROLE="worker"):
            await setup.initialise_workflows(project.path, "alpha")

        root = otel.span("setup.initialise_workflows")
        assert root.parent is None
        assert root.attributes["project.path"] == str(project.path)
        assert root.attributes["workflow.filter"] == "alpha"
        assert root.attributes["role"] == "worker"
        assert root.attributes["setup.config_load_ms"] >= 0
        assert root.attributes["setup.workflows"] == project.registry.workflow_count

        for phase in (
            "configure_reporters",
            "discover_workflows",
            "load_workflows",
            "configure_executor",
        ):
            span = otel.span(f"setup.{phase}")
            assert span.parent.span_id == root.context.span_id, phase
        assert otel.span("setup.configure_executor").attributes["executor.class"] == (
            "AsyncTaskWorkflowExecutor"
        )
        assert "setup.sources" in otel.span("setup.discover_workflows").attributes
        assert otel.span("setup.load_workflows").attributes["setup.failed_sources"] == 0

    async def test_the_registry_load_nests_under_its_phase(self, project, telemetry):
        otel = telemetry
        from volnux.otel.instrumentations import framework

        framework.patch_all()
        with override_settings(OTEL_ENABLED=True):
            await setup.initialise_workflows(project.path)
        load = otel.span("registry.load")
        assert load.parent.span_id == otel.span("setup.load_workflows").context.span_id

    async def test_one_trace(self, project, telemetry):
        otel = telemetry
        with override_settings(OTEL_ENABLED=True):
            await setup.initialise_workflows(project.path)
        assert len({s.context.trace_id for s in otel.spans()}) == 1

    async def test_a_failed_source_is_counted(self, project, telemetry):
        otel = telemetry
        project.registry._workflow_local_sources["bad"] = Source(
            "bad", error=OSError("disk")
        )
        with override_settings(OTEL_ENABLED=True):
            await setup.initialise_workflows(project.path)
        assert otel.span("setup.load_workflows").attributes["setup.failed_sources"] == 1

    async def test_the_duration_metric(self, project, telemetry):
        otel = telemetry
        with override_settings(OTEL_ENABLED=True):
            await setup.initialise_workflows(project.path)
        (point,) = otel.points("setup.duration")
        assert point["attributes"] == {"outcome": "success"} and point["count"] == 1

    async def test_a_failure_is_recorded_and_the_same_exception_raised(
        self, project, telemetry
    ):
        otel = telemetry
        with override_settings(
            OTEL_ENABLED=True, DEFAULT_WORKFLOW_EXECUTOR="builtins.dict"
        ):
            with pytest.raises(TypeError, match="WorkflowExecutor"):
                await setup.initialise_workflows(project.path)

        root = otel.span("setup.initialise_workflows")
        assert root.status.status_code is StatusCode.ERROR
        assert (
            otel.span("setup.configure_executor").status.status_code is StatusCode.ERROR
        )
        assert otel.total("setup.duration") == 1
        assert otel.points("setup.duration")[0]["attributes"] == {"outcome": "error"}

    async def test_the_duration_covers_the_configuration_too(self, project, telemetry):
        otel = telemetry
        with override_settings(OTEL_ENABLED=True):
            await setup.initialise_workflows(project.path)
        root = otel.span("setup.initialise_workflows")
        (point,) = otel.points("setup.duration")
        span_ms = (root.end_time - root.start_time) / 1e6
        assert point["sum"] >= span_ms  # (the config step is before the span)

    async def test_running_it_twice_traces_each_time(self, project, telemetry):
        otel = telemetry
        with override_settings(OTEL_ENABLED=True):
            await setup.initialise_workflows(project.path)
            project.registry.make_ready()
            await setup.initialise_workflows(project.path)
        assert len(otel.spans_named("setup.initialise_workflows")) == 2
        # the second time the registry was already loaded: no discovery phase
        assert len(otel.spans_named("setup.discover_workflows")) == 1


# ------------------------------------------------------------- starting it
class TestStartTracing:
    @pytest.fixture
    def started(self, monkeypatch):
        calls = []

        def setup_otel(**kwargs):
            calls.append(kwargs)
            VolnuxObservability._initialized = True
            VolnuxObservability._tracer_instance = object()

        monkeypatch.setattr("volnux.otel.bootstrap.setup_otel", setup_otel)
        return calls

    async def test_it_starts_telemetry_once(self, started):
        import volnux.otel  # noqa: F401  (so that it is "running" once started)

        with override_settings(OTEL_ENABLED=True):
            assert (
                observability.start_tracing_for_setup("proj", "1.0", "worker") is True
            )
            assert (
                observability.start_tracing_for_setup("proj", "1.0", "worker") is True
            )
        assert started == [
            {
                "project_name": "proj",
                "version": "1.0",
                "dev_mode": False,
                "role": "worker",
            }
        ]

    async def test_the_role_defaults_to_the_setting(self, started):
        with override_settings(OTEL_ENABLED=True, ROLE="worker"):
            observability.start_tracing_for_setup()
        assert started[0]["role"] == "worker"

    async def test_debug_means_development(self, started):
        with override_settings(OTEL_ENABLED=True, DEBUG=True):
            observability.start_tracing_for_setup()
        assert started[0]["dev_mode"] is True

    async def test_disabled(self, started):
        with override_settings(OTEL_ENABLED=False):
            assert observability.start_tracing_for_setup() is False
        assert started == []

    async def test_a_telemetry_that_fails_to_start_does_not_stop_setup(
        self, project, monkeypatch, caplog
    ):
        def broken(**kwargs):
            raise RuntimeError("collector config is bad")

        monkeypatch.setattr("volnux.otel.bootstrap.setup_otel", broken)
        with override_settings(OTEL_ENABLED=True):
            with caplog.at_level(logging.WARNING):
                engine = await setup.initialise_workflows(project.path)
        assert engine is project.engine
        assert "Could not start telemetry" in caplog.text

    async def test_a_missing_install_does_not_stop_setup(
        self, project, monkeypatch, caplog
    ):
        def missing(**kwargs):
            raise ImportError("No module named 'opentelemetry'")

        monkeypatch.setattr("volnux.otel.bootstrap.setup_otel", missing)
        with override_settings(OTEL_ENABLED=True):
            with caplog.at_level(logging.WARNING):
                engine = await setup.initialise_workflows(project.path)
        assert engine is project.engine
        assert "volnux[otel]" in caplog.text

    async def test_the_project_name_and_version_come_from_the_settings(
        self, project, monkeypatch
    ):
        seen = {}
        monkeypatch.setattr(
            observability,
            "start_tracing_for_setup",
            lambda **kwargs: seen.update(kwargs),
        )
        with override_settings(PROJECT_NAME="billing", VERSION="3.2.1"):
            await setup.initialise_workflows(project.path)
        assert seen == {"project_name": "billing", "version": "3.2.1", "role": "api"}

    async def test_the_project_name_defaults_to_the_directory(
        self, project, monkeypatch
    ):
        seen = {}
        monkeypatch.setattr(
            observability, "start_tracing_for_setup", lambda **kw: seen.update(kw)
        )
        await setup.initialise_workflows(project.path)
        assert seen["project_name"] == project.path.name


class TestShutdown:
    async def test_shutdown_observability_shuts_telemetry_down(
        self, telemetry, monkeypatch
    ):
        called = []
        monkeypatch.setattr("volnux.otel.shutdown_otel", lambda: called.append(1))
        setup.shutdown_observability()
        assert called == [1]

    async def test_nothing_to_shut_down(self, monkeypatch):
        called = []
        monkeypatch.setattr("volnux.otel.shutdown_otel", lambda: called.append(1))
        setup.shutdown_observability()
        assert called == []

    async def test_a_failing_shutdown_is_swallowed(self, telemetry, monkeypatch):
        def broken():
            raise RuntimeError("exporter down")

        monkeypatch.setattr("volnux.otel.shutdown_otel", broken)
        setup.shutdown_observability()

    async def test_exports(self):
        assert set(setup.__all__) == {
            "initialise_workflows",
            "get_project_init_module",
            "shutdown_observability",
        }
