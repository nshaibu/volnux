"""`volnux serve` / `volnux dev`: the commands, the servers they start, and how a
project is brought up for them."""

import asyncio
import inspect
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import volnux.engine.workflows.registry as registry_module
from volnux.cli.command.base import get_command_registry
from volnux.cli.command.builtins import serve as serve_module
from volnux.cli.command.builtins.serve import ServeCommand
from volnux.engine.workflows.registry import WorkflowRegistry
from volnux.engine.workflows.trigger import get_trigger_engine
from volnux.engine.workflows.trigger.triggers.base import TriggerType
from volnux.exceptions import CommandError


# ------------------------------------------------------- the commands exist
class TestCommandsAreRegistered:
    @pytest.mark.parametrize("name", ["serve", "dev", "init"])
    def test_the_command_is_known_to_the_cli(self, name):
        # (builtins/__init__ never imported them: "Unknown command: 'serve'")
        assert get_command_registry().get_by_name(name) is not None

    def test_serve_is_the_serve_command(self):
        assert get_command_registry().get_by_name("serve") is ServeCommand


class TestServerModule:
    def test_the_cli_imports_resolve(self):
        # (volnux.server did not exist: the import fell back to a stub that did nothing)
        from volnux.server import start_api_server, start_worker_server

        assert inspect.iscoroutinefunction(start_api_server)
        assert inspect.iscoroutinefunction(start_worker_server)

    @pytest.mark.parametrize("module", ["api_server", "worker_server", "utils"])
    def test_the_lifecycle_modules_have_no_undefined_names(self, module):
        import ast
        import builtins
        import importlib

        mod = importlib.import_module(f"volnux.adapters.lifecycle.{module}")
        tree = ast.parse(inspect.getsource(mod))
        defined = set(dir(builtins)) | set(vars(mod))
        loaded = {
            node.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
        }
        # names bound locally (parameters, assignments, comprehension targets...)
        local = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and isinstance(
                node.ctx, (ast.Store, ast.Del)
            ):
                local.add(node.id)
            elif isinstance(node, ast.arg):
                local.add(node.arg)
            elif isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
            ):
                local.add(node.name)
            elif isinstance(node, ast.ExceptHandler) and node.name:
                local.add(node.name)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    local.add((alias.asname or alias.name).split(".")[0])
        assert not (loaded - defined - local)


# --------------------------------------------------- bringing a project up
TEMPLATE_INIT = """\
from pathlib import Path
from asgiref import sync
from volnux.setup import initialise_workflows

project_dir = Path(__file__).parent

engine = sync.async_to_sync(initialise_workflows)(project_dir)
"""


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A project as `volnux init` writes it, with a registry of its own."""
    import volnux.setup as setup

    (tmp_path / "workflows").mkdir()
    (tmp_path / "init.py").write_text(TEMPLATE_INIT)
    monkeypatch.setattr(
        registry_module,
        "_workflow_registry",
        WorkflowRegistry(cache_dir=tmp_path / "c"),
    )
    engine = get_trigger_engine()
    monkeypatch.setattr(engine, "workflow_executor", None)
    monkeypatch.setattr(engine, "_running", False)
    monkeypatch.setattr("logging.config.dictConfig", lambda *a, **k: None)
    monkeypatch.setattr(setup, "_loading_init_files", set())
    return tmp_path


class TestTheProjectsInitFile:
    @pytest.mark.asyncio
    async def test_initialise_workflows_survives_an_init_py_that_calls_it(
        self, project
    ):
        """init.py is run to read its configuration, and the one `volnux init` writes
        calls initialise_workflows: that used to recurse until the process ran out of
        threads ("can't start new thread")."""
        from volnux.setup import initialise_workflows

        engine = await asyncio.wait_for(initialise_workflows(project), 30)
        assert engine is get_trigger_engine()
        assert engine.has_workflow_executor()

    @pytest.mark.asyncio
    async def test_the_guard_is_released_afterwards(self, project):
        import volnux.setup as setup

        await setup.initialise_workflows(project)
        assert setup._loading_init_files == set()

    @pytest.mark.asyncio
    async def test_a_failing_init_py_releases_the_guard_too(self, project):
        import volnux.setup as setup

        (project / "init.py").write_text("raise RuntimeError('bad init')\n")
        with pytest.raises(Exception):
            await setup.initialise_workflows(project)
        assert setup._loading_init_files == set()

    @pytest.mark.asyncio
    async def test_the_cli_mixin_loads_it_outside_the_event_loop(self, project):
        """init.py's async_to_sync refuses to run in a thread that has an event loop:
        every command that used the engine of init.py failed with that."""
        from volnux.cli.command.mixins.project import ProjectMixin

        mixin = ProjectMixin()
        engine = await asyncio.wait_for(mixin._initialise_workflows(project), 30)
        assert engine is get_trigger_engine()

    def test_the_sync_cli_wrapper(self, project):
        from volnux.cli.command.mixins.project import ProjectMixin

        engine = ProjectMixin().initialise_workflows(project)
        assert engine is get_trigger_engine()

    def test_an_init_py_without_an_engine_is_a_clear_error(self, project):
        from volnux.cli.command.mixins.project import ProjectMixin

        (project / "init.py").write_text("x = 1\n")
        with pytest.raises(CommandError, match="does not define an 'engine'"):
            ProjectMixin().initialise_workflows(project)


COUNTING_INIT = """\
import builtins
from pathlib import Path
from asgiref import sync
from volnux.setup import initialise_workflows

builtins._init_py_runs = getattr(builtins, "_init_py_runs", 0) + 1
PROJECT_NAME = "named-by-init"

project_dir = Path(__file__).parent
engine = sync.async_to_sync(initialise_workflows)(project_dir)
builtins._init_py_saw_executor = engine.has_workflow_executor()
"""


@pytest.fixture
def counted(project, monkeypatch):
    """The project's init.py counts its own runs."""
    import builtins

    monkeypatch.setattr(builtins, "_init_py_runs", 0, raising=False)
    monkeypatch.setattr(builtins, "_init_py_saw_executor", None, raising=False)
    # (init.py's settings must not stay in the process-wide configuration)
    import volnux.config as config_module

    monkeypatch.setattr(
        config_module.VolnuxConfig, "_instance", config_module.VolnuxConfig()
    )
    (project / "init.py").write_text(COUNTING_INIT)
    return project


class TestInitPyRunsOnce:
    @pytest.mark.asyncio
    async def test_initialise_workflows_runs_it_once(self, counted):
        import builtins

        from volnux.setup import initialise_workflows

        await initialise_workflows(counted)
        assert builtins._init_py_runs == 1

    def test_the_cli_runs_it_once(self, counted):
        import builtins

        from volnux.cli.command.mixins.project import ProjectMixin

        engine = ProjectMixin().initialise_workflows(counted)
        assert builtins._init_py_runs == 1
        assert engine is get_trigger_engine() and engine.has_workflow_executor()

    def test_the_cli_with_a_workflow_filter_runs_it_once(self, counted):
        import builtins

        from volnux.cli.command.mixins.project import ProjectMixin

        ProjectMixin().initialise_workflows(counted, "nothing-by-this-name")
        assert builtins._init_py_runs == 1

    @pytest.mark.asyncio
    async def test_the_call_inside_init_py_gets_the_engine_unwired(self, counted):
        """It is wired once, by the call that loads init.py, afterwards."""
        import builtins

        from volnux.setup import initialise_workflows

        engine = await initialise_workflows(counted)
        assert builtins._init_py_saw_executor is False  # not wired at that point
        assert engine.has_workflow_executor()  # and wired by the outer call

    @pytest.mark.asyncio
    async def test_the_project_is_wired_once(self, counted, monkeypatch):
        from volnux.engine.workflows.trigger.engine import TriggerEngine
        from volnux.setup import initialise_workflows

        wired = []
        original = TriggerEngine.set_workflow_executor
        monkeypatch.setattr(
            TriggerEngine,
            "set_workflow_executor",
            lambda self, e: wired.append(e) or original(self, e),
        )
        await initialise_workflows(counted)
        assert len(wired) == 1

    @pytest.mark.asyncio
    async def test_the_settings_in_init_py_are_in_place_before_the_wiring(
        self, counted, monkeypatch
    ):
        # (the nested call used to wire the project before its settings applied)
        import volnux.observability as observability
        from volnux.setup import initialise_workflows

        seen = {}
        monkeypatch.setattr(
            observability, "start_tracing_for_setup", lambda **kw: seen.update(kw)
        )
        await initialise_workflows(counted)
        assert seen["project_name"] == "named-by-init"

    def test_the_engine_is_read_from_that_run(self, counted):
        from volnux.cli.command.mixins.project import ProjectMixin
        from volnux.setup import get_project_init_module

        engine = ProjectMixin().initialise_workflows(counted)
        module = get_project_init_module(counted)
        assert module is not None and module.engine is engine

    def test_a_project_without_an_init_py(self, project):
        from volnux.cli.command.mixins.project import ProjectMixin
        from volnux.setup import get_project_init_module

        (project / "init.py").unlink()
        engine = ProjectMixin().initialise_workflows(project)
        assert engine is get_trigger_engine()
        assert get_project_init_module(project) is None

    @pytest.mark.asyncio
    async def test_running_it_again_runs_init_py_again(self, counted):
        """A second initialise_workflows (a reload) reads the file afresh."""
        import builtins

        from volnux.setup import initialise_workflows

        await initialise_workflows(counted)
        await initialise_workflows(counted)
        assert builtins._init_py_runs == 2

    @pytest.mark.asyncio
    async def test_a_different_project_called_from_init_py_is_not_nested(
        self, project, tmp_path_factory, monkeypatch
    ):
        # only the same init.py is a nested call
        from volnux.setup import initialise_workflows

        other = tmp_path_factory.mktemp("other")
        (other / "workflows").mkdir()
        (project / "init.py").write_text(
            "import asyncio\n"
            "from asgiref import sync\n"
            "from volnux.setup import initialise_workflows\n"
            f"sync.async_to_sync(initialise_workflows)(__import__('pathlib').Path({str(other)!r}))\n"
        )
        await initialise_workflows(project)  # no error, no recursion


# ------------------------------------------------------ the API server runs
class FakeServer:
    """Stands in for uvicorn.Server."""

    def __init__(self, behaviour):
        self.behaviour = behaviour
        self.started = False
        self.should_exit = False

    async def serve(self):
        if self.behaviour == "fails_to_start":
            return  # uvicorn returns after a startup failure, never started
        if self.behaviour == "crashes":
            raise OSError("address already in use")
        self.started = True
        while not self.should_exit:
            await asyncio.sleep(0.01)


def fake_engine():
    engine = SimpleNamespace(started=0)

    async def start():
        engine.started += 1

    engine.start = start
    return engine


class TestRunServer:
    @pytest.mark.asyncio
    async def test_a_startup_that_fails_fails_the_command(self):
        # (the process stayed up doing nothing: serve looked like a running server)
        from volnux.adapters.lifecycle.utils import _run_server

        engine = fake_engine()
        with pytest.raises(RuntimeError, match="failed to start"):
            await _run_server(FakeServer("fails_to_start"), engine, asyncio.Event())
        assert engine.started == 0  # triggers were not armed on a dead server

    @pytest.mark.asyncio
    async def test_a_crash_is_raised(self):
        from volnux.adapters.lifecycle.utils import _run_server

        with pytest.raises(OSError, match="address already in use"):
            await _run_server(FakeServer("crashes"), fake_engine(), asyncio.Event())

    @pytest.mark.asyncio
    async def test_a_server_that_starts_arms_the_triggers_and_stops_on_the_event(self):
        from volnux.adapters.lifecycle.utils import _run_server

        engine, stop, server = fake_engine(), asyncio.Event(), FakeServer("ok")
        task = asyncio.create_task(_run_server(server, engine, stop))
        await asyncio.sleep(0.4)
        assert server.started and engine.started == 1
        stop.set()
        await asyncio.wait_for(task, 5)
        assert server.should_exit is True

    @pytest.mark.asyncio
    async def test_the_triggers_wait_for_the_server(self):
        from volnux.adapters.lifecycle.utils import _start_triggers_after_startup

        engine, server = fake_engine(), FakeServer("ok")
        task = asyncio.create_task(_start_triggers_after_startup(server, engine))
        await asyncio.sleep(0.25)
        assert engine.started == 0
        server.started = True
        await asyncio.wait_for(task, 5)
        assert engine.started == 1

    @pytest.mark.asyncio
    async def test_the_triggers_are_not_armed_when_the_server_task_ended(self):
        from volnux.adapters.lifecycle.utils import _start_triggers_after_startup

        async def done():
            return None

        server_task = asyncio.create_task(done())
        await server_task
        engine = fake_engine()
        await _start_triggers_after_startup(FakeServer("ok"), engine, server_task)
        assert engine.started == 0


# ------------------------------------------------------ the worker server
class FakeCheckpointManager:
    def __init__(self, fail_drain=False):
        self.events = []
        self.running = False
        self.fail_drain = fail_drain

    async def start(self):
        self.running = True
        self.events.append("start")

    async def drain(self):
        self.events.append("drain")
        if self.fail_drain:
            raise RuntimeError("store down")

    async def stop(self):
        self.running = False
        self.events.append("stop")

    def is_healthy(self):
        return self.running

    async def health_check(self):
        return {"healthy": self.running}


@pytest.fixture
def worker(monkeypatch):
    """The worker with its telemetry, metrics server and checkpoint manager faked."""
    from volnux.adapters.lifecycle import worker_server

    order = []
    manager = FakeCheckpointManager()
    monkeypatch.setattr(
        worker_server,
        "_setup_otel",
        lambda engine, dev_mode, role: (lambda: order.append("otel")),
    )
    monkeypatch.setattr(
        worker_server,
        "_start_prometheus",
        lambda port: (lambda: order.append("prometheus")),
    )

    async def start_manager(engine):
        await manager.start()
        engine.checkpoint_manager = manager
        return manager

    monkeypatch.setattr(worker_server, "_start_checkpoint_manager", start_manager)
    return SimpleNamespace(module=worker_server, order=order, manager=manager)


class TestWorkerServer:
    @pytest.mark.asyncio
    async def test_it_runs_until_stopped_and_shuts_down_in_order(self, worker):
        engine, stop = SimpleNamespace(), asyncio.Event()
        task = asyncio.create_task(
            worker.module.start_worker_server(
                engine=engine,
                host="0.0.0.0",
                port=9090,
                metrics_port=9464,
                log_level="info",
                stop_event=stop,
            )
        )
        await asyncio.sleep(0.2)
        assert not task.done()  # alive until told to stop
        assert engine.checkpoint_manager is worker.manager and worker.manager.running
        stop.set()
        await asyncio.wait_for(task, 10)

        assert worker.manager.events == ["start", "drain", "stop"]
        assert worker.order == ["otel", "prometheus"]
        assert not worker.manager.running

    @pytest.mark.asyncio
    async def test_the_health_monitor_supervises_the_checkpoint_manager(self, worker):
        monitor = await worker.module._start_health_monitor(worker.manager)
        try:
            assert "checkpoint" in monitor._managers
            assert monitor._running
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_shutdown_goes_on_when_a_step_fails(self, worker):
        manager = FakeCheckpointManager(fail_drain=True)
        monitor = Mock()

        async def broken_stop():
            raise RuntimeError("monitor stuck")

        monitor.stop = broken_stop
        await worker.module._worker_shutdown(
            checkpoint_manager=manager,
            health_monitor=monitor,
            otel_shutdown=lambda: worker.order.append("otel"),
            prometheus_shutdown=lambda: worker.order.append("prometheus"),
        )
        assert manager.events == ["drain", "stop"]  # still stopped
        assert worker.order == ["otel", "prometheus"]

    @pytest.mark.asyncio
    async def test_shutdown_with_nothing_started(self, worker):
        await worker.module._worker_shutdown(None, None, lambda: None, lambda: None)

    @pytest.mark.asyncio
    async def test_components_that_failed_to_start_do_not_block_the_shutdown(
        self, worker
    ):
        async def broken(engine):
            raise RuntimeError("no checkpoint store")

        worker.module._start_checkpoint_manager = broken
        with pytest.raises(RuntimeError, match="no checkpoint store"):
            await worker.module.start_worker_server(
                engine=SimpleNamespace(),
                host="h",
                port=1,
                metrics_port=2,
                log_level="info",
                stop_event=asyncio.Event(),
            )
        assert worker.order == ["otel", "prometheus"]  # telemetry and metrics closed

    @pytest.mark.asyncio
    async def test_the_real_checkpoint_manager_is_built_from_the_settings(self):
        from volnux.adapters.lifecycle.worker_server import _start_checkpoint_manager

        engine = SimpleNamespace()
        manager = await _start_checkpoint_manager(engine)
        try:
            assert engine.checkpoint_manager is manager and manager.running
            assert manager.is_healthy()
        finally:
            await manager.stop()


# ------------------------------------------------------ the serve command
@pytest.fixture
def command(monkeypatch):
    cmd = ServeCommand.__new__(ServeCommand)
    cmd.errors = []
    monkeypatch.setattr(
        ServeCommand, "error", lambda self, msg: self.errors.append(msg)
    )
    monkeypatch.setattr(ServeCommand, "warning", lambda self, msg: None)
    monkeypatch.setattr(ServeCommand, "_configure_json_logging", lambda *a: None)
    monkeypatch.setattr(ServeCommand, "_print_startup_summary", lambda self, **k: None)
    monkeypatch.setattr(serve_module, "setting", lambda name, default=None: default)
    return cmd


OPTIONS = dict(
    role="api",
    host="127.0.0.1",
    port=1,
    worker_port=2,
    metrics_port=3,
    log_level="INFO",
    drain_timeout=1,
    project_dir="/some/project",
)


class TestServeCommand:
    def test_a_project_that_cannot_be_loaded_exits_with_an_error(
        self, command, monkeypatch
    ):
        def broken(self, project_dir, workflow_name=None):
            raise CommandError("init.py has no engine")

        monkeypatch.setattr(ServeCommand, "initialise_workflows", broken)
        with pytest.raises(SystemExit) as caught:
            command.handle(**OPTIONS)
        assert caught.value.code == 1
        assert "init.py has no engine" in command.errors[0]

    def test_no_project_found_exits_with_an_error(self, command, monkeypatch):
        def missing(self):
            raise CommandError("No Volnux project found")

        monkeypatch.setattr(ServeCommand, "resolve_project_dir", missing)
        with pytest.raises(SystemExit) as caught:
            command.handle(**{**OPTIONS, "project_dir": None})
        assert caught.value.code == 1

    def test_a_failed_preflight_exits_with_an_error(self, command, monkeypatch):
        monkeypatch.setattr(
            ServeCommand,
            "initialise_workflows",
            lambda self, d, w=None: SimpleNamespace(),
        )

        async def preflight(self, engine, role):
            raise CommandError("database 'default' unreachable")

        monkeypatch.setattr(ServeCommand, "_preflight", preflight)
        with pytest.raises(SystemExit) as caught:
            command.handle(**OPTIONS)
        assert caught.value.code == 1
        assert "unreachable" in command.errors[0]

    @pytest.mark.parametrize("role", ["api", "worker"])
    def test_the_role_picks_the_server(self, command, monkeypatch, role):
        engine = SimpleNamespace(project_name="demo")
        started = []

        async def start_api_server(**kwargs):
            started.append(("api", kwargs))

        async def start_worker_server(**kwargs):
            started.append(("worker", kwargs))

        monkeypatch.setattr(
            ServeCommand, "initialise_workflows", lambda self, d, w=None: engine
        )

        async def preflight(self, engine, role):
            return None

        monkeypatch.setattr(ServeCommand, "_preflight", preflight)
        monkeypatch.setattr("volnux.server.start_api_server", start_api_server)
        monkeypatch.setattr("volnux.server.start_worker_server", start_worker_server)
        monkeypatch.setattr(signal_module(), "signal", lambda *a: None)
        monkeypatch.setenv("VOLNUX_ROLE", "before")

        command.handle(**{**OPTIONS, "role": role})

        # the role reaches initialise_workflows (and its telemetry) as the ROLE setting
        import os

        assert os.environ["VOLNUX_ROLE"] == role

        ((which, kwargs),) = started
        assert which == role
        assert kwargs["engine"] is engine
        assert kwargs["metrics_port"] == 3
        if role == "api":
            assert kwargs["host"] == "127.0.0.1" and kwargs["port"] == 1
            assert kwargs["dev_mode"] is False
        else:
            assert kwargs["port"] == 2  # the worker port

    def test_a_missing_server_stack_is_a_clear_error_not_a_stub(
        self, command, monkeypatch
    ):
        import builtins

        engine = SimpleNamespace(project_name="demo")
        monkeypatch.setattr(
            ServeCommand, "initialise_workflows", lambda self, d, w=None: engine
        )

        async def preflight(self, engine, role):
            return None

        monkeypatch.setattr(ServeCommand, "_preflight", preflight)
        monkeypatch.setattr(signal_module(), "signal", lambda *a: None)
        real_import = builtins.__import__

        def refuse(name, *args, **kwargs):
            if name == "volnux.server":
                raise ImportError("No module named 'uvicorn'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", refuse)
        with pytest.raises(SystemExit) as caught:
            command.handle(**OPTIONS)
        monkeypatch.setattr(builtins, "__import__", real_import)
        assert caught.value.code == 1
        assert "uvicorn" in " ".join(command.errors)


def signal_module():
    import signal

    return signal


# --------------------------------------------------------------- pre-flight
class TestPreflight:
    @pytest.fixture
    def preflight(self, command, monkeypatch):
        calls = SimpleNamespace(broker_ok=False)

        async def check_backends(engine):
            return [("default", True, "reachable")]

        async def check_broker(url):
            return calls.broker_ok, "down"

        monkeypatch.setattr(
            "volnux.cli.command.mixins.check._check_backends", check_backends
        )
        monkeypatch.setattr(serve_module, "_check_broker", check_broker)
        return calls

    def engine(self, *trigger_types):
        return SimpleNamespace(
            triggers={
                str(i): SimpleNamespace(trigger_type=t)
                for i, t in enumerate(trigger_types)
            }
        )

    @pytest.mark.asyncio
    async def test_an_unreachable_broker_is_fine_without_a_schedule_trigger(
        self, command, preflight
    ):
        # (a project with no schedule trigger could not be served without a Redis)
        await command._preflight(self.engine(TriggerType.MANUAL), "api")

    @pytest.mark.asyncio
    async def test_an_unreachable_broker_fails_the_api_when_there_is_a_schedule_trigger(
        self, command, preflight
    ):
        with pytest.raises(CommandError, match="Celery broker unreachable"):
            await command._preflight(self.engine(TriggerType.SCHEDULE), "api")

    @pytest.mark.asyncio
    async def test_the_worker_only_warns(self, command, preflight, caplog):
        await command._preflight(self.engine(TriggerType.SCHEDULE), "worker")

    @pytest.mark.asyncio
    async def test_a_reachable_broker(self, command, preflight):
        preflight.broker_ok = True
        await command._preflight(self.engine(TriggerType.SCHEDULE), "api")

    @pytest.mark.asyncio
    async def test_an_unreachable_database_always_fails(
        self, command, preflight, monkeypatch
    ):
        async def check_backends(engine):
            return [("default", False, "connection refused")]

        monkeypatch.setattr(
            "volnux.cli.command.mixins.check._check_backends", check_backends
        )
        preflight.broker_ok = True
        with pytest.raises(CommandError, match="Database 'default' unreachable"):
            await command._preflight(self.engine(), "worker")
