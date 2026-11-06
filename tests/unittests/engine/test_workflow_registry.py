"""WorkflowRegistry: workflows, sources, discovery and loading."""

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from volnux.engine.workflows import registry as registry_module
from volnux.engine.workflows.registry import (
    WorkflowRegistry,
    get_workflow_registry,
    reset_workflow_registry,
)
from volnux.engine.workflows.source import RegistrySource, WorkflowSource
from volnux.event.registry import RegistryNotReady
from volnux.exceptions import ImproperlyConfigured


# --------------------------------------------------------------------- doubles
class FakeWorkflow:
    """The part of WorkflowConfig the registry uses."""

    def __init__(self, name, issues=(), events=(), pipeline=None, batch=None):
        self.name = name
        self._issues = list(issues)
        self._events = list(events)
        self._pipeline = pipeline
        self._batch = batch

    def check(self):
        return list(self._issues)

    def get_event_classes(self):
        return list(self._events)

    def get_pipeline_class(self):
        if self._pipeline is None:
            raise RuntimeError(f"Workflow '{self.name}' has no pipeline module.")
        return self._pipeline

    def get_batch_pipeline_class(self):
        if self._batch is None:
            raise RuntimeError(f"Workflow '{self.name}' has no batch pipeline module.")
        return self._batch


class EventA: ...


class EventB: ...


class PipelineA: ...


class PipelineB: ...


class BatchA: ...


class FakeSource:
    """A source whose load is a function."""

    def __init__(self, name, load, source_type=RegistrySource.LOCAL):
        self.name = name
        self.source_type = source_type
        self.location = name
        self._load = load
        self.calls = 0

    async def load_workflow_config(self, registry, params):
        self.calls += 1
        return await self._load(registry, params)


def ok(*workflows):
    async def load(registry, params):
        for workflow in workflows:
            registry.register(workflow)
        return SimpleNamespace(error=False, content="loaded")

    return load


def failing_result(message):
    async def load(registry, params):
        return SimpleNamespace(error=True, content=message)

    return load


def raising(exc):
    async def load(registry, params):
        raise exc

    return load


@pytest.fixture
def registry(tmp_path):
    return WorkflowRegistry(cache_dir=tmp_path / "cache")


def local_source(name, location="/w"):
    return WorkflowSource(
        name=name, location=location, source_type=RegistrySource.LOCAL
    )


def git_source(name):
    return WorkflowSource(
        name=name,
        location="https://example.com/repo.git",
        source_type=RegistrySource.GIT,
    )


# ------------------------------------------------------------------ construction
class TestConstruction:
    def test_it_starts_empty_and_not_ready(self, registry):
        assert registry.get_workflow_configs() == []
        assert not registry.is_ready()
        assert registry.load_errors == {}

    def test_building_one_creates_no_directory(self, tmp_path):
        cache = tmp_path / "nested" / "cache"
        WorkflowRegistry(cache_dir=cache)
        assert not cache.exists()

    def test_the_default_cache_is_under_the_home_directory(self):
        assert WorkflowRegistry()._cache_dir == (
            Path.home() / ".volnux" / "workflow_cache"
        )

    def test_no_lock_is_made_until_a_load(self, registry):
        # (a lock made at import belongs to whichever loop was current)
        assert registry._load_lock is None


# ---------------------------------------------------------------------- workflows
class TestWorkflows:
    def test_register_and_get(self, registry):
        workflow = FakeWorkflow("a")
        registry.register(workflow)
        assert registry.get_workflow_config("a") is workflow
        assert registry.get_workflow_configs() == [workflow]

    def test_unknown_workflow_is_none(self, registry):
        assert registry.get_workflow_config("nope") is None

    def test_duplicate_names_are_refused(self, registry):
        registry.register(FakeWorkflow("a"))
        with pytest.raises(ValueError, match="already registered"):
            registry.register(FakeWorkflow("a"))
        assert len(registry.get_workflow_configs()) == 1

    def test_configs_come_back_in_registration_order(self, registry):
        for name in ("c", "a", "b"):
            registry.register(FakeWorkflow(name))
        assert [w.name for w in registry.get_workflow_configs()] == ["c", "a", "b"]

    def test_the_returned_list_is_a_copy(self, registry):
        registry.register(FakeWorkflow("a"))
        registry.get_workflow_configs().clear()
        assert len(registry.get_workflow_configs()) == 1

    def test_check_all_reports_only_workflows_with_issues(self, registry):
        registry.register(FakeWorkflow("good"))
        registry.register(FakeWorkflow("bad", issues=["no pipeline", "no events"]))
        assert registry.check_all() == {"bad": ["no pipeline", "no events"]}

    def test_check_all_of_nothing(self, registry):
        assert registry.check_all() == {}


# ------------------------------------------------------------------------ sources
class TestSources:
    def test_local_and_remote_sources_are_kept_apart(self, registry):
        local, remote = local_source("l"), git_source("r")
        registry.add_workflow_source(local)
        registry.add_workflow_source(remote)
        assert registry._workflow_local_sources == {"l": local}
        assert registry._workflow_remote_sources == {"r": remote}

    def test_get_workflow_source_finds_either(self, registry):
        local, remote = local_source("l"), git_source("r")
        registry.add_workflow_source(local)
        registry.add_workflow_source(remote)
        assert registry.get_workflow_source("l") is local
        assert registry.get_workflow_source("r") is remote
        assert registry.get_workflow_source("x") is None

    def test_combined_sources_hold_both_kinds(self, registry):
        registry.add_workflow_source(git_source("r"))
        registry.add_workflow_source(local_source("l"))
        assert sorted(registry.combined_workflow_sources) == ["l", "r"]

    def test_a_local_source_wins_over_a_remote_of_the_same_name(self, registry):
        # (populate_local_workflow_configs can create such a pair)
        local, remote = local_source("x"), git_source("x")
        registry._workflow_remote_sources["x"] = remote
        registry._workflow_local_sources["x"] = local
        assert registry.get_workflow_source("x") is local

    def test_a_source_name_is_unique_across_both_kinds(self, registry):
        registry.add_workflow_source(local_source("x"))
        with pytest.raises(ValueError, match="already registered"):
            registry.add_workflow_source(git_source("x"))
        with pytest.raises(ValueError, match="already registered"):
            registry.add_workflow_source(local_source("x"))


# ---------------------------------------------------------------------- discovery
class TestPopulateLocal:
    def project(self, tmp_path, *names, files=()):
        workflows = tmp_path / "workflows"
        workflows.mkdir()
        for name in names:
            (workflows / name).mkdir()
        for name in files:
            (workflows / name).write_text("")
        return tmp_path

    @pytest.mark.asyncio
    async def test_every_workflow_directory_becomes_a_source(self, registry, tmp_path):
        project = self.project(tmp_path, "alpha", "beta")
        await registry.populate_local_workflow_configs(project)
        sources = registry._workflow_local_sources
        assert sorted(sources) == ["alpha", "beta"]
        assert sources["alpha"].source_type is RegistrySource.LOCAL
        assert Path(sources["alpha"].location) == project / "workflows" / "alpha"

    @pytest.mark.asyncio
    async def test_files_are_not_workflows(self, registry, tmp_path):
        project = self.project(tmp_path, "alpha", files=["README.md", "__init__.py"])
        await registry.populate_local_workflow_configs(project)
        assert list(registry._workflow_local_sources) == ["alpha"]

    @pytest.mark.asyncio
    async def test_cache_and_hidden_directories_are_not_workflows(
        self, registry, tmp_path
    ):
        project = self.project(tmp_path, "alpha", "__pycache__", ".git", "_private")
        await registry.populate_local_workflow_configs(project)
        assert list(registry._workflow_local_sources) == ["alpha"]

    @pytest.mark.asyncio
    async def test_one_workflow_can_be_asked_for(self, registry, tmp_path):
        project = self.project(tmp_path, "alpha", "beta")
        await registry.populate_local_workflow_configs(project, "beta")
        assert list(registry._workflow_local_sources) == ["beta"]

    @pytest.mark.asyncio
    async def test_a_workflow_that_is_not_there_is_reported(
        self, registry, tmp_path, caplog
    ):
        project = self.project(tmp_path, "alpha")
        with caplog.at_level(logging.WARNING):
            await registry.populate_local_workflow_configs(project, "ghost")
        assert registry._workflow_local_sources == {}
        assert "'ghost' was not found" in caplog.text

    @pytest.mark.asyncio
    async def test_sources_are_added_in_name_order(self, registry, tmp_path):
        project = self.project(tmp_path, "zeta", "alpha", "mid")
        await registry.populate_local_workflow_configs(project)
        assert list(registry._workflow_local_sources) == ["alpha", "mid", "zeta"]

    @pytest.mark.asyncio
    async def test_no_workflows_directory(self, registry, tmp_path, caplog):
        with caplog.at_level(logging.WARNING):
            await registry.populate_local_workflow_configs(tmp_path)
        assert registry._workflow_local_sources == {}
        assert "does not exist" in caplog.text

    @pytest.mark.asyncio
    async def test_workflows_that_is_a_file(self, registry, tmp_path):
        (tmp_path / "workflows").write_text("")
        with pytest.raises(ImproperlyConfigured, match="Not a directory"):
            await registry.populate_local_workflow_configs(tmp_path)

    @pytest.mark.asyncio
    async def test_populating_again_refreshes_instead_of_failing(
        self, registry, tmp_path
    ):
        project = self.project(tmp_path, "alpha")
        await registry.populate_local_workflow_configs(project)
        await registry.populate_local_workflow_configs(project)
        assert list(registry._workflow_local_sources) == ["alpha"]

    @pytest.mark.asyncio
    async def test_shadowing_a_remote_source_is_warned_about(
        self, registry, tmp_path, caplog
    ):
        registry.add_workflow_source(git_source("alpha"))
        project = self.project(tmp_path, "alpha")
        with caplog.at_level(logging.WARNING):
            await registry.populate_local_workflow_configs(project)
        assert "shadows the remote source" in caplog.text


# ------------------------------------------------------------------------ loading
class TestLoading:
    @pytest.mark.asyncio
    async def test_the_registry_is_ready_after_a_load(self, registry):
        registry._workflow_local_sources["a"] = FakeSource("a", ok(FakeWorkflow("a")))
        assert not registry.is_ready()
        await registry.load_workflow_configs()
        assert registry.is_ready()
        assert registry.get_workflow_config("a") is not None

    @pytest.mark.asyncio
    async def test_with_no_sources_it_is_ready_and_empty(self, registry, caplog):
        with caplog.at_level(logging.WARNING):
            await registry.load_workflow_configs()
        assert registry.is_ready() and registry.get_workflow_configs() == []
        assert "No workflow sources" in caplog.text

    @pytest.mark.asyncio
    async def test_every_source_loads(self, registry):
        for name in ("a", "b", "c"):
            registry._workflow_local_sources[name] = FakeSource(
                name, ok(FakeWorkflow(name))
            )
        await registry.load_workflow_configs()
        assert sorted(w.name for w in registry.get_workflow_configs()) == [
            "a",
            "b",
            "c",
        ]

    @pytest.mark.asyncio
    async def test_sources_load_concurrently(self, registry):
        started = []
        gate = asyncio.Event()

        def waiting(name):
            async def load(reg, params):
                started.append(name)
                if len(started) == 3:
                    gate.set()
                await asyncio.wait_for(gate.wait(), 2)  # needs the others to start
                return SimpleNamespace(error=False, content="")

            return load

        for name in ("a", "b", "c"):
            registry._workflow_local_sources[name] = FakeSource(name, waiting(name))
        await registry.load_workflow_configs()
        assert sorted(started) == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_the_loader_gets_the_registry_and_the_cache_dir(self, registry):
        seen = {}

        async def load(reg, params):
            seen.update(registry=reg, params=params)
            return SimpleNamespace(error=False, content="")

        registry._workflow_local_sources["a"] = FakeSource("a", load)
        await registry.load_workflow_configs()
        assert seen["registry"] is registry
        assert seen["params"] == {"cache_dir": registry._cache_dir}

    @pytest.mark.asyncio
    async def test_a_source_that_raises_does_not_stop_the_others(
        self, registry, caplog
    ):
        registry._workflow_local_sources["bad"] = FakeSource(
            "bad", raising(OSError("disk"))
        )
        registry._workflow_local_sources["good"] = FakeSource(
            "good", ok(FakeWorkflow("good"))
        )
        with caplog.at_level(logging.ERROR):
            await registry.load_workflow_configs()
        assert registry.is_ready()
        assert registry.get_workflow_config("good") is not None
        assert registry.load_errors == {"bad": "disk"}
        assert "Failed to load config from bad" in caplog.text

    @pytest.mark.asyncio
    async def test_an_error_result_is_a_failure_not_a_success(self, registry, caplog):
        registry._workflow_local_sources["bad"] = FakeSource(
            "bad", failing_result("no such package")
        )
        with caplog.at_level(logging.DEBUG):
            await registry.load_workflow_configs()
        assert registry.load_errors == {"bad": "no such package"}
        assert "Successfully loaded config from bad" not in caplog.text
        assert "Failed to load config from bad" in caplog.text

    @pytest.mark.asyncio
    async def test_a_successful_load_is_logged_as_one(self, registry, caplog):
        registry._workflow_local_sources["a"] = FakeSource("a", ok(FakeWorkflow("a")))
        with caplog.at_level(logging.DEBUG):
            await registry.load_workflow_configs()
        assert "Successfully loaded config from a" in caplog.text
        assert registry.load_errors == {}

    @pytest.mark.asyncio
    async def test_errors_are_for_the_last_load_only(self, registry):
        registry._workflow_local_sources["bad"] = FakeSource(
            "bad", raising(OSError("x"))
        )
        await registry.load_workflow_configs()
        assert registry.load_errors
        # a call after the first load has finished loads again
        registry._workflow_local_sources["bad"] = FakeSource("bad", ok())
        await registry.load_workflow_configs()
        assert registry.load_errors == {}

    @pytest.mark.asyncio
    async def test_is_not_ready_while_loading(self, registry):
        states = []

        async def load(reg, params):
            states.append(reg.is_ready())
            return SimpleNamespace(error=False, content="")

        registry._workflow_local_sources["a"] = FakeSource("a", load)
        await registry.load_workflow_configs()
        assert states == [False] and registry.is_ready()

    @pytest.mark.asyncio
    async def test_concurrent_calls_wait_for_the_one_load(self, registry):
        gate = asyncio.Event()

        async def load(reg, params):
            await gate.wait()
            reg.register(FakeWorkflow("a"))
            return SimpleNamespace(error=False, content="")

        source = FakeSource("a", load)
        registry._workflow_local_sources["a"] = source

        first = asyncio.create_task(registry.load_workflow_configs())
        await asyncio.sleep(0.02)
        second = asyncio.create_task(registry.load_workflow_configs())
        await asyncio.sleep(0.02)
        assert not second.done()  # waiting for the load, not returning early
        gate.set()
        await asyncio.gather(first, second)

        assert source.calls == 1  # loaded once
        assert registry.is_ready()
        assert len(registry.get_workflow_configs()) == 1

    @pytest.mark.asyncio
    async def test_the_second_caller_sees_a_ready_registry(self, registry):
        gate = asyncio.Event()

        async def load(reg, params):
            await gate.wait()
            return SimpleNamespace(error=False, content="")

        registry._workflow_local_sources["a"] = FakeSource("a", load)
        first = asyncio.create_task(registry.load_workflow_configs())
        await asyncio.sleep(0.02)

        async def second():
            await registry.load_workflow_configs()
            return registry.is_ready()

        waiter = asyncio.create_task(second())
        await asyncio.sleep(0.02)
        gate.set()
        await first
        assert await waiter is True

    @pytest.mark.asyncio
    async def test_the_cache_dir_is_created_for_a_remote_source(self, registry):
        registry._workflow_remote_sources["r"] = FakeSource(
            "r", ok(), source_type=RegistrySource.GIT
        )
        assert not registry._cache_dir.exists()
        await registry.load_workflow_configs()
        assert registry._cache_dir.is_dir()

    @pytest.mark.asyncio
    async def test_the_cache_dir_is_not_created_for_local_sources_only(self, registry):
        registry._workflow_local_sources["a"] = FakeSource("a", ok())
        await registry.load_workflow_configs()
        assert not registry._cache_dir.exists()

    @pytest.mark.asyncio
    async def test_an_uncreatable_cache_dir_does_not_stop_the_load(
        self, tmp_path, caplog
    ):
        blocker = tmp_path / "file"
        blocker.write_text("")
        registry = WorkflowRegistry(cache_dir=blocker / "cache")  # under a file
        registry._workflow_remote_sources["r"] = FakeSource(
            "r", ok(FakeWorkflow("r")), source_type=RegistrySource.GIT
        )
        with caplog.at_level(logging.WARNING):
            await registry.load_workflow_configs()
        assert registry.get_workflow_config("r") is not None
        assert "Cannot create the workflow cache" in caplog.text

    @pytest.mark.asyncio
    async def test_a_real_local_source_loads_through_its_loader(
        self, registry, tmp_path, monkeypatch
    ):
        # WorkflowSource.load_workflow_config is what the registry calls
        calls = []

        async def load(self, reg, options=None):
            calls.append((self.name, reg, options))
            return SimpleNamespace(error=False, content="")

        monkeypatch.setattr(WorkflowSource, "load_workflow_config", load)
        registry.add_workflow_source(local_source("real"))
        await registry.load_workflow_configs()
        assert calls == [("real", registry, {"cache_dir": registry._cache_dir})]


# ----------------------------------------------------------------------- readiness
class TestReadiness:
    def test_make_ready(self, registry):
        registry.make_ready()
        assert registry.is_ready()

    def test_loading_is_not_ready(self, registry):
        registry.make_ready()
        registry._loading = True
        assert not registry.is_ready()

    @pytest.mark.parametrize(
        "method", ["get_events", "get_pipelines", "get_batch_pipelines"]
    )
    def test_collections_need_a_ready_registry(self, registry, method):
        with pytest.raises(RegistryNotReady):
            getattr(registry, method)()


class TestCollections:
    @pytest.fixture
    def ready(self, registry):
        registry.register(
            FakeWorkflow("a", events=[EventA, EventB], pipeline=PipelineA, batch=BatchA)
        )
        registry.register(
            FakeWorkflow("b", events=[EventB], pipeline=PipelineB, batch=BatchA)
        )
        registry.make_ready()
        return registry

    def test_events_are_the_union(self, ready):
        assert ready.get_events() == {EventA, EventB}

    def test_pipelines(self, ready):
        assert ready.get_pipelines() == {PipelineA, PipelineB}

    def test_batch_pipelines_are_distinct(self, ready):
        assert ready.get_batch_pipelines() == {BatchA}

    def test_a_workflow_without_a_pipeline_is_skipped(self, ready, caplog):
        ready.register(FakeWorkflow("nopipe", pipeline=None, batch=None))
        with caplog.at_level(logging.WARNING):
            assert ready.get_pipelines() == {PipelineA, PipelineB}
            assert ready.get_batch_pipelines() == {BatchA}
        assert "Skipping pipeline discovery for workflow 'nopipe'" in caplog.text
        assert "Skipping batch pipeline discovery for workflow 'nopipe'" in caplog.text

    def test_an_empty_ready_registry(self, registry):
        registry.make_ready()
        assert registry.get_events() == set()
        assert registry.get_pipelines() == set()
        assert registry.get_batch_pipelines() == set()


# ------------------------------------------------------------------------ singleton
class TestSingleton:
    @pytest.fixture(autouse=True)
    def fresh(self):
        reset_workflow_registry()
        yield
        reset_workflow_registry()

    def test_it_is_one_object(self):
        assert get_workflow_registry() is get_workflow_registry()

    def test_it_is_made_on_first_use(self):
        assert registry_module._workflow_registry is None
        get_workflow_registry()
        assert registry_module._workflow_registry is not None

    def test_reset_makes_a_new_one(self):
        first = get_workflow_registry()
        first.register(FakeWorkflow("a"))
        reset_workflow_registry()
        second = get_workflow_registry()
        assert second is not first and second.get_workflow_configs() == []

    def test_the_cache_dir_setting_is_used(self, tmp_path):
        from volnux.conf import override_settings

        with override_settings(WORKFLOWS_CACHE_DIR=str(tmp_path / "wf-cache")):
            registry = get_workflow_registry()
        assert registry._cache_dir == tmp_path / "wf-cache"

    def test_importing_the_module_does_not_read_the_configuration(self, monkeypatch):
        import importlib
        import sys

        from volnux import config as config_module

        calls = []
        real = config_module.setting
        monkeypatch.setattr(
            config_module, "setting", lambda *a, **k: calls.append(a) or real(*a, **k)
        )
        saved = sys.modules.pop("volnux.engine.workflows.registry")
        try:
            importlib.import_module("volnux.engine.workflows.registry")
            assert calls == []
        finally:
            sys.modules["volnux.engine.workflows.registry"] = saved
