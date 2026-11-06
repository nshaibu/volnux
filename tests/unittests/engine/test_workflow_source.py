"""WorkflowSource: where workflows come from, and how a source drives its loader."""

import asyncio
import logging
import os
import shutil
import subprocess
import textwrap
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict

import pytest

from volnux.engine.workflows import source as source_module
from volnux.engine.workflows.registry import WorkflowRegistry
from volnux.engine.workflows.source import (
    RegistrySource,
    SourceCredentials,
    WorkflowSource,
)
from volnux.exceptions import ImproperlyConfigured
from volnux.result import EventResult


def make(**overrides) -> WorkflowSource:
    base = dict(name="src", source_type="pypi", location="acme-flow", version="1.0.0")
    base.update(overrides)
    return WorkflowSource(**base)


# --- credentials ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs, valid",
    [
        ({"token": "t"}, True),
        ({"username": "u", "password": "p"}, True),
        ({"username": "u"}, False),
        ({"password": "p"}, False),
        ({}, False),
        ({"email": "e@x"}, False),
    ],
)
def test_credentials_need_a_token_or_a_username_and_password(kwargs, valid):
    assert SourceCredentials(**kwargs).is_valid() is valid


def test_credentials_never_show_their_values():
    text = repr(SourceCredentials(username="bob", password="hunter2", token="tok-123"))
    assert "hunter2" not in text and "tok-123" not in text and "bob" not in text


# --- construction ----------------------------------------------------------------------------------------


def test_defaults():
    source = make()
    assert (source.timeout, source.retries, source.priority) == (30_000, 3, 1)
    assert (
        source.credentials is None and source.events is None and source.metadata == {}
    )


@pytest.mark.parametrize(
    "given, expected",
    [
        ("local", RegistrySource.LOCAL),
        ("pypi", RegistrySource.PYPI),
        ("git", RegistrySource.GIT),
        ("hub", RegistrySource.HUB),
        (RegistrySource.GIT, RegistrySource.GIT),
    ],
)
def test_the_source_type_may_be_given_as_text(given, expected):
    assert make(source_type=given).source_type is expected


@pytest.mark.parametrize("bad", ["ftp", "", "PYPI", 5, None])
def test_an_unknown_source_type_is_refused(bad):
    with pytest.raises(ValueError, match="source_type"):
        make(source_type=bad)


@pytest.mark.parametrize("name", ["", "   ", None, 5])
def test_a_source_needs_a_name(name):
    with pytest.raises(ValueError, match="name"):
        make(name=name)


@pytest.mark.parametrize("location", ["", "  ", None])
def test_a_source_needs_a_location(location):
    with pytest.raises(ValueError, match="location"):
        make(location=location)


@pytest.mark.parametrize("timeout", [0, -5, 1.5, "30", True])
def test_the_timeout_is_a_positive_number_of_milliseconds(timeout):
    with pytest.raises(ValueError, match="timeout"):
        make(timeout=timeout)


@pytest.mark.parametrize("retries", [-1, 2.5, "3", True])
def test_retries_are_zero_or_more(retries):
    with pytest.raises(ValueError, match="retries"):
        make(retries=retries)
    assert make(retries=0).retries == 0


@pytest.mark.parametrize("events", ["OneEvent", [1, 2], ["ok", 3]])
def test_events_are_a_list_of_names(events):
    with pytest.raises(ValueError, match="events"):
        make(events=events)


def test_events_are_copied_and_a_local_source_warns_that_they_do_nothing(caplog):
    names = ["A", "B"]
    with caplog.at_level(logging.WARNING):
        source = make(source_type="local", location=".", events=names, version=None)
    names.append("C")
    assert source.events == ["A", "B"] and "no effect on a local source" in caplog.text


def test_the_repr_hides_the_credentials_and_the_metadata():
    source = make(
        credentials=SourceCredentials(token="tok-123"),
        metadata={"index_url": "https://u:s3cret@p/simple"},
    )
    text = repr(source)
    assert "tok-123" not in text and "s3cret" not in text and "src" in text


# --- what each loader is called with ----------------------------------------------------------------------


def test_a_local_source_calls_the_loader_with_a_directory(tmp_path):
    """The local loader's argument is `workflow_dir`; the source passed `location` and the loader
    got None for the argument it wanted."""
    source = make(source_type="local", location=str(tmp_path), version=None)

    call = source.build_loader_call("REGISTRY")

    assert call == {"workflow_dir": tmp_path, "registry": "REGISTRY"}
    assert isinstance(call["workflow_dir"], Path)


def test_a_pypi_source_passes_name_version_credentials_and_index():
    credentials = SourceCredentials(token="t")
    source = make(
        credentials=credentials,
        timeout=45_000,
        metadata={"index_url": "https://p/simple"},
    )

    call = source.build_loader_call("R")

    assert call == {
        "location": "acme-flow",
        "registry": "R",
        "version": "1.0.0",
        "credentials": credentials,
        "timeout": 45_000,
        "index_url": "https://p/simple",
    }


def test_a_git_source_counts_its_timeout_in_seconds():
    """The git loader takes seconds; 30 000 ms went to it as 30 000 seconds."""
    call = make(
        source_type="git", location="https://g/r.git", timeout=30_000
    ).build_loader_call("R")
    assert call["timeout"] == 30.0


def test_a_git_source_names_the_directory_and_the_ref():
    source = make(
        source_type="git", location="https://g/r.git", name="billing", version=None
    )
    call = source.build_loader_call("R")
    assert (
        call["workflow_name"] == "billing"
        and call["branch"] == "main"
        and call["as_package"] is False
    )

    pinned = make(source_type="git", location="https://g/r.git", version="v1.2")
    assert pinned.build_loader_call("R")["branch"] == "v1.2"

    custom = make(
        source_type="git",
        location="https://g/r.git",
        version="v1.2",
        metadata={
            "workflow_name": "flows/billing",
            "branch": "deadbeef",
            "as_package": True,
        },
    )
    call = custom.build_loader_call("R")
    assert (call["workflow_name"], call["branch"], call["as_package"]) == (
        "flows/billing",
        "deadbeef",
        True,
    )


def test_a_hub_source_passes_the_package_version_and_credentials():
    credentials = SourceCredentials(token="jwt")
    call = make(source_type="hub", credentials=credentials).build_loader_call("R")
    assert call == {
        "location": "acme-flow",
        "registry": "R",
        "version": "1.0.0",
        "credentials": credentials,
        "timeout": 30_000,
    }


def test_a_path_location_is_given_to_the_loader_as_text_where_it_wants_text(tmp_path):
    assert make(location=tmp_path).build_loader_call("R")["location"] == str(tmp_path)


# --- the options of the loader -----------------------------------------------------------------------------


def test_loader_options_are_the_registrys_plus_the_sources_own():
    source = make(
        source_type="hub",
        metadata={
            "eventhub_url": "https://hub",
            "options": {"extra": 1},
            "unrelated": 9,
        },
    )

    merged = source.loader_options({"cache_dir": "/cache"})

    assert merged == {"cache_dir": "/cache", "eventhub_url": "https://hub", "extra": 1}


def test_a_source_can_override_the_registrys_cache_dir():
    assert (
        make(metadata={"cache_dir": "/mine"}).loader_options({"cache_dir": "/cache"})[
            "cache_dir"
        ]
        == "/mine"
    )


def test_without_any_options_there_are_none():
    assert make().loader_options(None) == {}


# --- what a source cannot do ------------------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["pypi", "hub"])
def test_a_pinned_source_type_needs_a_version(kind):
    assert (
        "needs a version" in make(source_type=kind, version=None).missing_requirement()
    )
    assert make(source_type=kind).missing_requirement() is None


def test_a_git_source_may_go_without_a_version():
    assert make(source_type="git", version=None).missing_requirement() is None


def test_a_local_source_needs_its_directory(tmp_path):
    assert (
        "not found"
        in make(
            source_type="local", location=tmp_path / "nope", version=None
        ).missing_requirement()
    )
    assert (
        make(source_type="local", location=tmp_path, version=None).missing_requirement()
        is None
    )


# --- driving the loader (fake loaders record the call) -----------------------------------------------------------


class FakeLoader:
    """Stands in for a loader class; one log per subclass."""

    calls: list = []
    outcome = "ok"
    instances: list = []

    def __init__(self, execution_context, task_id, options=None):
        self.execution_context, self.task_id, self.options = (
            execution_context,
            task_id,
            options,
        )
        self.retry_policy = None
        type(self).instances.append(self)

    async def __call__(self, **kwargs):
        type(self).calls.append(kwargs)
        if type(self).outcome == "raise":
            raise RuntimeError("loader exploded")
        if type(self).outcome == "fail":
            return EventResult(
                error=True, event_name="x", content=None, task_id="t", workflow_id="w"
            )
        if type(self).outcome == "fail-with-reason":
            return EventResult(
                error=True,
                event_name="x",
                content="bad credentials",
                task_id="t",
                workflow_id="w",
            )
        return EventResult(
            error=False, event_name="x", content="loaded", task_id="t", workflow_id="w"
        )


@pytest.fixture
def fakes(monkeypatch):
    made = {}
    for kind in RegistrySource:
        cls = type(
            f"Fake{kind.value}",
            (FakeLoader,),
            {"calls": [], "instances": [], "outcome": "ok"},
        )
        monkeypatch.setitem(source_module._LOADER_REGISTRY, kind, cls)
        made[kind.value] = cls
    return SimpleNamespace(**made)


@pytest.mark.asyncio
async def test_the_loader_for_the_source_type_is_called_with_its_arguments(fakes):
    source = make(source_type="pypi", metadata={"index_url": "https://p/simple"})

    result = await source.load_workflow_config("REGISTRY", {"cache_dir": "/c"})

    assert result.error is False and result.content == "loaded"
    [call] = fakes.pypi.calls
    assert (
        call["location"] == "acme-flow"
        and call["registry"] == "REGISTRY"
        and call["index_url"] == "https://p/simple"
    )
    assert fakes.git.calls == [] and fakes.hub.calls == [] and fakes.local.calls == []


@pytest.mark.asyncio
async def test_the_loader_is_built_with_the_source_name_and_the_options(fakes):
    source = make(source_type="hub", metadata={"eventhub_url": "https://hub"})

    await source.load_workflow_config(None, {"cache_dir": "/c"})

    [loader] = fakes.hub.instances
    assert loader.task_id == "src" and loader.execution_context is None
    assert loader.options.extras == {"cache_dir": "/c", "eventhub_url": "https://hub"}


@pytest.mark.asyncio
async def test_the_retry_policy_is_set_on_the_instance_from_the_source(fakes):
    await make(retries=4).load_workflow_config(None)

    [loader] = fakes.pypi.instances
    assert loader.retry_policy.max_attempts == 4
    assert TimeoutError in loader.retry_policy.retry_on_exceptions
    assert ConnectionError in loader.retry_policy.retry_on_exceptions
    assert fakes.pypi.__dict__.get("retry_policy") is None  # not the class's


@pytest.mark.asyncio
async def test_no_retries_means_no_policy_is_set(fakes):
    await make(retries=0).load_workflow_config(None)
    assert fakes.pypi.instances[0].retry_policy is None


@pytest.mark.asyncio
async def test_concurrent_loads_of_one_type_keep_their_own_policy(fakes):
    await asyncio.gather(
        make(name="a", retries=2).load_workflow_config(None),
        make(name="b", retries=5).load_workflow_config(None),
    )

    assert sorted(i.retry_policy.max_attempts for i in fakes.pypi.instances) == [2, 5]


@pytest.mark.asyncio
async def test_a_loader_that_fails_quietly_gets_a_message(fakes):
    """The loaders log the reason and return only (False, None): the caller got a result with
    nothing in it."""
    fakes.pypi.outcome = "fail"

    result = await make().load_workflow_config(None)

    assert result.error is True
    assert (
        "Failed to load workflow source 'src'" in result.content
        and "log" in result.content
    )


@pytest.mark.asyncio
async def test_a_reason_the_loader_gave_is_kept(fakes):
    fakes.pypi.outcome = "fail-with-reason"
    assert (await make().load_workflow_config(None)).content == "bad credentials"


@pytest.mark.asyncio
async def test_a_loader_that_raises_is_a_failed_result_not_an_exception(fakes, caplog):
    fakes.pypi.outcome = "raise"

    with caplog.at_level(logging.ERROR):
        result = await make().load_workflow_config(None)

    assert result.error is True and "loader exploded" in result.content
    assert result.task_id == "src" and result.event_name == "src"
    assert "raised an unexpected error" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["pypi", "hub"])
async def test_a_source_that_cannot_be_loaded_does_not_reach_the_loader(fakes, kind):
    result = await make(source_type=kind, version=None).load_workflow_config(None)

    assert result.error is True and "needs a version" in result.content
    assert getattr(fakes, kind).instances == []


@pytest.mark.asyncio
async def test_a_missing_local_directory_does_not_reach_the_loader(fakes, tmp_path):
    result = await make(
        source_type="local", location=tmp_path / "gone", version=None
    ).load_workflow_config(None)

    assert (
        result.error is True
        and "not found" in result.content
        and fakes.local.instances == []
    )


@pytest.mark.asyncio
async def test_a_source_type_with_no_loader_is_a_configuration_error(monkeypatch):
    monkeypatch.delitem(source_module._LOADER_REGISTRY, RegistrySource.GIT)
    with pytest.raises(ImproperlyConfigured, match="No loader registered"):
        await make(source_type="git", version=None).load_workflow_config(None)


# --- for real: local and git sources, and a registry built from them --------------------------------------------------


def write_workflow(directory: Path, name: str, extra: str = "") -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "__init__.py").write_text("")
    class_name = "Flow" + uuid.uuid4().hex[:8]
    (directory / "workflow.py").write_text(
        textwrap.dedent(
            f"""
            from volnux.engine.workflows import WorkflowConfig

            class {class_name}(WorkflowConfig):
                name = "{name}"
                {extra}

                def ready(self):
                    pass
            """
        )
    )


@pytest.fixture
def registry(tmp_path):
    return WorkflowRegistry(cache_dir=tmp_path / "cache")


@pytest.mark.asyncio
async def test_a_local_source_registers_its_workflow(registry, tmp_path):
    """No local source could be loaded: the loader's `workflow_dir` argument was never given."""
    write_workflow(tmp_path / "wf" / "billing", "billing")
    source = WorkflowSource(
        name="billing", source_type="local", location=tmp_path / "wf" / "billing"
    )

    result = await source.load_workflow_config(
        registry, {"cache_dir": tmp_path / "cache"}
    )

    assert result.error is False
    assert [w.name for w in registry.get_workflow_configs()] == ["billing"]
    assert registry.get_workflow_config("billing").path == tmp_path / "wf" / "billing"


@pytest.mark.asyncio
async def test_a_local_directory_without_a_workflow_is_an_error_result(
    registry, tmp_path
):
    """The local loader answered True whether or not it found a workflow."""
    empty = tmp_path / "wf" / "empty"
    empty.mkdir(parents=True)
    source = WorkflowSource(name="empty", source_type="local", location=empty)

    result = await source.load_workflow_config(registry, {})

    assert result.error is True and registry.get_workflow_configs() == []


@pytest.mark.asyncio
async def test_a_workflow_file_with_no_config_class_is_an_error_result(
    registry, tmp_path
):
    folder = tmp_path / "wf" / "plain"
    folder.mkdir(parents=True)
    (folder / "__init__.py").write_text("")
    (folder / "workflow.py").write_text("x = 1\n")

    result = await WorkflowSource(
        name="plain", source_type="local", location=folder
    ).load_workflow_config(registry, {})

    assert result.error is True


@pytest.mark.asyncio
async def test_the_registry_loads_every_local_workflow_it_finds(registry, tmp_path):
    project = tmp_path / "project"
    for name in ("alpha", "beta", "gamma"):
        write_workflow(project / "workflows" / name, name)
    await registry.populate_local_workflow_configs(project)

    await registry.load_workflow_configs()

    assert sorted(w.name for w in registry.get_workflow_configs()) == [
        "alpha",
        "beta",
        "gamma",
    ]
    assert registry.is_ready()


@pytest.mark.asyncio
async def test_one_broken_workflow_does_not_stop_the_others(registry, tmp_path):
    project = tmp_path / "project"
    write_workflow(project / "workflows" / "good", "good")
    (project / "workflows" / "bad").mkdir(parents=True)
    (project / "workflows" / "bad" / "__init__.py").write_text("")
    (project / "workflows" / "bad" / "workflow.py").write_text(
        "raise RuntimeError('broken workflow')\n"
    )
    await registry.populate_local_workflow_configs(project)

    await registry.load_workflow_configs()

    assert [w.name for w in registry.get_workflow_configs()] == ["good"]
    assert registry.is_ready()


@pytest.mark.asyncio
async def test_one_workflow_loads_only_once_per_name(registry, tmp_path):
    write_workflow(tmp_path / "wf" / "billing", "billing")
    source = WorkflowSource(
        name="billing", source_type="local", location=tmp_path / "wf" / "billing"
    )
    await source.load_workflow_config(registry, {})

    second = await source.load_workflow_config(registry, {})

    assert second.error is True  # the registry refuses a second workflow of that name
    assert len(registry.get_workflow_configs()) == 1


needs_git = pytest.mark.skipif(
    shutil.which("git") is None, reason="git is not installed"
)


def git(repo: Path, *args: str) -> str:
    env = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
        "PATH": os.environ["PATH"],
        "HOME": str(repo.parent),
    }
    return subprocess.run(
        ["git", *args], cwd=repo, env=env, check=True, capture_output=True, text=True
    ).stdout.strip()


@needs_git
@pytest.mark.asyncio
async def test_a_git_source_clones_and_registers_the_workflow_at_the_pinned_ref(
    registry, tmp_path
):
    """The source's 30 000 ms went to git as 30 000 s, and its version never became the ref."""
    remote = tmp_path / "remote"
    remote.mkdir()
    git(remote, "init", "-q", "-b", "main")
    write_workflow(remote / "billing", "billing_v1")
    git(remote, "add", "-A")
    git(remote, "commit", "-q", "-m", "one")
    git(remote, "tag", "v1.0")
    write_workflow(remote / "billing", "billing_v2")
    git(remote, "add", "-A")
    git(remote, "commit", "-q", "-m", "two")

    source = WorkflowSource(
        name="billing", source_type="git", location=str(remote), version="v1.0"
    )
    result = await source.load_workflow_config(
        registry, {"cache_dir": tmp_path / "cache"}
    )

    assert result.error is False
    assert [w.name for w in registry.get_workflow_configs()] == [
        "billing_v1"
    ]  # the tag, not main


@needs_git
@pytest.mark.asyncio
async def test_a_git_source_without_a_cache_dir_fails_clearly(registry, tmp_path):
    remote = tmp_path / "remote"
    remote.mkdir()
    git(remote, "init", "-q", "-b", "main")
    write_workflow(remote / "billing", "billing")
    git(remote, "add", "-A")
    git(remote, "commit", "-q", "-m", "one")

    result = await WorkflowSource(
        name="billing", source_type="git", location=str(remote)
    ).load_workflow_config(registry, {})

    assert result.error is True and registry.get_workflow_configs() == []


@pytest.mark.asyncio
async def test_a_pypi_source_that_pip_cannot_find_is_an_error_result(
    registry, tmp_path, monkeypatch
):
    monkeypatch.setenv("PIP_NO_INDEX", "1")  # offline: nothing can be found
    monkeypatch.setenv("PIP_DISABLE_PIP_VERSION_CHECK", "1")
    source = WorkflowSource(
        name="nothing",
        source_type="pypi",
        location="volnux-no-such-package-xyz",
        version="9.9.9",
        retries=0,
    )

    result = await source.load_workflow_config(registry, {})

    assert result.error is True and registry.get_workflow_configs() == []
