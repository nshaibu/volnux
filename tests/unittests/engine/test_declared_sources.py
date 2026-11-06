from pathlib import Path

import pytest

from volnux.engine.workflows.declarative_sources import (
    SourceDeclarationError,
    build_source,
    build_sources,
)
from volnux.engine.workflows.registry import WorkflowRegistry
from volnux.engine.workflows.source import RegistrySource, WorkflowSource
from volnux.engine.workflows.workflow import WorkflowConfig
from volnux.parser import pointy_parser


def build(source, base=None):
    return build_sources(pointy_parser(source + "\nA"), base)


class TestBuild:
    def test_hub(self):
        (s,) = build(
            '@source: hub("trading_events", "trading-events", version="2.1.0", '
            'timeout=5000, retries=1, priority=3, events=["Validate"], '
            'eventhub_url="https://hub")'
        )
        assert (s.name, s.source_type, s.location) == (
            "trading_events",
            RegistrySource.HUB,
            "trading-events",
        )
        assert (s.version, s.timeout, s.retries, s.priority) == ("2.1.0", 5000, 1, 3)
        assert s.events == ["Validate"]
        assert s.metadata == {"eventhub_url": "https://hub"}

    def test_git_and_its_github_alias(self):
        a, b = build(
            '@source: git("n", "https://x/y.git", version="1.5.0", branch="dev")\n'
            '@source: github("m", "https://github.com/x/y.git", as_package=true)'
        )
        assert a.source_type is b.source_type is RegistrySource.GIT
        assert a.version == "1.5.0" and a.metadata == {"branch": "dev"}
        assert b.metadata == {"as_package": True}

    def test_pypi(self):
        (s,) = build(
            '@source: pypi("utils", "shared-utils", version="v3.0.0", '
            'index_url="https://pypi.example/simple")'
        )
        assert s.source_type is RegistrySource.PYPI
        assert s.metadata == {"index_url": "https://pypi.example/simple"}

    def test_location_keyword(self):
        (s,) = build('@source: local("shared", location="/abs/shared")')
        assert s.location == "/abs/shared"

    def test_relative_local_location_follows_the_workflow_directory(self):
        (s,) = build('@source: local("shared", "../shared")', Path("/wf/trading"))
        assert s.source_type is RegistrySource.LOCAL
        assert Path(s.location) == Path("/wf/trading/../shared")

    def test_absolute_local_location_is_kept(self):
        (s,) = build('@source: local("shared", "/abs/shared")', Path("/wf"))
        assert s.location == "/abs/shared"

    def test_order_is_declaration_order(self):
        sources = build(
            '@source: local("b", "/b")\n@source: local("a", "/a")\n@source: local("c", "/c")'
        )
        assert [s.name for s in sources] == ["b", "a", "c"]

    def test_token_from_env_becomes_credentials(self, monkeypatch):
        monkeypatch.setenv("GIT_TOKEN", "t0k")
        (s,) = build('@source: git("n", "u", token=$env.GIT_TOKEN)')
        assert s.credentials.token == "t0k"
        assert "t0k" not in repr(s)

    def test_env_values_are_read_when_built(self, monkeypatch):
        monkeypatch.setenv("PKG", "from-env")
        monkeypatch.setenv("VER", "9.9.9")
        (s,) = build('@source: hub("n", $env.PKG, version=$env.VER)')
        assert (s.location, s.version) == ("from-env", "9.9.9")

    def test_no_sources(self):
        assert build("") == []


class TestErrors:
    @pytest.mark.parametrize(
        "source, fragment",
        [
            ('@source: hub("a", "x")', "version"),  # WorkflowSource refuses later
            ('@source: git("a", "x", timeout=0)', "timeout"),
            ('@source: hub("trading-events", "x", version="1")', "namespace"),
            ('@source: git("a", "x", bogus=1)', "unknown argument"),
            ('@source: git("a")', "location"),
            ('@source: git("a", "x", events="p")', "events"),
        ],
    )
    def test_invalid(self, source, fragment):
        if "version" in fragment:
            # missing version is reported when loading, not when describing
            (s,) = build(source)
            assert (
                s.missing_requirement() and "needs a version" in s.missing_requirement()
            )
            return
        with pytest.raises(SourceDeclarationError, match=fragment):
            build(source)

    def test_error_names_the_declaration_and_line(self):
        with pytest.raises(SourceDeclarationError, match=r"@source: git \(line 2\)"):
            build('@version: "1"\n@source: git("a", "x", timeout=0)')

    def test_unset_env_location(self, monkeypatch):
        monkeypatch.delenv("PKG", raising=False)
        with pytest.raises(SourceDeclarationError, match="location"):
            build('@source: hub("a", $env.PKG, version="1")')


class StubWorkflow(WorkflowConfig):
    name = "stub"

    def ready(self):
        pass

    def get_pointy_ast(self):
        return self.program


@pytest.fixture
def registry(tmp_path):
    return WorkflowRegistry(cache_dir=tmp_path / "cache")


def workflow(source, registry, tmp_path):
    wf = StubWorkflow(tmp_path)
    wf.program = pointy_parser(source + "\nA")
    wf._registry = registry
    return wf


class TestRegisterDeclaredSources:
    def test_registers_each_source_with_the_registry(self, registry, tmp_path):
        wf = workflow(
            '@source: hub("trading_events", "trading-events", version="2.1.0")\n'
            '@source: local("shared", "./shared")',
            registry,
            tmp_path,
        )
        sources = wf.register_declared_sources()
        assert [s.name for s in sources] == ["trading_events", "shared"]
        assert registry.get_workflow_source("trading_events").version == "2.1.0"
        assert registry.get_workflow_source("shared").location == str(
            tmp_path / "shared"
        )

    def test_nothing_is_registered_when_a_declaration_is_invalid(
        self, registry, tmp_path
    ):
        wf = workflow(
            '@source: local("ok", "./ok")\n@source: git("bad", "x", timeout=0)',
            registry,
            tmp_path,
        )
        with pytest.raises(SourceDeclarationError):
            wf.register_declared_sources()
        assert registry.get_workflow_source("ok") is None

    def test_nothing_is_registered_when_a_namespace_is_taken(self, registry, tmp_path):
        registry.add_workflow_source(
            WorkflowSource(name="shared", source_type="local", location="/elsewhere")
        )
        wf = workflow(
            '@source: local("fresh", "./f")\n@source: local("shared", "./s")',
            registry,
            tmp_path,
        )
        with pytest.raises(ValueError, match="already registered: shared"):
            wf.register_declared_sources()
        assert registry.get_workflow_source("fresh") is None
        assert registry.get_workflow_source("shared").location == "/elsewhere"

    def test_a_workflow_without_sources(self, registry, tmp_path):
        assert workflow("", registry, tmp_path).register_declared_sources() == []
