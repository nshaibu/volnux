"""A Volnux project has a Django-style settings.py, found before anything reads
the settings."""

import importlib.util
import os
import sys
import uuid
from pathlib import Path

import pytest

from volnux.cli._bootstrap import (
    ENVIRONMENT_VARIABLE,
    configure_settings_module,
    find_project_dir,
)
from volnux.cli.command.builtins.init_project import InitProjectCommand
from volnux.cli.command.mixins.project import ProjectMixin
from volnux.exceptions import CommandError


def forget_settings_module(monkeypatch):
    """Start without VOLNUX_SETTINGS_MODULE and put it back afterwards, even if
    the code under test sets it itself (monkeypatch only undoes what it changed)."""
    monkeypatch.setenv(ENVIRONMENT_VARIABLE, "placeholder")
    monkeypatch.delenv(ENVIRONMENT_VARIABLE)
    monkeypatch.setattr(sys, "path", list(sys.path))


@pytest.fixture
def clean(monkeypatch):
    forget_settings_module(monkeypatch)


def make_project(root: Path, settings=True, legacy=False, init=True) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    if init:
        (root / "init.py").write_text("")
    if settings:
        (root / "settings.py").write_text("DEBUG = False\n")
    if legacy:
        (root / "config.py").write_text("PROJECT_DIR = None\n")
    return root


class TestFindingTheProject:
    def test_in_the_project_directory(self, tmp_path):
        project = make_project(tmp_path / "p")
        assert find_project_dir(project) == project

    def test_from_a_sub_directory(self, tmp_path):
        project = make_project(tmp_path / "p")
        sub = project / "workflows" / "deep"
        sub.mkdir(parents=True)
        assert find_project_dir(sub) == project

    @pytest.mark.parametrize(
        "kwargs",
        [dict(init=False), dict(settings=False), dict(settings=False, legacy=True)],
    )
    def test_it_needs_init_py_and_settings_py(self, tmp_path, kwargs):
        # (a legacy config.py is not settings: nothing is read from it)
        project = make_project(tmp_path / "p", **kwargs)
        assert find_project_dir(project) is None

    def test_outside_any_project(self, tmp_path):
        assert find_project_dir(tmp_path) is None


class TestSettingsModuleFromTheCli:
    def test_sets_the_variable_and_the_path(self, tmp_path, clean):
        project = make_project(tmp_path / "p")
        assert configure_settings_module(project) == "settings"
        assert os.environ[ENVIRONMENT_VARIABLE] == "settings"
        assert sys.path[0] == str(project)

    def test_from_a_sub_directory(self, tmp_path, clean):
        project = make_project(tmp_path / "p")
        sub = project / "workflows"
        sub.mkdir()
        configure_settings_module(sub)
        assert sys.path[0] == str(project)

    def test_the_environment_wins(self, tmp_path, clean, monkeypatch):
        project = make_project(tmp_path / "p")
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "myproject.settings")
        assert configure_settings_module(project) is None
        assert os.environ[ENVIRONMENT_VARIABLE] == "myproject.settings"
        assert str(project) not in sys.path

    def test_nothing_happens_outside_a_project(self, tmp_path, clean):
        before = list(sys.path)
        assert configure_settings_module(tmp_path) is None
        assert ENVIRONMENT_VARIABLE not in os.environ and sys.path == before

    def test_the_path_is_not_added_twice(self, tmp_path, clean):
        project = make_project(tmp_path / "p")
        sys.path.insert(0, str(project))
        configure_settings_module(project)
        assert sys.path.count(str(project)) == 1

    def test_the_project_settings_are_then_what_the_config_reads(
        self, tmp_path, clean, monkeypatch
    ):
        from volnux.config import VolnuxConfig

        project = make_project(tmp_path / "p")
        (project / "settings.py").write_text("API_PORT = 7777\n")
        monkeypatch.delitem(sys.modules, "settings", raising=False)
        configure_settings_module(project)
        monkeypatch.setattr(
            VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
        )
        assert VolnuxConfig().get("API_PORT") == 7777
        monkeypatch.delitem(sys.modules, "settings", raising=False)

    def test_importing_the_cli_package_runs_it(self):
        import volnux.cli as cli

        assert "configure_settings_module" in vars(cli)


class TestScaffold:
    @pytest.fixture
    def project(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        InitProjectCommand().handle(name="shop", path=None, force=False)
        return tmp_path / "shop"

    def load(self, path: Path):
        spec = importlib.util.spec_from_file_location(
            f"scaffold_{uuid.uuid4().hex[:6]}", path
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_it_writes_settings_py_not_config_py(self, project):
        assert (project / "settings.py").is_file()
        assert not (project / "config.py").exists()
        assert (project / "init.py").is_file()

    def test_the_settings_are_valid_and_django_like(self, project):
        module = self.load(project / "settings.py")
        assert module.PROJECT_NAME == "shop"
        assert module.BASE_DIR == project.resolve()
        assert module.PROJECT_DIR == module.BASE_DIR
        assert module.DEBUG is False
        assert module.WORKFLOWS == []
        assert set(module.DATABASES) == {"default", "triggers"}
        assert module.DATABASES["default"]["ENGINE"].endswith(
            "InMemoryKeyValueStoreBackend"
        )

    def test_the_trigger_database_is_inside_the_project(self, project):
        module = self.load(project / "settings.py")
        assert Path(module.DATABASES["triggers"]["NAME"]).parent == project.resolve()

    def test_no_secret_is_written(self, project):
        text = (project / "settings.py").read_text()
        assert "SECRET_KEY = None" in text and "# SECRET_KEY" in text
        assert 'PASSWORD": "..."' in text  # an example, commented out
        assert not [l for l in text.splitlines() if l.startswith("PASSWORD")]

    def test_the_text_has_no_unrendered_placeholders(self, project):
        text = (project / "settings.py").read_text()
        assert "{project_name}" not in text and "{version}" not in text
        assert "{{" not in text

    def test_the_project_is_found_and_its_settings_load(self, project, monkeypatch):
        from volnux.config import VolnuxConfig

        monkeypatch.chdir(project)
        forget_settings_module(monkeypatch)
        monkeypatch.delitem(sys.modules, "settings", raising=False)
        assert configure_settings_module() == "settings"
        monkeypatch.setattr(
            VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
        )
        config = VolnuxConfig()
        assert config.get("PROJECT_NAME") == "shop"
        assert config.get("DEBUG") is False
        monkeypatch.delitem(sys.modules, "settings", raising=False)

    def test_an_existing_project_is_not_overwritten(self, project):
        from volnux.exceptions import CommandError as Error

        with pytest.raises(Error):
            InitProjectCommand().handle(name="shop", path=None, force=False)


class Command(ProjectMixin):
    """The mixin's needs from a command."""

    def __init__(self):
        self.warnings = []

    def warning(self, message):
        self.warnings.append(message)

    def success(self, message):
        pass


class TestProjectDetection:
    def test_settings_py_marks_a_project(self, tmp_path, monkeypatch):
        project = make_project(tmp_path / "p")
        monkeypatch.chdir(project)
        assert Command().resolve_project_dir() == project.resolve()

    def test_a_legacy_config_py_still_marks_one(self, tmp_path, monkeypatch):
        project = make_project(tmp_path / "p", settings=False, legacy=True)
        monkeypatch.chdir(project)
        assert Command().resolve_project_dir() == project.resolve()

    def test_no_project(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with pytest.raises(CommandError, match="No Volnux project"):
            Command().resolve_project_dir()

    def test_the_project_configuration_is_settings_py(self, tmp_path, monkeypatch):
        project = make_project(tmp_path / "p")
        (project / "settings.py").write_text("PROJECT_DIR = 'here'\n")
        monkeypatch.chdir(project)
        command = Command()
        module = command.load_project_config()
        assert module.PROJECT_DIR == "here" and command.warnings == []

    def test_a_legacy_config_py_is_loaded_with_a_warning(self, tmp_path, monkeypatch):
        project = make_project(tmp_path / "p", settings=False, legacy=True)
        monkeypatch.chdir(project)
        command = Command()
        assert command.load_project_config() is not None
        assert any("rename it settings.py" in w for w in command.warnings)

    def test_settings_py_wins_over_a_legacy_file(self, tmp_path, monkeypatch):
        project = make_project(tmp_path / "p", legacy=True)
        (project / "settings.py").write_text("PROJECT_DIR = 'new'\n")
        monkeypatch.chdir(project)
        assert Command().load_project_config().PROJECT_DIR == "new"

    def test_neither_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        command = Command()
        assert command.load_project_config() is None
        assert any("No settings.py" in w for w in command.warnings)

    def test_a_syntax_error_names_the_file(self, tmp_path, monkeypatch):
        project = make_project(tmp_path / "p")
        (project / "settings.py").write_text("DEBUG = (\n")
        monkeypatch.chdir(project)
        with pytest.raises(CommandError, match="settings.py"):
            Command().load_project_config()
