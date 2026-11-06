"""volnux.conf: settings the way Django has them."""

import sys
import uuid

import pytest

from volnux.conf import (
    ENVIRONMENT_VARIABLE,
    REMOVED_SETTINGS,
    override_settings,
    settings,
)
from volnux.config import VolnuxConfig, setting
from volnux.exceptions import ImproperlyConfigured


@pytest.fixture
def shared(isolated, monkeypatch):
    """The shared configuration is a fresh one with these defaults."""

    def make(defaults=None, **kw):
        cfg = isolated(defaults, **kw)
        monkeypatch.setattr(VolnuxConfig, "_instance", cfg)
        return cfg

    return make


@pytest.fixture
def module(monkeypatch, isolated):
    """Write a settings module that can be imported, under a unique name."""

    def write(source, name=None):
        name = name or f"proj_settings_{uuid.uuid4().hex[:8]}"
        (isolated.tmp_path / f"{name}.py").write_text(source)
        monkeypatch.syspath_prepend(str(isolated.tmp_path))
        monkeypatch.delitem(sys.modules, name, raising=False)
        return name

    return write


class TestSettingsObject:
    def test_reads_a_setting(self, shared):
        shared({"API_PORT": 9})
        assert settings.API_PORT == 9

    def test_an_unknown_setting_is_an_attribute_error(self, shared):
        shared({})
        with pytest.raises(AttributeError, match="NOPE"):
            settings.NOPE

    def test_only_upper_case_names_are_settings(self, shared):
        shared({"API_PORT": 9})
        with pytest.raises(AttributeError, match="UPPER_CASE"):
            settings.api_port

    def test_it_is_read_only(self, shared):
        shared({})
        with pytest.raises(AttributeError, match="read-only"):
            settings.API_PORT = 1

    def test_get_with_a_default(self, shared):
        shared({"A": 1})
        assert settings.get("A") == 1 and settings.get("B", 7) == 7
        assert settings.get("B") is None

    def test_membership_and_dir(self, shared):
        shared({"A": 1, "B": None})
        assert "A" in settings and "NOPE" not in settings
        assert {"A", "B"} <= set(dir(settings))

    def test_the_environment_is_read_live(self, shared, monkeypatch):
        shared({"API_PORT": 9})
        monkeypatch.setenv("VOLNUX_API_PORT", "10")
        assert settings.API_PORT == 10

    def test_the_real_defaults(self):
        assert settings.DEBUG is False
        assert settings.SECRET_KEY is None
        assert set(settings.DATABASES) == {"default", "triggers"}


class TestOverrideSettings:
    def test_as_a_context_manager(self, shared):
        shared({"A": 1})
        with override_settings(A=2, NEW=3):
            assert settings.A == 2 and settings.NEW == 3
        assert settings.A == 1 and "NEW" not in settings

    def test_as_a_decorator(self, shared):
        shared({"A": 1})

        @override_settings(A=5)
        def inside():
            return settings.A

        assert inside() == 5 and settings.A == 1

    def test_they_nest(self, shared):
        shared({"A": 1})
        with override_settings(A=2):
            with override_settings(A=3):
                assert settings.A == 3
            assert settings.A == 2
        assert settings.A == 1

    def test_restored_when_the_block_raises(self, shared):
        shared({"A": 1})
        with pytest.raises(RuntimeError):
            with override_settings(A=2):
                raise RuntimeError
        assert settings.A == 1

    def test_everything_that_reads_a_setting_sees_it(self, shared, monkeypatch):
        cfg = shared({"A": 1})
        monkeypatch.setenv("VOLNUX_A", "7")
        with override_settings(A=2):
            # even the environment does not beat an override (as in a test)
            assert settings.A == 2 and setting("A") == 2 and cfg.get("A") == 2
            assert cfg.resolve("a") == 2

    def test_names_are_case_insensitive(self, shared):
        shared({"A": 1})
        with override_settings(a=4):
            assert settings.A == 4

    def test_a_removed_setting_is_refused(self, shared):
        shared({})
        with pytest.raises(ImproperlyConfigured, match="DATABASES"):
            with override_settings(KEY_VALUE_STORE_CONFIG={}):
                pass


class TestConfigure:
    def test_sets_settings_from_code(self, shared):
        shared({"A": 1})
        settings.configure(A=2, B=3)
        assert (settings.A, settings.B) == (2, 3)

    def test_only_upper_case(self, shared):
        shared({})
        with pytest.raises(AttributeError):
            settings.configure(lower=1)

    def test_removed_names_are_refused(self, shared):
        shared({})
        with pytest.raises(ImproperlyConfigured, match="SECRET_KEY"):
            settings.configure(JWT_SECRET="x")


class TestSettingsModule:
    def test_the_variable_name(self):
        assert ENVIRONMENT_VARIABLE == "VOLNUX_SETTINGS_MODULE"

    def test_a_project_module_overrides_the_defaults(
        self, isolated, module, monkeypatch
    ):
        name = module("API_PORT = 9001\nDEBUG = True\n")
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, name)
        cfg = isolated({"API_PORT": 8080, "OTHER": "kept", "DEBUG": False})
        assert (cfg.get("API_PORT"), cfg.get("DEBUG"), cfg.get("OTHER")) == (
            9001,
            True,
            "kept",
        )

    def test_precedence(self, isolated, module, monkeypatch):
        # defaults < settings module < VOLNUX_CONFIG file < explicit file < environment
        name = module(
            "A = 'module'\nB = 'module'\nC = 'module'\nD = 'module'\nE = 'module'\n"
        )
        env_file = isolated.tmp_path / "env_file.py"
        env_file.write_text(
            "B = 'env file'\nC = 'env file'\nD = 'env file'\nE = 'env file'\n"
        )
        explicit = isolated.tmp_path / "explicit.py"
        explicit.write_text("C = 'explicit'\nD = 'explicit'\nE = 'explicit'\n")
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, name)
        monkeypatch.setenv("VOLNUX_CONFIG", str(env_file))
        monkeypatch.setenv("VOLNUX_D", "environment")
        monkeypatch.setenv("VOLNUX_E", "environment")
        cfg = isolated(
            {"A": "d", "B": "d", "C": "d", "D": "d", "E": "d", "F": "d"},
            config_file=str(explicit),
        )
        assert [cfg.get(k) for k in "ABCDEF"] == [
            "module",
            "env file",
            "explicit",
            "environment",
            "environment",
            "d",
        ]

    def test_a_named_module_replaces_the_search_for_settings_py(
        self, isolated, module, monkeypatch
    ):
        found = isolated.tmp_path / "found_settings.py"
        found.write_text("FROM = 'discovered file'\n")
        monkeypatch.setattr(
            VolnuxConfig,
            "_search_for_config_file_in_dir",
            staticmethod(lambda: str(found)),
        )
        assert isolated({}).get("FROM") == "discovered file"  # no module named

        monkeypatch.setenv(ENVIRONMENT_VARIABLE, module("OTHER = 1\n"))
        cfg = isolated({})
        assert cfg.get("FROM", "absent") == "absent"

    def test_only_upper_case_names(self, isolated, module, monkeypatch):
        monkeypatch.setenv(
            ENVIRONMENT_VARIABLE, module("REAL = 1\nhelper = 2\n_x = 3\n")
        )
        cfg = isolated({})
        assert cfg.get("REAL") == 1
        assert cfg.get("helper", "absent") == "absent"

    def test_the_variable_is_not_a_setting(self, isolated, module, monkeypatch):
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, module("A = 1\n"))
        assert isolated({}).get("SETTINGS_MODULE", "absent") == "absent"

    def test_a_module_that_cannot_be_imported_is_an_error(self, isolated, monkeypatch):
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, "no_such_settings_module_xyz")
        with pytest.raises(ImproperlyConfigured, match="no_such_settings_module_xyz"):
            isolated({})

    def test_a_module_that_fails_is_an_error_naming_it(
        self, isolated, module, monkeypatch
    ):
        name = module("raise RuntimeError('boom')\n")
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, name)
        with pytest.raises(ImproperlyConfigured, match=f"{name}.*boom"):
            isolated({})

    def test_the_shared_instance_reads_it(self, isolated, module, monkeypatch):
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, module("API_PORT = 4242\n"))
        monkeypatch.setattr(VolnuxConfig, "_instance", None)
        monkeypatch.setattr(
            VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
        )
        assert settings.API_PORT == 4242


class TestRemovedSettings:
    def test_every_removed_name_names_its_replacement(self):
        assert len(REMOVED_SETTINGS) >= 10
        for old, new in REMOVED_SETTINGS.items():
            assert old.isupper() and new

    @pytest.mark.parametrize("old, new", sorted(REMOVED_SETTINGS.items()))
    def test_from_a_settings_module(self, isolated, module, monkeypatch, old, new):
        name = module(f"{old} = 1\n")
        monkeypatch.setenv(ENVIRONMENT_VARIABLE, name)
        with pytest.raises(ImproperlyConfigured) as exc:
            isolated({})
        message = str(exc.value)
        assert old in message and new.split()[0] in message and name in message

    @pytest.mark.parametrize("old, new", sorted(REMOVED_SETTINGS.items()))
    def test_from_a_settings_file(self, isolated, old, new):
        path = isolated.tmp_path / "old.py"
        path.write_text(f"{old} = 1\n")
        with pytest.raises(ImproperlyConfigured, match=old):
            isolated({}, config_file=str(path))

    @pytest.mark.parametrize("old, new", sorted(REMOVED_SETTINGS.items()))
    def test_from_the_environment(self, isolated, monkeypatch, old, new):
        monkeypatch.setenv(f"VOLNUX_{old}", "x")
        with pytest.raises(ImproperlyConfigured) as exc:
            isolated({})
        assert f"VOLNUX_{old}" in str(exc.value) and new.split()[0] in str(exc.value)

    def test_a_broken_file_is_still_only_logged(self, isolated, caplog):
        # only the renamed settings are fatal; other file problems stay logged
        path = isolated.tmp_path / "bad.py"
        path.write_text("raise RuntimeError('boom')\n")
        assert isolated({"A": 1}, config_file=str(path)).get("A") == 1

    def test_the_shipped_defaults_use_none_of_them(self):
        import volnux.conf.global_settings as defaults

        assert not {n for n in vars(defaults) if n.isupper()} & set(REMOVED_SETTINGS)

    def test_the_old_names_in_the_messages_are_the_ones_users_wrote(self):
        for old in (
            "KEY_VALUE_STORE_CONFIG",
            "STREAM_BACKEND_CONFIG",
            "TRIGGER_STATE_BACKEND_CONFIG",
            "SAGA_DLQ_BACKEND_CONFIG",
            "ENV",
            "JWT_SECRET",
            "LOGGING_CONFIG",
            "PROJECT_ROOT_DIR",
            "API_CORS_ORIGINS",
            "DEFAULT_EVENT_VERSIONING",
        ):
            assert old in REMOVED_SETTINGS
