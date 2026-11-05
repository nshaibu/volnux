import logging
import threading
from pathlib import Path

import pytest

from volnux.config import VolnuxConfig


def write(path, text):
    path.write_text(text)
    return str(path)


class TestPrecedence:
    def test_environment_beats_the_defaults(self, isolated, monkeypatch):
        # it used to be overwritten by the default, so no default could be overridden
        monkeypatch.setenv("VOLNUX_MAX_RETRIES", "9")
        assert isolated({"MAX_RETRIES": 5}).get("MAX_RETRIES") == 9

    def test_a_file_beats_the_defaults(self, isolated):
        path = write(isolated.tmp_path / "s.py", "MAX_RETRIES = 7")
        assert isolated({"MAX_RETRIES": 5}, config_file=path).get("MAX_RETRIES") == 7

    def test_environment_beats_a_file(self, isolated, monkeypatch):
        monkeypatch.setenv("VOLNUX_MAX_RETRIES", "9")
        path = write(isolated.tmp_path / "s.py", "MAX_RETRIES = 7")
        assert isolated({"MAX_RETRIES": 5}, config_file=path).get("MAX_RETRIES") == 9

    def test_settings_nobody_overrides_keep_their_default(self, isolated):
        assert isolated({"A": 1, "B": 2}).get("B") == 2

    def test_later_files_beat_earlier_ones(self, isolated, monkeypatch):
        first = write(isolated.tmp_path / "first.py", "A = 'first'\nB = 'first'")
        second = write(isolated.tmp_path / "second.py", "A = 'second'")
        monkeypatch.setenv("VOLNUX_CONFIG", first)
        cfg = isolated({}, config_file=second)
        assert (cfg.get("A"), cfg.get("B")) == ("second", "first")


class TestEnvironmentTypes:
    @pytest.mark.parametrize(
        "default, raw, expected",
        [
            (5, "9", 9),
            (5, " 9 ", 9),
            (0.5, "1.25", 1.25),
            (True, "false", False),
            (True, "0", False),
            (False, "YES", True),
            (False, "on", True),
            ([1], "[1, 2]", [1, 2]),
            ({"a": 1}, '{"b": 2}', {"b": 2}),
            ((1,), "[3, 4]", (3, 4)),
            (Path("/a"), "/b/c", Path("/b/c")),
            ("text", "other", "other"),
        ],
    )
    def test_converted_to_the_type_of_the_setting(
        self, isolated, monkeypatch, default, raw, expected
    ):
        monkeypatch.setenv("VOLNUX_THING", raw)
        value = isolated({"THING": default}).get("THING")
        assert value == expected and type(value) is type(expected)

    def test_unknown_settings_stay_text(self, isolated, monkeypatch):
        monkeypatch.setenv("VOLNUX_NEW_THING", "12")
        assert isolated({}).get("NEW_THING") == "12"

    @pytest.mark.parametrize(
        "default, raw",
        [(5, "nine"), (True, "maybe"), ([1], '{"a": 1}'), ([1], "not json")],
    )
    def test_a_value_that_does_not_fit_is_kept_as_text_and_reported(
        self, isolated, monkeypatch, caplog, default, raw
    ):
        monkeypatch.setenv("VOLNUX_THING", raw)
        with caplog.at_level(logging.WARNING, logger="volnux.config"):
            assert isolated({"THING": default}).get("THING") == raw
        assert "VOLNUX_THING" in caplog.text

    def test_a_none_setting_takes_the_text(self, isolated, monkeypatch):
        monkeypatch.setenv("VOLNUX_THING", "x")
        assert isolated({"THING": None}).get("THING") == "x"

    @pytest.mark.parametrize("default", [None, "missing"])
    def test_json_objects_and_arrays_are_recognised_without_a_type(
        self, isolated, monkeypatch, default
    ):
        # a backend config is None until configured, and arrives as JSON
        monkeypatch.setenv("VOLNUX_BACKEND", '{"ENGINE": "x.Y", "n": 1}')
        monkeypatch.setenv("VOLNUX_ORIGINS", '["a", "b"]')
        defaults = {"BACKEND": None, "ORIGINS": None} if default is None else {}
        cfg = isolated(defaults)
        assert cfg.get("BACKEND") == {"ENGINE": "x.Y", "n": 1}
        assert cfg.get("ORIGINS") == ["a", "b"]

    @pytest.mark.parametrize("raw", ["{not json", "[abc", "{}x", "plain"])
    def test_text_that_only_looks_like_json_stays_text(
        self, isolated, monkeypatch, raw
    ):
        monkeypatch.setenv("VOLNUX_THING", raw)
        assert isolated({"THING": None}).get("THING") == raw

    def test_a_string_setting_is_never_parsed(self, isolated, monkeypatch):
        monkeypatch.setenv("VOLNUX_NAME", '["not", "a", "list"]')
        assert isolated({"NAME": "default"}).get("NAME") == '["not", "a", "list"]'


class TestEnvironmentSecrets:
    def test_the_private_key_never_becomes_a_setting(self, isolated, monkeypatch):
        monkeypatch.setenv("VOLNUX_MESH_PRIVATE_KEY", "TOP-SECRET-PEM")
        cfg = isolated({})
        assert cfg.get("MESH_PRIVATE_KEY", None) is None
        assert "TOP-SECRET-PEM" not in cfg.to_json()
        assert "TOP-SECRET-PEM" not in cfg.to_base64()

    def test_other_variables_still_load(self, isolated, monkeypatch):
        monkeypatch.setenv("VOLNUX_API_URL", "http://x")
        assert isolated({}).get("API_URL") == "http://x"

    def test_the_bare_prefix_is_ignored(self, isolated, monkeypatch):
        monkeypatch.setenv("VOLNUX_", "x")
        assert isolated({}).get("", None) is None


class TestFileFailures:
    def test_a_failing_file_does_not_stop_the_next(self, isolated, monkeypatch):
        bad = write(isolated.tmp_path / "bad.py", "raise RuntimeError('boom')")
        good = write(isolated.tmp_path / "good.py", "A = 'ok'")
        monkeypatch.setenv("VOLNUX_CONFIG", bad)
        assert isolated({}, config_file=good).get("A") == "ok"

    @pytest.mark.parametrize(
        "source",
        ["raise RuntimeError('boom')", "undefined_name + 1", "def broken(:\n"],
    )
    def test_any_error_in_a_file_is_logged_not_raised(self, isolated, caplog, source):
        # a NameError / ValueError used to escape the constructor, and with it the
        # import of everything that creates the shared instance
        path = write(isolated.tmp_path / "bad.py", source)
        with caplog.at_level(logging.ERROR, logger="volnux.config"):
            cfg = isolated({"A": 1}, config_file=path)
        assert cfg.get("A") == 1
        assert "bad.py" in caplog.text

    def test_every_failure_is_logged_even_if_a_later_file_loads(
        self, isolated, monkeypatch, caplog
    ):
        bad = write(isolated.tmp_path / "bad_one.py", "raise RuntimeError('first')")
        good = write(isolated.tmp_path / "good.py", "A = 1")
        monkeypatch.setenv("VOLNUX_CONFIG", bad)
        with caplog.at_level(logging.ERROR, logger="volnux.config"):
            isolated({}, config_file=good)
        assert "bad_one.py" in caplog.text

    def test_a_missing_file_is_logged_not_raised(self, isolated, caplog):
        with caplog.at_level(logging.ERROR, logger="volnux.config"):
            isolated({}, config_file=str(isolated.tmp_path / "missing.py"))
        assert "missing.py" in caplog.text


class TestEntryNames:
    def test_names_are_case_insensitive(self, isolated):
        from volnux.config import ConfigEntry

        cfg = isolated({})
        cfg.add_entry_config(ConfigEntry(value=1, name="lower_case"))
        assert cfg.get("lower_case") == 1 and cfg.get("LOWER_CASE") == 1

    def test_only_upper_case_names_are_settings(self, isolated):
        # as in Django: `from x import y` and helper variables are not settings
        path = write(
            isolated.tmp_path / "s.py",
            "Mixed_Case = 3\nhelper = 4\n_private = 5\nREAL = 6\n",
        )
        cfg = isolated({}, config_file=path)
        assert cfg.get("REAL") == 6
        for name in ("mixed_case", "helper", "_private"):
            assert cfg.get(name, "absent") == "absent"


class TestNamespaces:
    def test_add_and_get_in_a_namespace(self, isolated):
        cfg = isolated({})
        cfg.add("k", 1, namespace="ns")  # used to raise AttributeError
        assert cfg.get("k", namespace="ns") == 1

    def test_namespaces_are_separate_from_the_main_store_and_each_other(self, isolated):
        cfg = isolated({})
        cfg.add("k", "main")
        cfg.add("k", "one", namespace="one")
        cfg.add("k", "two", namespace="two")
        assert [cfg.get("k", namespace=n) for n in ("one", "two")] == ["one", "two"]
        assert cfg.get("k") == "main"

    def test_a_namespaced_key_is_not_in_the_main_store(self, isolated):
        cfg = isolated({})
        cfg.add("only_ns", 1, namespace="ns")
        assert cfg.get("only_ns", None) is None

    def test_reading_does_not_create_a_namespace(self, isolated):
        cfg = isolated({})
        assert cfg.get("k", None, namespace="nowhere") is None
        assert "nowhere" not in cfg._namespace_store
        assert cfg.to_dict()["namespace_store"] == {}

    def test_missing_key_in_a_namespace_raises_without_default(self, isolated):
        with pytest.raises(AttributeError):
            isolated({}).get("k", namespace="ns")

    @pytest.mark.asyncio
    async def test_async_helpers_take_a_namespace(self, isolated):
        cfg = isolated({})
        await cfg.add_async("k", 5, namespace="ns")
        assert await cfg.get_async("k", namespace="ns") == 5
        assert await cfg.get_async("k", None) is None


class TestConcurrency:
    def test_readers_and_writers_do_not_collide(self, isolated):
        cfg = isolated({})
        errors = []

        def writer(n):
            try:
                for i in range(300):
                    cfg.add(f"K{n}_{i}", i)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        def reader():
            try:
                for i in range(600):
                    cfg.get("K0_1", None)
                    cfg.get(f"K1_{i % 300}", None)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
        threads += [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert cfg.get("K3_299") == 299

    def test_instances_have_their_own_lock(self, isolated):
        assert isolated({})._lock is not isolated({})._lock


class TestSingleton:
    def test_get_instance_is_shared(self, isolated, monkeypatch):
        monkeypatch.setattr(VolnuxConfig, "_instance", None)
        assert VolnuxConfig.get_instance() is VolnuxConfig.get_instance()


class TestLoggingSettings:
    def test_other_libraries_loggers_are_not_silenced(self):
        from volnux.conf import global_settings as settings

        assert settings.LOGGING["disable_existing_loggers"] is False
