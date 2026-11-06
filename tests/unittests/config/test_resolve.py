import json
import logging

import pytest

import volnux.config as config_module
from volnux.config import VolnuxConfig, is_sensitive_setting, setting


class TestResolve:
    def test_the_environment_is_read_when_asked_not_when_built(
        self, isolated, monkeypatch
    ):
        cfg = isolated({"LIMIT": 5})
        monkeypatch.setenv("VOLNUX_LIMIT", "9")  # set after the config exists
        assert cfg.resolve("LIMIT") == 9
        assert cfg.get("LIMIT") == 5  # get() is unchanged: the build-time view

    def test_environment_beats_the_store(self, isolated, monkeypatch):
        monkeypatch.setenv("VOLNUX_LIMIT", "9")
        cfg = isolated({"LIMIT": 5}, config_file=None)
        cfg.add("limit", 7)  # a later write to the store
        assert cfg.resolve("LIMIT") == 9

    def test_without_the_environment_the_store_answers(self, isolated):
        assert isolated({"LIMIT": 5}).resolve("limit") == 5

    def test_the_default_is_used_only_when_nothing_defines_the_setting(self, isolated):
        cfg = isolated({"LIMIT": 5})
        assert cfg.resolve("LIMIT", 99) == 5
        assert cfg.resolve("OTHER", 99) == 99

    def test_a_missing_setting_without_a_default_raises(self, isolated):
        with pytest.raises(AttributeError, match="OTHER"):
            isolated({}).resolve("OTHER")

    def test_a_none_setting_is_defined(self, isolated):
        assert isolated({"ID": None}).resolve("ID", "fallback") is None

    @pytest.mark.parametrize(
        "default, raw, expected",
        [(5, "9", 9), (0.5, "2", 2.0), (True, "no", False), ([1], "[2]", [2])],
    )
    def test_environment_values_take_the_type_of_the_setting(
        self, isolated, monkeypatch, default, raw, expected
    ):
        cfg = isolated({"THING": default})
        monkeypatch.setenv("VOLNUX_THING", raw)
        value = cfg.resolve("THING")
        assert value == expected and type(value) is type(expected)

    def test_the_type_can_come_from_the_default_when_the_store_has_none(
        self, isolated, monkeypatch
    ):
        cfg = isolated({})
        monkeypatch.setenv("VOLNUX_THING", "42")
        assert cfg.resolve("THING", 0) == 42
        assert cfg.resolve("THING") == "42"  # nothing to go by: text

    def test_a_value_that_does_not_fit_is_kept_as_text_and_reported(
        self, isolated, monkeypatch, caplog
    ):
        cfg = isolated({"LIMIT": 5})
        monkeypatch.setenv("VOLNUX_LIMIT", "lots")
        with caplog.at_level(logging.WARNING, logger="volnux.config"):
            assert cfg.resolve("LIMIT") == "lots"
        assert "VOLNUX_LIMIT" in caplog.text

    def test_the_private_key_is_never_a_setting(self, isolated, monkeypatch):
        cfg = isolated({})
        monkeypatch.setenv("VOLNUX_MESH_PRIVATE_KEY", "PEM")
        assert cfg.resolve("MESH_PRIVATE_KEY", None) is None

    def test_setting_uses_the_shared_instance(self, isolated, monkeypatch):
        cfg = isolated({"LIMIT": 5})
        monkeypatch.setattr(VolnuxConfig, "_instance", cfg)
        monkeypatch.setenv("VOLNUX_LIMIT", "8")
        assert setting("LIMIT") == 8
        assert setting("NOPE", "d") == "d"


class TestNoneSettings:
    def test_upper_case_none_is_loaded(self, isolated):
        assert isolated({"PROJECT_ID": None}).get("PROJECT_ID") is None

    def test_lower_case_none_is_not(self, isolated):
        with pytest.raises(AttributeError):
            isolated({"incidental": None}).get("incidental")


class TestSensitiveSettings:
    @pytest.mark.parametrize(
        "name",
        [
            "SECRET_KEY",
            "MESH_PRIVATE_KEY",
            "MESH_PUBLIC_KEY",
            "DB_PASSWORD",
            "API_TOKEN",
            "stripe_secret",
            "SOME_PRIVATE_KEY",
        ],
    )
    def test_secret_names(self, name):
        assert is_sensitive_setting(name)

    @pytest.mark.parametrize(
        "name",
        ["API_PORT", "K8S_KEY_TTL_SECONDS", "JWT_ALGORITHM", "DATABASES"],
    )
    def test_other_names(self, name):
        assert not is_sensitive_setting(name)

    def test_secrets_stay_out_of_every_carrier(self, isolated, monkeypatch):
        monkeypatch.setenv("VOLNUX_SECRET_KEY", "SUPER-SECRET-VALUE")
        cfg = isolated({"API_PORT": 8080})
        cfg.add("db_password", "hunter2")
        cfg.add("api_token", "tok-123", namespace="team")
        cfg.add("visible", "yes", namespace="team")

        assert cfg.get("SECRET_KEY") == "SUPER-SECRET-VALUE"  # usable here ...
        for carrier in (
            json.dumps(cfg.to_dict()),
            cfg.to_json(),
            cfg.to_base64(),
        ):
            assert "SUPER-SECRET-VALUE" not in carrier  # ... never sent on
        text = cfg.to_json()
        assert "hunter2" not in text and "tok-123" not in text
        assert "8080" in text and "VISIBLE" in text

    def test_a_worker_rebuilt_from_a_carrier_has_no_secret(self, isolated, monkeypatch):
        monkeypatch.setenv("VOLNUX_SECRET_KEY", "SUPER-SECRET-VALUE")
        worker = VolnuxConfig.from_json(isolated({"API_PORT": 8080}).to_json())
        assert worker.get("API_PORT") == 8080
        assert worker.get("SECRET_KEY", None) is None
