"""DATABASES: Django-style entries, mapped to the arguments the backends take."""

import asyncio
import copy
import json
from pathlib import Path

import pytest

from volnux.backends.databases import (
    DEFAULT_DB_ALIAS,
    get_database_config,
    get_databases,
)
from volnux.backends.connection import ConnectionConfig
from volnux.conf import override_settings
from volnux.exceptions import ImproperlyConfigured

MEMORY = "volnux.backends.stores.inmemory.InMemoryKeyValueStoreBackend"
SQLITE = "volnux.backends.stores.sqlite.SqliteStoreBackend"
REDIS = "volnux.backends.stores.redis.RedisStoreBackend"
POSTGRES = "volnux.backends.stores.postgres.PostgresStoreBackend"


def databases(**aliases):
    return override_settings(DATABASES={"default": {"ENGINE": MEMORY}, **aliases})


class TestMapping:
    def test_django_keys_become_connector_arguments(self):
        entry = {
            "ENGINE": POSTGRES,
            "NAME": "volnux",
            "HOST": "db.internal",
            "PORT": 5432,
            "USER": "app",
            "PASSWORD": "s3cret",
        }
        with databases(default=entry):
            assert get_database_config() == {
                "ENGINE": POSTGRES,
                "CONNECTOR_CONFIG": {
                    "database": "volnux",
                    "host": "db.internal",
                    "port": 5432,
                    "username": "app",
                    "password": "s3cret",
                },
            }

    def test_options_are_passed_through(self):
        entry = {
            "ENGINE": POSTGRES,
            "HOST": "h",
            "OPTIONS": {"pool_size": 20, "ssl_enabled": True, "extra_params": {"a": 1}},
        }
        with databases(default=entry):
            connector = get_database_config()["CONNECTOR_CONFIG"]
        assert connector == {
            "host": "h",
            "pool_size": 20,
            "ssl_enabled": True,
            "extra_params": {"a": 1},
        }

    def test_nothing_to_pass_means_no_connector_config(self):
        with databases():
            assert get_database_config() == {"ENGINE": MEMORY}

    def test_none_values_are_left_out(self):
        with databases(default={"ENGINE": REDIS, "HOST": "h", "PASSWORD": None}):
            assert get_database_config()["CONNECTOR_CONFIG"] == {"host": "h"}

    def test_a_zero_is_a_value(self):
        # Redis database 0, port 0 ... must not be mistaken for "not set"
        with databases(default={"ENGINE": REDIS, "HOST": "h", "NAME": 0}):
            assert get_database_config()["CONNECTOR_CONFIG"]["database"] == 0

    def test_a_path_name_becomes_text(self, tmp_path):
        with databases(default={"ENGINE": SQLITE, "NAME": tmp_path / "v.db"}):
            assert get_database_config()["CONNECTOR_CONFIG"] == {
                "database": str(tmp_path / "v.db")
            }

    def test_the_result_is_a_copy(self):
        entry = {"ENGINE": REDIS, "OPTIONS": {"extra_params": {"a": 1}}}
        with databases(default=entry):
            config = get_database_config()
            config["CONNECTOR_CONFIG"]["extra_params"]["a"] = 2
            config["CONNECTOR_CONFIG"]["new"] = 1
            assert get_database_config()["CONNECTOR_CONFIG"] == {
                "extra_params": {"a": 1}
            }
        assert entry["OPTIONS"] == {"extra_params": {"a": 1}}

    def test_the_mapped_arguments_build_a_connection_config(self):
        entry = {
            "ENGINE": POSTGRES,
            "NAME": "volnux",
            "HOST": "db",
            "PORT": 5432,
            "USER": "u",
            "PASSWORD": "p",
            "OPTIONS": {"timeout": 5, "pool_size": 2},
        }
        with databases(default=entry):
            connector = get_database_config()["CONNECTOR_CONFIG"]
        config = ConnectionConfig(**connector)
        assert (config.host, config.port, config.database) == ("db", 5432, "volnux")
        assert (config.username, config.password) == ("u", "p")
        assert (config.timeout, config.pool_size) == (5, 2)


class TestAliases:
    def test_a_configured_alias(self):
        with databases(triggers={"ENGINE": SQLITE, "NAME": "x.db"}):
            assert get_database_config("triggers")["ENGINE"] == SQLITE

    def test_an_alias_that_is_not_configured_uses_default(self):
        with databases():
            assert get_database_config("streams") == get_database_config("default")
            assert get_database_config("anything") == {"ENGINE": MEMORY}

    def test_the_default_alias_is_default(self):
        assert DEFAULT_DB_ALIAS == "default"
        with databases():
            assert get_database_config() == get_database_config("default")

    def test_the_shipped_defaults(self):
        config = get_database_config("triggers")
        assert config["ENGINE"] == SQLITE
        assert config["CONNECTOR_CONFIG"] == {"database": "volnux.db"}
        assert get_database_config("default") == {"ENGINE": MEMORY}

    def test_get_databases_returns_the_setting(self):
        with databases(x={"ENGINE": MEMORY}):
            assert set(get_databases()) == {"default", "x"}

    def test_from_the_environment_as_json(self, monkeypatch):
        monkeypatch.setenv(
            "VOLNUX_DATABASES",
            json.dumps(
                {
                    "default": {"ENGINE": REDIS, "HOST": "r", "PORT": 6379},
                    "triggers": {"ENGINE": SQLITE, "NAME": "/data/v.db"},
                }
            ),
        )
        assert get_database_config()["CONNECTOR_CONFIG"] == {"host": "r", "port": 6379}
        assert get_database_config("triggers")["CONNECTOR_CONFIG"] == {
            "database": "/data/v.db"
        }


class TestValidation:
    def bad(self, databases_value, match):
        with override_settings(DATABASES=databases_value):
            with pytest.raises(ImproperlyConfigured, match=match):
                get_database_config()

    def test_databases_must_be_a_dict_of_dicts(self):
        self.bad(["default"], "dict of database aliases")
        self.bad({"default": "x"}, "dict of database aliases")
        self.bad(None, "dict of database aliases")
        self.bad({1: {}}, "dict of database aliases")

    def test_default_is_required(self):
        self.bad({"other": {"ENGINE": MEMORY}}, "'default'")
        self.bad({}, "'default'")

    def test_engine_is_required(self):
        self.bad({"default": {}}, "ENGINE")
        self.bad({"default": {"ENGINE": ""}}, "ENGINE")
        self.bad({"default": {"ENGINE": 5}}, "ENGINE")

    def test_unknown_keys_are_typos(self):
        self.bad({"default": {"ENGINE": MEMORY, "HOSTNAME": "h"}}, "HOSTNAME")
        self.bad({"default": {"ENGINE": MEMORY, "host": "h"}}, "'host'")

    def test_the_old_connector_config_is_refused_with_the_replacement(self):
        self.bad(
            {"default": {"ENGINE": MEMORY, "CONNECTOR_CONFIG": {"host": "h"}}},
            "CONNECTOR_CONFIG.*NAME, HOST, PORT, USER, PASSWORD and OPTIONS",
        )

    def test_options_must_be_a_dict(self):
        self.bad({"default": {"ENGINE": MEMORY, "OPTIONS": "x"}}, "OPTIONS")

    @pytest.mark.parametrize(
        "key, argument",
        [
            ("NAME", "database"),
            ("HOST", "host"),
            ("PORT", "port"),
            ("USER", "username"),
            ("PASSWORD", "password"),
        ],
    )
    def test_a_value_given_twice_is_refused(self, key, argument):
        self.bad(
            {"default": {"ENGINE": MEMORY, key: "x", "OPTIONS": {argument: "y"}}},
            f"{key} and OPTIONS\\['{argument}'\\]",
        )

    def test_the_alias_is_named_in_the_message(self):
        with databases(triggers={"ENGINE": MEMORY, "BOGUS": 1}):
            with pytest.raises(ImproperlyConfigured, match=r"DATABASES\['triggers'\]"):
                get_database_config("triggers")

    def test_a_bad_default_is_reported_for_an_alias_that_falls_back_to_it(self):
        with override_settings(DATABASES={"default": {}}):
            with pytest.raises(ImproperlyConfigured, match=r"DATABASES\['default'\]"):
                get_database_config("streams")

    def test_a_password_is_never_in_an_error(self):
        entries = [
            {"ENGINE": MEMORY, "PASSWORD": "TOPSECRET", "HOSTNAME": "h"},
            {
                "ENGINE": MEMORY,
                "PASSWORD": "TOPSECRET",
                "OPTIONS": {"password": "TOPSECRET"},
            },
            {"PASSWORD": "TOPSECRET"},
            {"ENGINE": MEMORY, "PASSWORD": "TOPSECRET", "CONNECTOR_CONFIG": {}},
        ]
        for entry in entries:
            with override_settings(DATABASES={"default": entry}):
                with pytest.raises(ImproperlyConfigured) as exc:
                    get_database_config()
            assert "TOPSECRET" not in str(exc.value)


class TestRealBackends:
    def test_a_sqlite_database_from_settings(self, tmp_path):
        from volnux.import_utils import import_string

        entry = {"ENGINE": SQLITE, "NAME": tmp_path / "t.db"}
        with databases(triggers=entry):
            config = get_database_config("triggers")
        backend = import_string(config["ENGINE"])(**config["CONNECTOR_CONFIG"])
        try:
            assert (tmp_path / "t.db").exists()
        finally:
            backend.close()

    def test_the_memory_backend_takes_no_arguments(self):
        from volnux.import_utils import import_string

        with databases():
            config = get_database_config()
        assert import_string(config["ENGINE"])(**config.get("CONNECTOR_CONFIG", {}))


class TestModelAliases:
    CASES = [
        ("volnux.engine.workflows.trigger.state", "TriggerStateRecord", "triggers"),
        ("volnux.engine.workflows.trigger.event", "TriggerEvent", "messaging"),
        ("volnux.governance.events", "GovernanceEvent", "messaging"),
        (
            "volnux.flows.bridge.communications.tasks.channels.messaging",
            "ChannelEnvelope",
            "messaging",
        ),
        ("volnux.mixins.stream.queue", "StreamChunk", "streams"),
        ("volnux.backends.saga._dlq", "DeadLetterEntry", "saga"),
        ("volnux.backends.saga.recovery", "SagaRecoveryState", "saga"),
        ("volnux.execution.rehydrator.engine.snapshot", "TaskSnapshot", "checkpoints"),
        (
            "volnux.execution.rehydrator.engine.snapshot",
            "EngineStateSnapshot",
            "checkpoints",
        ),
        (
            "volnux.execution.rehydrator.event.snapshot",
            "EventCheckpointSnapshot",
            "checkpoints",
        ),
        ("volnux.result.result", "EventResult", "default"),
        ("volnux.execution.context", "ExecutionContext", "default"),
        ("volnux.models.base", "GovernanceModel", "default"),
    ]

    @pytest.mark.parametrize("module, name, alias", CASES, ids=[c[1] for c in CASES])
    def test_alias(self, module, name, alias):
        import importlib

        model = getattr(importlib.import_module(module), name)
        assert model.get_database_alias() == alias

    @pytest.mark.parametrize("module, name, alias", CASES, ids=[c[1] for c in CASES])
    def test_the_model_reads_its_database(self, module, name, alias):
        import importlib

        model = getattr(importlib.import_module(module), name)
        entry = {"ENGINE": "x.Backend", "HOST": f"host-of-{alias}"}
        with databases(**{alias: entry} if alias != "default" else {}):
            config = model.get_backend_config()
        if alias == "default":
            assert config["ENGINE"] == MEMORY
        else:
            assert config == {
                "ENGINE": "x.Backend",
                "CONNECTOR_CONFIG": {"host": f"host-of-{alias}"},
            }

    @pytest.mark.parametrize("module, name, alias", CASES, ids=[c[1] for c in CASES])
    def test_without_its_database_it_uses_default(self, module, name, alias):
        import importlib

        model = getattr(importlib.import_module(module), name)
        entry = {"ENGINE": "x.Backend", "HOST": "the-default"}
        if alias == "triggers":
            pytest.skip("the shipped settings configure this one")
        with override_settings(DATABASES={"default": entry}):
            assert model.get_backend_config()["CONNECTOR_CONFIG"] == {
                "host": "the-default"
            }

    def test_models_can_still_be_constructed(self):
        # an attribute named like a field would have become a required one
        from volnux.engine.workflows.trigger.triggers import ManualTrigger

        ManualTrigger("wf")

    def test_a_model_overrides_the_alias_with_a_method(self):
        from volnux.mixins.key_value_store_integration import (
            KeyValueStoreIntegrationMixin,
        )

        class Mine(KeyValueStoreIntegrationMixin):
            @classmethod
            def get_database_alias(cls):
                return "mine"

        with databases(mine={"ENGINE": "x.Mine"}):
            assert Mine.get_backend_config() == {"ENGINE": "x.Mine"}


class TestCheckCommand:
    def run(self):
        from volnux.cli.command.mixins.check import _check_backends

        return asyncio.run(_check_backends(None))

    def test_every_alias_is_pinged(self, tmp_path):
        entries = {
            "default": {"ENGINE": MEMORY},
            "triggers": {"ENGINE": SQLITE, "NAME": str(tmp_path / "c.db")},
        }
        with override_settings(DATABASES=entries):
            results = self.run()
        assert [name for name, _, _ in results] == ["default", "triggers"]
        assert all(ok for _, ok, _ in results), results

    def test_an_unreachable_one_is_reported_not_raised(self):
        entries = {
            "default": {"ENGINE": MEMORY},
            "broken": {"ENGINE": "volnux.backends.stores.nope.Missing"},
        }
        with override_settings(DATABASES=entries):
            results = dict((n, (ok, msg)) for n, ok, msg in self.run())
        assert results["default"][0] is True
        assert (
            results["broken"][0] is False and "import" in results["broken"][1].lower()
        )

    def test_an_invalid_entry_is_reported_per_alias(self):
        entries = {"default": {"ENGINE": MEMORY}, "bad": {"BOGUS": 1}}
        with override_settings(DATABASES=entries):
            results = dict((n, (ok, msg)) for n, ok, msg in self.run())
        assert results["bad"][0] is False and "BOGUS" in results["bad"][1]

    def test_invalid_databases_is_one_result(self):
        with override_settings(DATABASES={"nope": {}}):
            [(name, ok, message)] = self.run()
        assert name == "DATABASES" and not ok and "'default'" in message
