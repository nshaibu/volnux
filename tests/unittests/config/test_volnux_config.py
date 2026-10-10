import os
import importlib.util
from types import ModuleType
from unittest.mock import Mock

import pytest

import volnux.config as config_module
from volnux.config import (
    ConfigEntry,
    VolnuxConfig,
    ENV_MESH_NODE_ID,
    ENV_MESH_PUBLIC_KEY,
    ENV_MESH_PRIVATE_KEY,
)


class DummyStore:
    def __init__(self):
        self.entries = []

    def add(self, entry):
        self.entries.append(entry)

    def get_entry_by_hash(self, entry_hash):
        for entry in self.entries:
            if hash(entry) == entry_hash:
                return entry
        raise KeyError(entry_hash)

    def get(self, **kwargs):
        name = kwargs.get("name")
        for entry in self.entries:
            if entry.name == name:
                return entry
        raise KeyError(name)

    def __contains__(self, item):
        return item in self.entries

    def __len__(self):
        return len(self.entries)


class DummySigner:
    def __init__(self):
        self.sign_calls = []
        self.verify_calls = []

    def sign(self, data: bytes) -> bytes:
        self.sign_calls.append(data)
        return b"signed:" + data

    def verify(self, data: bytes, signature: bytes) -> bool:
        self.verify_calls.append((data, signature))
        return signature == b"signed:" + data


@pytest.fixture
def cfg(monkeypatch):
    # monkeypatch.setattr(config_module, "ResultSet", lambda: DummyStore())
    monkeypatch.setattr(
        config_module, "default_settings", ModuleType("default_settings")
    )
    monkeypatch.setattr(
        VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
    )
    monkeypatch.setattr(VolnuxConfig, "load_from_file", lambda self, file_path: None)
    return VolnuxConfig(config_file=None)


def test_get_instance_returns_singleton(monkeypatch):
    # monkeypatch.setattr(config_module, "ResultSet", lambda: DummyStore())
    monkeypatch.setattr(
        config_module, "default_settings", ModuleType("default_settings")
    )
    monkeypatch.setattr(
        VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
    )
    monkeypatch.setattr(VolnuxConfig, "load_from_file", lambda self, file_path: None)

    VolnuxConfig._instance = None
    first = VolnuxConfig.get_instance()
    second = VolnuxConfig.get_instance()

    assert first is second


def test_get_node_id_returns_generated_or_existing_value(monkeypatch):
    # monkeypatch.setattr(config_module, "ResultSet", lambda: DummyStore())
    monkeypatch.setattr(
        config_module, "default_settings", ModuleType("default_settings")
    )
    monkeypatch.setattr(
        VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
    )
    monkeypatch.setattr(VolnuxConfig, "load_from_file", lambda self, file_path: None)

    monkeypatch.delenv(config_module.ENV_MESH_NODE_ID, raising=False)
    monkeypatch.setattr(config_module, "_generate_node_id", lambda: "node-123")

    inst = VolnuxConfig(config_file=None)
    assert inst.get_node_id() == "node-123"
    assert config_module.os.environ[config_module.ENV_MESH_NODE_ID] == "node-123"


def test_get_config_files_respects_precedence(monkeypatch):
    monkeypatch.setattr(
        VolnuxConfig,
        "_search_for_config_file_in_dir",
        staticmethod(lambda: "/tmp/current/settings.py"),
    )
    monkeypatch.setenv(config_module.ENV_CONFIG, "/tmp/env/settings.py")

    inst = object.__new__(VolnuxConfig)
    files = list(inst._get_config_files("/tmp/arg/settings.py"))

    assert files == [
        "/tmp/current/settings.py",
        "/tmp/env/settings.py",
        "/tmp/arg/settings.py",
    ]


def test_search_for_config_file_in_dir_returns_current_file(monkeypatch):
    monkeypatch.setenv(config_module.ENV_CONFIG_DIR, "/tmp/app")
    monkeypatch.setattr(
        config_module.os.path, "isfile", lambda p: p == "/tmp/app/settings.py"
    )

    assert VolnuxConfig._search_for_config_file_in_dir() == "/tmp/app/settings.py"


def test_search_for_config_file_in_dir_finds_immediate_subdir(monkeypatch):
    monkeypatch.setenv(config_module.ENV_CONFIG_DIR, "/tmp/app")
    monkeypatch.setattr(
        config_module.os.path, "isfile", lambda p: p == "/tmp/app/workflow/settings.py"
    )
    monkeypatch.setattr(config_module.os, "listdir", lambda p: ["workflow"])
    monkeypatch.setattr(
        config_module.os.path, "isdir", lambda p: p == "/tmp/app/workflow"
    )

    assert (
        VolnuxConfig._search_for_config_file_in_dir() == "/tmp/app/workflow/settings.py"
    )


def test_load_module_adds_supported_values(monkeypatch):
    # monkeypatch.setattr(config_module, "ResultSet", lambda: DummyStore())
    monkeypatch.setattr(
        config_module, "default_settings", ModuleType("default_settings")
    )
    monkeypatch.setattr(
        VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
    )
    monkeypatch.setattr(VolnuxConfig, "load_from_file", lambda self, file_path: None)

    inst = VolnuxConfig(config_file=None)

    module = ModuleType("custom_settings")
    module.A = 1
    module.B = "x"
    module.C = [1, 2]
    module._private = "not-skipped"
    module.__hidden__ = "skip"

    inst._load_module(module)

    names = [entry.name for entry in inst._store]
    assert "A" in names
    assert "B" in names
    assert "C" in names
    assert "_PRIVATE" in names
    assert "__HIDDEN__" not in names


def test_load_module_rejects_non_module():
    inst = object.__new__(VolnuxConfig)

    with pytest.raises(TypeError, match="config_module must be of type ModuleType"):
        inst._load_module("not-a-module")  # type: ignore[arg-type]


def test_get_config_entry_finds_entry_by_name(cfg):
    cfg.add("service_url", "https://example.test")

    entry = cfg.get_config_entry("service_url")

    assert entry is not None
    assert entry.value == "https://example.test"


def test_load_from_environment_variable_and_override_manually(monkeypatch, cfg):
    monkeypatch.setenv("VOLNUX_API_URL", "from-env")
    cfg.load_from_environ()
    assert cfg.get("API_URL") == "from-env"

    cfg.add("API_URL", "from-store") # overridden

    assert cfg.get("API_URL") == "from-store"


def test_get_reads_store_when_env_missing(cfg):
    cfg.add("API_URL", "from-store")

    assert cfg.get("API_URL") == "from-store"


def test_get_returns_default_when_missing(cfg):
    assert cfg.get("MISSING_KEY", default="fallback") == "fallback"


def test_get_raises_when_missing_and_no_default(cfg):
    with pytest.raises(AttributeError, match="Missing configuration key 'MISSING_KEY'"):
        cfg.get("MISSING_KEY")


def test_get_allows_none_default(cfg):
    assert cfg.get("MISSING_KEY", default=None) is None


def test_add_uppercases_key_and_stores_value(cfg):
    cfg.add("my_key", 42)

    entry = cfg.get_config_entry("my_key")
    assert entry is not None
    assert entry.name == "MY_KEY"
    assert entry.value == 42


def test_add_uses_current_node_id_when_node_id_is_missing(monkeypatch):
    # monkeypatch.setattr(config_module, "ResultSet", lambda: DummyStore())
    monkeypatch.setattr(
        config_module, "default_settings", ModuleType("default_settings")
    )
    monkeypatch.setattr(
        VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
    )
    monkeypatch.setattr(VolnuxConfig, "load_from_file", lambda self, file_path: None)

    inst = VolnuxConfig(config_file=None)
    monkeypatch.setattr(inst, "get_node_id", lambda: "node-abc")

    inst.add("feature_flag", True)
    entry = inst.get_config_entry("feature_flag")

    assert entry is not None
    assert entry.origin_mesh_node == "node-abc"


def test_add_entry_config_signs_local_entry_when_signer_is_present(monkeypatch):
    # monkeypatch.setattr(config_module, "ResultSet", lambda: DummyStore())
    monkeypatch.setattr(
        config_module, "default_settings", ModuleType("default_settings")
    )
    monkeypatch.setattr(
        VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
    )
    monkeypatch.setattr(VolnuxConfig, "load_from_file", lambda self, file_path: None)

    signer = DummySigner()
    inst = VolnuxConfig(config_file=None, signer=signer)

    entry = ConfigEntry(
        value="hello", name="MESSAGE", origin_mesh_node=inst.get_node_id()
    )
    inst.add_entry_config(entry)

    assert entry.signature is not None
    assert signer.sign_calls


def test_update_from_mesh_accepts_newer_entry(cfg):
    local = ConfigEntry(value="old", name="KEY", origin_mesh_node="node-a", timestamp=1)
    incoming = ConfigEntry(
        value="new", name="KEY", origin_mesh_node="node-b", timestamp=2
    )

    cfg._store.add(local)

    assert cfg.update_from_mesh(incoming) is True
    assert cfg.get("KEY") == "new"


def test_update_from_mesh_accepts_tie_breaker_by_node_id(cfg):
    # # Current node ID is node-a, incoming entry is from node-b
    # monkeypatch.setenv(ENV_MESH_NODE_ID, "node-a")

    local = ConfigEntry(value="old", name="KEY", origin_mesh_node="node-a", timestamp=5)
    incoming = ConfigEntry(
        value="new", name="KEY", origin_mesh_node="node-z", timestamp=5
    )

    cfg._store.add(local)

    assert cfg.update_from_mesh(incoming) is True
    assert cfg.get("KEY") == "new"


def test_update_from_mesh_rejects_stale_entry(cfg):
    local = ConfigEntry(value="old", name="KEY", origin_mesh_node="node-z", timestamp=5)
    incoming = ConfigEntry(
        value="new", name="KEY", origin_mesh_node="node-a", timestamp=4
    )

    cfg._store.add(local)

    assert cfg.update_from_mesh(incoming) is False
    assert cfg.get("KEY") == "old"


def test_getattr_reads_config_value(cfg):
    cfg.add("service_name", "volnux")

    assert cfg.service_name == "volnux"


def test_getattr_rejects_private_attrs(cfg):
    with pytest.raises(AttributeError):
        _ = cfg._secret


@pytest.mark.asyncio
async def test_add_async_calls_sync_add(monkeypatch, cfg):
    called = {"count": 0}

    def fake_add(key, value, node_id=None, timestamp=None, namespace=None):
        called["count"] += 1

    monkeypatch.setattr(cfg, "add", fake_add)

    await cfg.add_async("k", "v")

    assert called["count"] == 1


@pytest.mark.asyncio
async def test_get_async_calls_sync_get(monkeypatch, cfg):
    monkeypatch.setattr(cfg, "get", lambda key, default=None: "async-value")

    result = await cfg.get_async("k")

    assert result == "async-value"


def test_repr_includes_store_length(cfg):
    cfg.add("a", 1)
    assert repr(cfg) == "VolnuxConfig <len=2>"
