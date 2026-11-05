import os

import pytest
from types import ModuleType

import volnux.config as config_module
from volnux.config import ENV_MESH_NODE_ID, VolnuxConfig


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    """Build VolnuxConfig objects that read nothing from the machine: no settings
    file is searched for, the defaults are the ones given, and the process
    environment (which the constructor writes the node id to) is restored."""

    # No VOLNUX_* variable of the machine, or set by another test (the governance
    # conftest sets VOLNUX_PROJECT_ID for the whole session), may reach these.
    for var in [v for v in os.environ if v.startswith("VOLNUX_")]:
        monkeypatch.delenv(var)
    monkeypatch.setattr(
        VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
    )

    def make(defaults=None, config_file=None, signer=None):
        module = ModuleType("test_defaults")
        for name, value in (defaults or {}).items():
            setattr(module, name, value)
        monkeypatch.setattr(config_module, "default_settings", module)
        monkeypatch.delenv(ENV_MESH_NODE_ID, raising=False)
        return VolnuxConfig(config_file=config_file, signer=signer)

    make.tmp_path = tmp_path
    return make
