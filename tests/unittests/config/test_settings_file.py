"""volnux.conf.global_settings: every parameter is documented, safe to ship, and used."""

import ast
import re
from pathlib import Path

import pytest

import volnux
import volnux.conf.global_settings as settings
from volnux.config import ConfigEntry, VolnuxConfig, is_sensitive_setting

SETTINGS_PATH = Path(settings.__file__)
SOURCE = SETTINGS_PATH.read_text()
LINES = SOURCE.splitlines()
TREE = ast.parse(SOURCE)
PACKAGE = Path(volnux.__file__).parent


def declared():
    """(name, line number) of every setting assigned at the top level."""
    found = []
    for node in TREE.body:
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id.isupper()
        ):
            found.append((node.targets[0].id, node.lineno))
    return found


SETTINGS = declared()
NAMES = [name for name, _ in SETTINGS]


def comment_block(lineno):
    """The comment lines directly above a line, top to bottom."""
    block = []
    i = lineno - 2
    while i >= 0 and LINES[i].startswith("#"):
        block.append(LINES[i])
        i -= 1
    return block[::-1]


def test_there_are_many_settings():
    assert len(NAMES) >= 90


def test_names_are_unique():
    assert len(NAMES) == len(set(NAMES))


@pytest.mark.parametrize("name, lineno", SETTINGS, ids=NAMES)
def test_every_setting_is_documented(name, lineno):
    block = comment_block(lineno)
    text = "\n".join(block)
    assert any(line.rstrip() == f"# {name}" for line in block), "no title line"
    for label in ("What:", "Used by:", "Env:", "Production:"):
        assert label in text, f"{name}: missing '{label}'"
    assert f"VOLNUX_{name}" in text, f"{name}: the Env line must name VOLNUX_{name}"
    after = text.split("Production:", 1)[1]
    assert after.strip("# \n"), f"{name}: the Production example is empty"
    what = text.split("What:", 1)[1].split("Used by:", 1)[0]
    assert len(what.replace("#", "").split()) >= 4, f"{name}: What is too short"


def test_the_file_explains_its_own_conventions():
    head = "\n".join(LINES[:50])
    for phrase in (
        "VOLNUX_<NAME>",
        "VOLNUX_SETTINGS_MODULE",
        "SECRETS",
        "Production",
        "NO external",
        "global_settings",
    ):
        assert phrase in head


@pytest.mark.parametrize("name", NAMES)
def test_values_can_be_loaded_as_settings(name):
    value = getattr(settings, name)
    assert value is None or isinstance(
        value, (int, float, str, bool, list, dict, tuple, Path)
    )


@pytest.mark.parametrize("name", NAMES)
def test_secrets_have_no_value_in_the_file(name):
    if is_sensitive_setting(name):
        assert getattr(settings, name) is None


def test_secrets_are_not_the_old_hardcoded_one():
    assert "volnux-jwt-secret" not in SOURCE.replace("$(", "")


def test_no_real_credentials_in_examples():
    # production examples show hosts, never a password
    assert not re.search(r"password\"?\s*[:=]\s*\"[^\"]+\"", SOURCE, re.I)


def test_defaults_need_no_infrastructure():
    assert settings.DATABASES["default"]["ENGINE"].endswith(
        "InMemoryKeyValueStoreBackend"
    )
    assert "sqlite" in settings.DATABASES["triggers"]["ENGINE"].lower()
    assert set(settings.DATABASES) == {"default", "triggers"}
    assert settings.OTEL_ENABLED is False
    assert "storage" not in settings.CHECKPOINT_CONFIG  # it was never read


def test_unsafe_defaults_are_fail_closed():
    assert settings.DEBUG is False
    assert settings.SECRET_KEY is None
    assert settings.API_DOCS is False


# ---------------------------------------------------------------------------
# no inert settings
# ---------------------------------------------------------------------------

#: Settings that no code reads by name, and why.
NOT_READ_BY_NAME = {
    # read as attributes of the config object by the modules that need them
}


def _code():
    texts = []
    for path in PACKAGE.rglob("*.py"):
        if path == SETTINGS_PATH or "protos" in path.parts:
            continue
        texts.append(path.read_text(errors="ignore"))
    return "\n".join(texts)


CODE = _code()


@pytest.mark.parametrize("name", NAMES)
def test_every_setting_is_used_by_the_code(name):
    if name in NOT_READ_BY_NAME:
        return
    quoted = re.search(rf"[\"']{name}[\"']", CODE)
    attribute = re.search(rf"\b(?:conf|CONF|config|volnux_config)\.{name}\b", CODE)
    assert quoted or attribute, f"{name} is defined in settings.py but nothing reads it"


@pytest.mark.parametrize("name", NAMES)
def test_used_by_names_files_that_exist(name):
    lineno = dict(SETTINGS)[name]
    text = " ".join(comment_block(lineno))
    used = text.split("Used by:", 1)[1].split("Env:", 1)[0]
    for path in re.findall(r"volnux/[\w/\.\*]+", used):
        path = path.rstrip(",.")
        if path.endswith("*"):
            assert list(PACKAGE.parent.glob(path)), f"{name}: no file matches {path}"
        else:
            assert (PACKAGE.parent / path).exists(), f"{name}: {path} does not exist"


# ---------------------------------------------------------------------------
# the file as the configuration loads it
# ---------------------------------------------------------------------------


@pytest.fixture
def real_config(monkeypatch):
    monkeypatch.setattr(
        VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
    )
    for var in list(__import__("os").environ):
        if var.startswith("VOLNUX_") and var not in ("VOLNUX_MESH_NODE_ID",):
            monkeypatch.delenv(var)
    return VolnuxConfig()


def test_every_setting_reaches_the_configuration(real_config):
    for name in NAMES:
        assert real_config.get_config_entry(name) is not None, name


def test_none_settings_read_as_none_not_missing(real_config):
    assert real_config.get("PROJECT_ID") is None
    assert real_config.get("SHARED_PROCESS_POOL_SIZE") is None


def test_the_defaults_serialise_without_secrets(real_config):
    text = real_config.to_json()
    again = VolnuxConfig.from_json(text)
    assert again.get("MAX_EVENT_RETRIES") == 5
    assert again.get("SECRET_KEY", "absent") == "absent"
    assert again.get("MIGRATION_DIR") == str(settings.MIGRATION_DIR)


def test_previous_values_are_unchanged():
    assert settings.MAX_EVENT_RETRIES == 5
    assert settings.MAX_EVENT_BACKOFF_FACTOR == 0.05
    assert settings.MAX_EVENT_BACKOFF == 100
    assert settings.MAX_BATCH_PROCESSING_WORKERS == 4
    assert settings.MAX_ENGINE_ITERATIONS == 10000
    assert settings.DEFAULT_CONNECTION_TIMEOUT == 30
    assert settings.DATA_CHUNK_SIZE == 4096
    assert settings.CONNECTION_BACKLOG_SIZE == 5
    assert settings.DATA_QUEUE_SIZE == 1000
    assert settings.PIPELINE_REPORTERS == [
        "volnux.governance.reporter.install_governance_reporter"
    ]
    assert settings.LOGGING["version"] == 1
    assert settings.LOGGING["disable_existing_loggers"] is False


# ---------------------------------------------------------------------------
# the Django layout
# ---------------------------------------------------------------------------


def test_old_names_are_not_settings_any_more():
    from volnux.conf import REMOVED_SETTINGS

    assert not set(REMOVED_SETTINGS) & set(NAMES)


def test_the_redis_channel_settings_are_gone():
    for name in (
        "REDIS_URL",
        "TASK_LOCATION_TTL",
        "TASK_LOCATION_REFRESH",
        "NODE_HEARTBEAT_TTL",
        "SUBSCRIBE_TIMEOUT",
        "CHANNEL_QUEUE_SIZE",
    ):
        assert name not in NAMES
    assert "redis_channel" not in SOURCE


def test_django_names_exist():
    for name in (
        "DEBUG",
        "SECRET_KEY",
        "BASE_DIR",
        "DATABASES",
        "LOGGING",
        "CORS_ALLOWED_ORIGINS",
    ):
        assert name in NAMES


def test_the_database_aliases_the_models_use_are_documented():
    from volnux.backends.databases import get_database_config  # noqa: F401

    block = " ".join(comment_block(dict(SETTINGS)["DATABASES"]))
    for alias in ("default", "triggers", "messaging", "streams", "saga", "checkpoints"):
        assert f"'{alias}'" in block, alias


def test_the_old_module_is_gone():
    with pytest.raises(ImportError):
        __import__("volnux.settings")


def test_settings_are_upper_case_names():
    assert all(name == name.upper() for name in NAMES)
