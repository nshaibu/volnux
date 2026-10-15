"""Tests for the pluggable pipeline-reporter installer.

``volnux.reporting`` is the config-driven seam that lets a deployment — or a
team — install one or more pipeline lifecycle reporters (the governance reporter
or a custom one) without the framework importing any reporter itself. These
tests drive ``ensure_pipeline_reporters_installed`` against a stubbed config and
stubbed imports, so no real reporter, messaging backend or engine is involved.
"""

import pytest

from volnux import reporting


class _StubConfig:
    """A config whose only known key is ``PIPELINE_REPORTERS``."""

    def __init__(self, reporters):
        self._reporters = reporters

    def get(self, key, default=None):
        return self._reporters if key == "PIPELINE_REPORTERS" else default


def _patch_config(monkeypatch, reporters):
    """Point ``reporting`` at a config holding the given reporter list.

    Patches only the ``reporting`` module's own ``VolnuxConfig`` reference, so
    the real ``volnux.config.VolnuxConfig`` singleton is never touched.
    """

    class _Stub:
        @classmethod
        def get_instance(cls):
            return _StubConfig(reporters)

    monkeypatch.setattr(reporting, "VolnuxConfig", _Stub)


def _recorder(calls):
    """An installer that records its keyword arguments on every call."""

    def installer(**kwargs):
        calls.append(kwargs)

    return installer


@pytest.fixture(autouse=True)
def _reset_attempted(monkeypatch):
    """``ensure_pipeline_reporters_installed`` is at-most-once per process; each
    test gets a fresh process-level flag so tests cannot leak into one another.
    """
    monkeypatch.setattr(reporting, "_attempted", False)
    yield
    monkeypatch.setattr(reporting, "_attempted", False)


# --- No-op when unconfigured ---------------------------------------------


def test_is_a_noop_when_no_reporters_are_configured(monkeypatch):
    _patch_config(monkeypatch, None)

    def explode(path):
        raise AssertionError(f"import_string should not be called, got {path}")

    monkeypatch.setattr(reporting, "import_string", explode)

    reporting.ensure_pipeline_reporters_installed()  # must not raise


def test_is_a_noop_when_the_reporter_list_is_empty(monkeypatch):
    _patch_config(monkeypatch, [])

    def explode(path):
        raise AssertionError("import_string should not be called")

    monkeypatch.setattr(reporting, "import_string", explode)

    reporting.ensure_pipeline_reporters_installed()


# --- Installing ----------------------------------------------------------


def test_installs_a_string_path_installer(monkeypatch):
    calls = []
    _patch_config(monkeypatch, ["team.reporting.install"])
    monkeypatch.setattr(reporting, "import_string", lambda path: _recorder(calls))

    reporting.ensure_pipeline_reporters_installed()

    assert calls == [{}]  # called once, with no kwargs


def test_passes_kwargs_for_a_mapping_entry(monkeypatch):
    calls = []
    _patch_config(
        monkeypatch,
        [{"path": "team.reporting.install", "kwargs": {"endpoint": "https://..."}}],
    )
    monkeypatch.setattr(reporting, "import_string", lambda path: _recorder(calls))

    reporting.ensure_pipeline_reporters_installed()

    assert calls == [{"endpoint": "https://..."}]


def test_mixed_entries_install_in_order(monkeypatch):
    calls = []
    _patch_config(
        monkeypatch,
        [
            "plain.string.path",
            {"path": "team.reporting.install", "kwargs": {"topic": "ops"}},
        ],
    )
    monkeypatch.setattr(reporting, "import_string", lambda path: _recorder(calls))

    reporting.ensure_pipeline_reporters_installed()

    assert calls == [{}, {"topic": "ops"}]


# --- At most once --------------------------------------------------------


def test_runs_the_configured_list_at_most_once(monkeypatch):
    calls = []
    _patch_config(monkeypatch, ["a", "b"])
    monkeypatch.setattr(reporting, "import_string", lambda path: _recorder(calls))

    reporting.ensure_pipeline_reporters_installed()
    reporting.ensure_pipeline_reporters_installed()

    assert len(calls) == 2  # "a" and "b", once each — not doubled


# --- Isolation -----------------------------------------------------------


def test_one_failing_installer_does_not_stop_the_rest(monkeypatch):
    calls = []

    def resolve(path):
        if path == "bad":

            def bad(**kwargs):
                raise RuntimeError("installer exploded")

            return bad
        return _recorder(calls)

    _patch_config(monkeypatch, ["good", "bad", "good2"])
    monkeypatch.setattr(reporting, "import_string", resolve)

    reporting.ensure_pipeline_reporters_installed()  # must not raise

    assert len(calls) == 2  # the two good installers still ran


def test_an_unimportable_path_is_isolated(monkeypatch):
    calls = []

    def resolve(path):
        if path == "missing":
            raise ImportError("no such module")
        return _recorder(calls)

    _patch_config(monkeypatch, ["missing", "good"])
    monkeypatch.setattr(reporting, "import_string", resolve)

    reporting.ensure_pipeline_reporters_installed()  # must not raise

    assert len(calls) == 1


def test_an_invalid_entry_is_skipped(monkeypatch):
    _patch_config(monkeypatch, [123])  # neither a string nor a mapping

    def explode(path):
        raise AssertionError("import_string should not be called")

    monkeypatch.setattr(reporting, "import_string", explode)

    reporting.ensure_pipeline_reporters_installed()  # must not raise
