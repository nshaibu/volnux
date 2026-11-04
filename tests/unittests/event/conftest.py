"""
pytest configuration — SCOPED to the EventBase / mixins test suite.

This conftest is intentionally self-contained:

  * It manipulates sys.path so the real `volnux` package becomes importable.
  * It patches `VolnuxConfig.get_instance` BEFORE any `volnux.*` module is
    imported, so the module-level `conf = VolnuxConfig.get_instance()` calls
    inside `volnux.mixins.event` / `volnux.mixins._phase_decorator` /
    `volnux.mixins.checkpointing` etc. all hit a controlled mock and never
    reach out to a real config backend.
  * It snapshots and restores the global event registry around every test
    so that the auto-registration performed by `EventMeta` does not leak
    between tests.
  * It exposes a minimal concrete `EventBase` subclass + builder fixtures
    used by every test file.

Nothing here is exported to other test folders — placing this file under
`/home/z/my-project/event/conftest.py` keeps the patches local to the
`event/` collection tree.

CONFIG
------
Set the env var `VOLNUX_REPO_ROOT` to the directory that *contains* the
`volnux/` package. Defaults to `/home/z/my-project` if unset.

If the path to `EventBase` differs from `volnux.event.base`, set
`VOLNUX_EVENT_BASE_MODULE` to the correct dotted module path (e.g.
`volnux.event` if EventBase lives in the package __init__).
"""

from __future__ import annotations

import os
import sys
import asyncio
import importlib
import types
from typing import Any, Dict, Optional

import pytest
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# 1. Repo root on sys.path
# ---------------------------------------------------------------------------

# _REPO_ROOT = os.environ.get("VOLNUX_REPO_ROOT")
# if os.path.isdir(_REPO_ROOT) and _REPO_ROOT not in sys.path:
#     sys.path.insert(0, _REPO_ROOT)

# Where EventBase lives. Override with VOLNUX_EVENT_BASE_MODULE if your layout
# differs (e.g. "volnux.event" if it's in package __init__).
_EVENT_BASE_MODULE = os.environ.get("VOLNUX_EVENT_BASE_MODULE", "volnux.event.base")


# ---------------------------------------------------------------------------
# 2. Mock VolnuxConfig.get_instance BEFORE any volnux.* module is imported
# ---------------------------------------------------------------------------
# Several volnux modules do `conf = VolnuxConfig.get_instance()` at import
# time (RetryPolicy dataclass defaults, module-level globals). We install a
# stub so any later `.get(key, default=...)` returns the supplied default,
# making the test suite fully deterministic without touching a real backend.


class _StubVolnuxConfig:
    """Deterministic stand-in for VolnuxConfig used by the event test suite."""

    _instance: Optional["_StubVolnuxConfig"] = None

    def __init__(self) -> None:
        self._overrides: Dict[str, Any] = {}

    @classmethod
    def get_instance(cls) -> "_StubVolnuxConfig":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get(self, key: str, default: Any = None) -> Any:
        return self._overrides.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Install a per-key override (for tests that flip flags)."""
        self._overrides[key] = value

    def reset(self) -> None:
        self._overrides.clear()


def _install_volnux_config_stub() -> None:
    """
    Install `_StubVolnuxConfig` as `volnux.config.VolnuxConfig`.

    Two cases:
      1. `volnux` is already importable on sys.path → import the real
         `volnux.config` module and swap `VolnuxConfig` on it. This keeps
         every other attribute of the real module intact.
      2. `volnux` is not yet importable → register a synthetic
         `volnux` + `volnux.config` package pair in sys.modules so that
         subsequent `from volnux.config import VolnuxConfig` succeeds.
    """
    try:
        real = importlib.import_module("volnux.config")
        real.VolnuxConfig = _StubVolnuxConfig  # type: ignore[attr-defined]
        return
    except ImportError:
        pass

    # Fall back to synthesising the package — needed only when conftest runs
    # before volnux is on sys.path. Tests that import other volnux submodules
    # will still fail in that case, which is the desired behaviour.
    if "volnux" not in sys.modules:
        volnux_pkg = types.ModuleType("volnux")
        volnux_pkg.__path__ = []  # mark as package
        sys.modules["volnux"] = volnux_pkg

    if "volnux.config" not in sys.modules:
        cfg_mod = types.ModuleType("volnux.config")
        cfg_mod.VolnuxConfig = _StubVolnuxConfig  # type: ignore[attr-defined]
        sys.modules["volnux.config"] = cfg_mod


_install_volnux_config_stub()


# ---------------------------------------------------------------------------
# 3. Lazy import of EventBase (defers failure to fixture resolution time)
# ---------------------------------------------------------------------------
# We do NOT import EventBase at conftest collection time. Doing so would fail
# collection entirely if the path differs. Instead, fixtures import it lazily
# so the failure surfaces as a clear per-test error.

_DUMMY_EVENT_CACHE: Dict[str, type] = {}


def _load_event_base():
    """Import and return (EventBase, EventMeta, EventType, EventCategory,
    get_event_registry) from the configured module path.

    Tries VOLNUX_EVENT_BASE_MODULE first, then a list of common fallbacks.
    """
    candidates = [
        _EVENT_BASE_MODULE,
        "volnux.event.base",
        "volnux.event",
        "volnux.core.event",
    ]
    seen = set()
    last_err: Optional[Exception] = None
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        try:
            mod = importlib.import_module(path)
            if hasattr(mod, "EventBase"):
                return (
                    mod.EventBase,
                    getattr(mod, "EventMeta", None),
                    getattr(mod, "EventType", None),
                    getattr(mod, "EventCategory", None),
                    getattr(mod, "get_event_registry", None),
                )
        except Exception as e:  # noqa: BLE001 — keep trying
            last_err = e
    raise ImportError(
        f"Could not import EventBase from any of {candidates}. "
        f"Last error: {last_err!r}. Set VOLNUX_EVENT_BASE_MODULE to the "
        f"correct dotted path."
    )


# ---------------------------------------------------------------------------
# 4. Registry isolation — snapshot & restore around every test
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _isolate_event_registry():
    """Snapshot & restore the global event registry around every test.

    EventMeta auto-registers every concrete subclass; without isolation,
    throw-away event classes created in tests would pollute the shared
    registry. We snapshot the registered-class set + records dict before
    the test and remove any new entries on teardown.
    """
    try:
        from volnux.event.base import get_event_registry
    except Exception:
        try:
            from volnux.event import get_event_registry  # type: ignore
        except Exception:
            yield
            return

    registry = get_event_registry()
    before_classes = set(getattr(registry, "_classes", set()) or set())
    before_records = dict(getattr(registry, "_records", {}) or {})

    yield

    # Restore by removing any classes/records added during the test.
    if hasattr(registry, "_classes"):
        new = set(registry._classes) - before_classes
        for cls in new:
            try:
                registry._classes.discard(cls)
            except Exception:
                pass
    if hasattr(registry, "_records"):
        registry._records.clear()
        registry._records.update(before_records)


# ---------------------------------------------------------------------------
# 5. Asyncio configuration
# ---------------------------------------------------------------------------


@pytest.fixture
def event_loop():
    """Yield a fresh event loop per test."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# ---------------------------------------------------------------------------
# 6. Concrete dummy event subclass + builder fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def dummy_event_cls():
    """Return a minimal concrete EventBase subclass.

    The class is built lazily per-test (cached on the module dict) so that
    registry isolation actually has a chance to clean it up.
    """
    EventBase, *_ = _load_event_base()

    class _DummyEvent(EventBase):  # type: ignore[misc, valid-type]
        """Minimal concrete EventBase used by the suite."""

        async def process(self, *args, **kwargs):
            return True, {"ok": True}

    return _DummyEvent


@pytest.fixture
def execution_context():
    """A MagicMock standing in for ExecutionContext."""
    ctx = MagicMock(name="ExecutionContext")
    return ctx


@pytest.fixture
def checkpoint_manager():
    """A MagicMock standing in for VolnuxCheckPointManager."""
    mgr = MagicMock(name="VolnuxCheckPointManager")
    mgr.enqueue = MagicMock()
    return mgr


@pytest.fixture
def command_channel():
    """A MagicMock standing in for CommandChannelBase.

    Has async `send_message` and async `receive_command` stubs. By default
    `receive_command` returns None after the timeout (no command available).
    """
    ch = MagicMock(name="CommandChannelBase")

    async def _send_message(_msg):
        return None

    async def _receive_command(timeout=0.01):
        await asyncio.sleep(timeout)
        return None

    ch.send_message = MagicMock(side_effect=_send_message)
    ch.receive_command = MagicMock(side_effect=_receive_command)
    return ch


@pytest.fixture
def make_event(dummy_event_cls, execution_context):
    """Factory that builds a fully-initialized dummy event instance."""
    from volnux.constants import EMPTY
    from volnux.parser.options import StopCondition

    def _make(
        *,
        task_id: str = "test-task-id",
        checkpoint_manager: Any = None,
        command_channel: Any = None,
        previous_result: Any = EMPTY,
        stop_condition: Any = StopCondition.NEVER,
        run_bypass_event_checks: bool = False,
        options: Any = None,
        sequence_number: Optional[int] = None,
    ):
        return dummy_event_cls(
            execution_context=execution_context,
            task_id=task_id,
            checkpoint_manager=checkpoint_manager,
            command_channel=command_channel,
            previous_result=previous_result,
            stop_condition=stop_condition,
            run_bypass_event_checks=run_bypass_event_checks,
            options=options,
            sequence_number=sequence_number,
        )

    return _make


@pytest.fixture
def event(make_event, checkpoint_manager):
    """A ready-to-use fully-initialized dummy event instance."""
    return make_event(checkpoint_manager=checkpoint_manager)


# ---------------------------------------------------------------------------
# 7. VolnuxConfig stub accessor
# ---------------------------------------------------------------------------


@pytest.fixture
def volnux_config_stub():
    """Expose the stub VolnuxConfig instance so tests can install overrides."""
    stub = _StubVolnuxConfig.get_instance()
    stub.reset()
    yield stub
    stub.reset()


# ---------------------------------------------------------------------------
# 8. Helpers for steps_runner tests
# ---------------------------------------------------------------------------


@pytest.fixture
def make_step():
    """Build a step function decorated with `_phase` for steps_runner tests."""
    import inspect as _inspect

    def _make(name="step", phase=None, body=None, *, raises=None):
        async def _step(*args, **kwargs):
            if raises is not None:
                raise raises
            if body is not None:
                res = body(*args, **kwargs)
                if _inspect.isawaitable(res):
                    res = await res
                return res
            return None

        _step.__name__ = name
        _step._phase = phase  # type: ignore[attr-defined]
        return _step

    return _make
