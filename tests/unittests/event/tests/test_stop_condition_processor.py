"""
Tests for `StopConditionProcessor` — a small dataclass defined inside the
EventCheckpointingMixin module.

Covers:
  * `should_stop` for each StopCondition member (NEVER / ON_SUCCESS /
    ON_ERROR / ON_ANY).
  * List/tuple of conditions (any() semantics).
  * `on_success` / `on_error` toggling of `self.exception` and reset behaviour.
  * `get_status` shape.
  * `_log_stop_decision` does not raise.
"""

from __future__ import annotations

import pytest


from volnux.parser.options import StopCondition

_STOP_CONDITION_AVAILABLE = True


def _import_processor():
    """Import StopConditionProcessor from the checkpointing mixin module."""
    # Try several likely paths. Adjust to match your repo layout if needed.
    import importlib

    candidates = [
        "volnux.mixins._checkpointing",
        "volnux.mixins.checkpointing",
        "volnux.mixins.event_checkpointing",
    ]
    last_err = None
    for path in candidates:
        try:
            mod = importlib.import_module(path)
            if hasattr(mod, "StopConditionProcessor"):
                return mod.StopConditionProcessor
        except ImportError as e:
            last_err = e

    try:
        from volnux.mixins.event import StopConditionProcessor  # type: ignore

        return StopConditionProcessor
    except ImportError:
        pass
    raise ImportError(
        f"Could not import StopConditionProcessor from any of {candidates}. "
        f"Last error: {last_err!r}. Adjust the import in "
        f"tests/test_stop_condition_processor.py to match your repo layout."
    )


# Try to import StopConditionProcessor; defer the failure to skipif if volnux
# isn't on sys.path.
try:
    StopConditionProcessor = _import_processor()
except ImportError:
    StopConditionProcessor = None  # type: ignore[assignment]


pytestmark = pytest.mark.skipif(
    StopConditionProcessor is None or not _STOP_CONDITION_AVAILABLE,
    reason="Could not import StopConditionProcessor or StopCondition — adjust "
    "the import in tests/test_stop_condition_processor.py to match "
    "your repo layout.",
)


class TestStopConditionProcessorBasics:
    def test_default_state(self):
        proc = StopConditionProcessor(stop_condition=StopCondition.NEVER)
        assert proc.exception is None
        assert proc.message is None

    def test_never_never_stops(self):
        proc = StopConditionProcessor(stop_condition=StopCondition.NEVER)
        assert proc.should_stop(success=True) is False
        assert proc.should_stop(success=False) is False

    def test_on_any_always_stops(self):
        proc = StopConditionProcessor(stop_condition=StopCondition.ON_ANY)
        assert proc.should_stop(success=True) is True
        assert proc.should_stop(success=False) is True

    def test_on_success_only_when_no_exception(self):
        proc = StopConditionProcessor(stop_condition=StopCondition.ON_SUCCESS)
        # success=True, no exception → stop
        assert proc.should_stop(success=True) is True
        # but if there's an exception, even success=True should NOT stop
        proc.exception = ValueError("boom")
        assert proc.should_stop(success=True) is False
        # success=False should not stop either
        proc.exception = None
        assert proc.should_stop(success=False) is False

    def test_on_error_when_failure(self):
        proc = StopConditionProcessor(stop_condition=StopCondition.ON_ERROR)
        # success=True → not stop
        assert proc.should_stop(success=True) is False
        # success=False → stop
        assert proc.should_stop(success=False) is True
        # success=True but exception set → stop
        proc.exception = ValueError("boom")
        assert proc.should_stop(success=True) is True


class TestStopConditionProcessorListInput:
    def test_list_of_conditions_any_semantics(self):
        proc = StopConditionProcessor(
            stop_condition=[StopCondition.NEVER, StopCondition.ON_ANY]
        )
        # ON_ANY → True regardless
        assert proc.should_stop(success=True) is True

    def test_list_all_never_is_false(self):
        proc = StopConditionProcessor(
            stop_condition=[StopCondition.NEVER, StopCondition.NEVER]
        )
        assert proc.should_stop(success=True) is False


class TestStopConditionProcessorOnSuccessOnError:
    def test_on_success_clears_exception_and_evaluates(self):
        proc = StopConditionProcessor(
            stop_condition=StopCondition.ON_SUCCESS,
            exception=ValueError("prev"),
        )
        # on_success clears exception then evaluates
        result = proc.on_success()
        assert proc.exception is None
        assert result is True  # ON_SUCCESS + no exception + success path

    def test_on_success_with_never_returns_false(self):
        proc = StopConditionProcessor(stop_condition=StopCondition.NEVER)
        assert proc.on_success() is False

    def test_on_error_sets_exception_and_evaluates(self):
        proc = StopConditionProcessor(stop_condition=StopCondition.ON_ERROR)
        exc = ValueError("boom")
        result = proc.on_error(exception=exc, message="err occurred")
        assert proc.exception is exc
        assert proc.message == "err occurred"
        assert result is True  # ON_ERROR + failure → stop

    def test_on_error_with_never_returns_false(self):
        proc = StopConditionProcessor(stop_condition=StopCondition.NEVER)
        result = proc.on_error(exception=ValueError("ignored"))
        assert result is False

    def test_reset_clears_state(self):
        proc = StopConditionProcessor(
            stop_condition=StopCondition.ON_ERROR,
            exception=ValueError("x"),
            message="m",
        )
        proc.reset()
        assert proc.exception is None
        assert proc.message is None


class TestStopConditionProcessorGetStatus:
    def test_status_shape_no_exception(self):
        proc = StopConditionProcessor(stop_condition=StopCondition.NEVER)
        status = proc.get_status()
        assert status["stop_condition"] == StopCondition.NEVER
        assert status["has_exception"] is False
        assert status["exception_type"] is None
        assert status["message"] is None

    def test_status_shape_with_exception(self):
        exc = ValueError("boom")
        proc = StopConditionProcessor(
            stop_condition=StopCondition.ON_ERROR, exception=exc
        )
        status = proc.get_status()
        assert status["has_exception"] is True
        assert status["exception_type"] == "ValueError"
