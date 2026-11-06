import pytest

from volnux.mixins.event.checkpointer import StopConditionProcessor
from volnux.parser.options import StopCondition


def proc(condition):
    return StopConditionProcessor(stop_condition=condition)


class TestSingleCondition:
    def test_never_never_stops(self):
        p = proc(StopCondition.NEVER)
        assert not p.on_success() and not p.on_error(ValueError("x"))

    def test_on_success(self):
        p = proc(StopCondition.ON_SUCCESS)
        assert p.on_success()
        assert not p.on_error(ValueError("x"))

    def test_on_error(self):
        p = proc(StopCondition.ON_ERROR)
        assert not p.on_success()
        assert p.on_error(ValueError("x"))

    def test_on_any(self):
        p = proc(StopCondition.ON_ANY)
        assert p.on_success() and p.on_error(ValueError("x"))

    def test_on_exception_is_not_evaluated_here(self):
        # ON_EXCEPTION is part of the enum but unknown to the processor (logged, ignored)
        p = proc(StopCondition.ON_EXCEPTION)
        assert not p.on_success() and not p.on_error(ValueError("x"))


class TestShouldStop:
    def test_depends_on_the_success_flag(self):
        p = proc(StopCondition.ON_ERROR)
        assert p.should_stop(success=False) and not p.should_stop(success=True)

    def test_a_recorded_exception_counts_as_an_error_even_if_success_is_passed(self):
        p = proc(StopCondition.ON_ERROR)
        p.exception = ValueError("x")
        assert p.should_stop(success=True)

    def test_a_recorded_exception_prevents_on_success_from_stopping(self):
        p = proc(StopCondition.ON_SUCCESS)
        p.exception = ValueError("x")
        assert not p.should_stop(success=True)


class TestListsOfConditions:
    def test_any_matching_condition_stops(self):
        p = proc([StopCondition.ON_SUCCESS, StopCondition.ON_ERROR])
        assert p.on_success() and p.on_error(ValueError("x"))

    def test_only_matching_conditions_count(self):
        p = proc([StopCondition.NEVER, StopCondition.ON_ERROR])
        assert not p.on_success() and p.on_error(ValueError("x"))

    def test_tuple_is_accepted(self):
        assert proc((StopCondition.ON_SUCCESS,)).on_success()

    def test_empty_list_never_stops(self):
        p = proc([])
        assert not p.on_success() and not p.on_error(ValueError("x"))

    def test_single_never_in_a_list(self):
        assert not proc([StopCondition.NEVER]).on_success()


class TestState:
    def test_on_success_clears_a_previous_exception(self):
        p = proc(StopCondition.NEVER)
        p.on_error(ValueError("x"))
        p.on_success()
        assert p.exception is None

    def test_on_error_records_exception_and_message(self):
        p = proc(StopCondition.NEVER)
        error = ValueError("x")
        p.on_error(error, message="boom")
        assert p.exception is error and p.message == "boom"

    def test_on_error_keeps_the_old_message_when_none_is_given(self):
        p = proc(StopCondition.NEVER)
        p.message = "kept"
        p.on_error(ValueError("x"))
        assert p.message == "kept"

    def test_reset(self):
        p = proc(StopCondition.NEVER)
        p.on_error(ValueError("x"), message="m")
        p.reset()
        assert p.exception is None and p.message is None

    def test_get_status(self):
        p = proc(StopCondition.ON_ERROR)
        assert p.get_status() == {
            "stop_condition": StopCondition.ON_ERROR,
            "has_exception": False,
            "exception_type": None,
            "message": None,
        }
        p.on_error(KeyError("k"), message="m")
        status = p.get_status()
        assert status["has_exception"] and status["exception_type"] == "KeyError"
        assert status["message"] == "m"
