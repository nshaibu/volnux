import itertools

import pytest

from volnux import EventBase
from volnux.signal.signals import event_init


_names = itertools.count()


def make_event(schema, name=None):
    name = name or "InitEv%d" % next(_names)
    cls = type(
        name,
        (EventBase,),
        {"INIT_PARAMS_SCHEMA": schema, "process": lambda s, *a, **k: (True, None)},
    )
    return cls, object.__new__(cls)


def init(cls, event, **init_kwargs):
    """Emit like EventCheckpointingMixin does: sender is the concrete class."""
    [(_, response)] = event_init.emit(sender=cls, event=event, init_kwargs=init_kwargs)
    return response


def test_listener_fires_for_event_subclasses():
    cls, ev = make_event({"foo": {}})
    assert [
        fn.__name__ for fn, _ in event_init.emit(sender=cls, event=ev, init_kwargs={})
    ] == ["inject_event_initialisation_extra_params"]


def test_value_is_injected_as_attribute():
    cls, ev = make_event({"foo": {"type": int}})
    assert init(cls, ev, foo=3) is None
    assert ev.foo == 3


def test_event_without_schema_is_untouched():
    cls, ev = make_event({})
    assert init(cls, ev, foo=1) is None
    assert not hasattr(ev, "foo")


def test_required_missing_returns_type_error():
    cls, ev = make_event({"foo": {"required": True}})
    assert isinstance(init(cls, ev), TypeError)


def test_default_and_default_factory():
    cls, ev = make_event({"a": {"default": 5}, "b": {"default_factory": list}})
    init(cls, ev)
    assert ev.a == 5 and ev.b == []


def test_passed_value_overrides_default():
    cls, ev = make_event({"a": {"default": 5}})
    init(cls, ev, a=9)
    assert ev.a == 9


def test_failing_default_factory_is_runtime_error_with_cause():
    def factory():
        raise KeyError("k")

    cls, ev = make_event({"a": {"default_factory": factory}})
    err = init(cls, ev)
    assert isinstance(err, RuntimeError) and isinstance(err.__cause__, KeyError)


def test_choices():
    cls, ev = make_event({"mode": {"choices": ["a", "b"]}})
    assert isinstance(init(cls, ev, mode="c"), ValueError)
    cls, ev = make_event({"mode": {"choices": ["a", "b"]}})
    assert init(cls, ev, mode="a") is None and ev.mode == "a"


def test_validator_failure_keeps_type_and_chains_cause():
    def positive(value, name, event):
        if value <= 0:
            raise ValueError("must be positive")

    cls, ev = make_event({"n": {"validators": [positive]}})
    err = init(cls, ev, n=-1)
    assert isinstance(err, ValueError)
    assert "Validation failed for parameter 'n'" in str(err)
    assert isinstance(err.__cause__, ValueError)
    assert not hasattr(ev, "n")


def test_validator_with_unconstructible_exception_type_falls_back_to_value_error():
    class WeirdError(Exception):
        def __init__(self, a, b):
            super().__init__(a, b)

    def bad(value, name, event):
        raise WeirdError(1, 2)

    cls, ev = make_event({"n": {"validators": [bad]}})
    assert isinstance(init(cls, ev, n=1), ValueError)


def test_protected_names_are_skipped_with_warning():
    cls, ev = make_event({"process": {"default": 1}})
    with pytest.warns(UserWarning, match="process"):
        init(cls, ev)
    assert ev.process != 1
