import pytest

from volnux.decorators import listener
from volnux.signal.signals import GenericSender, SoftSignal

from .conftest import OtherSender, Sender


def test_listener_receives_sender_signal_and_args(signal):
    def handler(sender, signal, value):
        return sender, signal, value

    signal.connect(Sender, handler)
    [(fn, response)] = signal.emit(Sender, value=1)
    assert fn is handler and response == (Sender, signal, 1)


def test_response_pairs_are_in_connection_order(signal):
    calls = []

    def first(**kw):
        calls.append("first")

    def second(**kw):
        calls.append("second")

    signal.connect(Sender, first)
    signal.connect(None, second)
    result = signal.emit(Sender, value=1)
    assert [fn for fn, _ in result] == [first, second]
    assert calls == ["first", "second"]  # specific sender before wildcard


def test_listener_only_called_for_its_sender(signal):
    def handler(**kw):
        return "hit"

    signal.connect(Sender, handler)
    assert signal.emit(OtherSender, value=1) == []


def test_none_sender_is_wildcard(signal):
    def handler(**kw):
        return kw["sender"]

    signal.connect(None, handler)
    assert signal.emit(Sender, value=1)[0][1] is Sender
    assert signal.emit(OtherSender, value=1)[0][1] is OtherSender


def test_emit_with_none_or_generic_sender_runs_wildcard_once(signal):
    calls = []

    def handler(**kw):
        calls.append(1)

    signal.connect(None, handler)
    signal.emit(None, value=1)
    signal.emit(GenericSender, value=1)
    assert len(calls) == 2


def test_emit_without_listeners_returns_empty_list(signal):
    assert signal.emit(Sender, value=1) == []


def test_failing_listener_returns_exception_and_does_not_stop_others(signal):
    def bad(**kw):
        raise RuntimeError("boom")

    def good(**kw):
        return "ok"

    signal.connect(Sender, bad)
    signal.connect(Sender, good)
    responses = dict((fn, r) for fn, r in signal.emit(Sender, value=1))
    assert isinstance(responses[bad], RuntimeError)
    assert responses[good] == "ok"


def test_bad_emit_arguments_do_not_raise(signal):
    def handler(**kw):
        return "ok"

    signal.connect(Sender, handler)
    [(_, response)] = signal.emit(Sender, unexpected=1)  # missing "value"
    assert isinstance(response, TypeError)


def test_listener_without_kwargs_gets_only_bound_arguments(signal):
    def handler(sender, signal, value):
        return value

    signal.connect(Sender, handler)
    assert signal.emit(Sender, value=7)[0][1] == 7


def test_pipeline_and_process_ids_are_optional_but_forwarded(signal):
    def handler(sender, signal, value, pipeline_id=None, process_id=None):
        return pipeline_id, process_id

    signal.connect(Sender, handler)
    assert signal.emit(Sender, value=1, pipeline_id="p", process_id=2)[0][1] == ("p", 2)


def test_connecting_same_listener_twice_calls_once(signal):
    calls = []

    def handler(**kw):
        calls.append(1)

    signal.connect(Sender, handler)
    signal.connect(Sender, handler)
    signal.emit(Sender, value=1)
    assert len(calls) == 1


def test_bound_method_listener(signal):
    class Receiver:
        def on_signal(self, **kw):
            return kw["value"]

    receiver = Receiver()
    signal.connect(Sender, receiver.on_signal)
    assert signal.emit(Sender, value=3)[0][1] == 3


def test_connect_rejects_non_callable_and_unhashable_sender(signal):
    with pytest.raises(TypeError):
        signal.connect(Sender, "not callable")
    with pytest.raises(TypeError, match="hashable"):
        signal.connect([], lambda **kw: None)
    with pytest.raises(TypeError, match="hashable"):
        signal.emit({}, value=1)


def test_instance_senders_work(signal):
    a, b = Sender(), Sender()

    def handler(**kw):
        return kw["sender"]

    signal.connect(a, handler)
    assert signal.emit(a, value=1)[0][1] is a
    assert signal.emit(b, value=1) == []


def test_provide_args_validation(make_signal):
    with pytest.raises(TypeError):
        SoftSignal("bad_signal_args", provide_args="abc")
    assert {"sender", "signal", "x"} == make_signal("x")._provide_args


class TestListenerDecorator:
    def test_single_signal_with_sender(self, signal):
        @listener(signal, sender=Sender)
        def handler(sender, signal, value):
            return value

        assert signal.emit(Sender, value=5)[0][1] == 5
        assert signal.emit(OtherSender, value=5) == []

    def test_multiple_signals(self, make_signal):
        s1, s2 = make_signal("value"), make_signal("value")

        @listener([s1, s2], sender=Sender)
        def handler(**kw):
            return kw["value"]

        assert s1.emit(Sender, value=1)[0][1] == 1
        assert s2.emit(Sender, value=2)[0][1] == 2

    def test_decorator_returns_original_function(self, signal):
        def handler(**kw):
            return 1

        assert listener(signal)(handler) is handler
