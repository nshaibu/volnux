from volnux.signal.signals import GenericSender

from .conftest import OtherSender, Sender


def test_disconnect_removes_listener(signal):
    def handler(**kw):
        return 1

    signal.connect(Sender, handler)
    signal.disconnect(Sender, handler)
    assert signal.emit(Sender, value=1) == []
    assert Sender not in signal._listeners


def test_disconnect_none_removes_wildcard_listener(signal):
    def handler(**kw):
        return 1

    signal.connect(None, handler)
    signal.disconnect(None, handler)
    assert signal.emit(Sender, value=1) == []
    assert GenericSender not in signal._listeners


def test_disconnect_only_removes_given_listener(signal):
    def one(**kw):
        return 1

    def two(**kw):
        return 2

    signal.connect(Sender, one)
    signal.connect(Sender, two)
    signal.disconnect(Sender, one)
    assert [r for _, r in signal.emit(Sender, value=1)] == [2]


def test_disconnect_unknown_listener_or_sender_is_noop(signal):
    def handler(**kw):
        return 1

    signal.connect(Sender, handler)
    signal.disconnect(OtherSender, handler)
    signal.disconnect(Sender, lambda **kw: None)
    assert len(signal.emit(Sender, value=1)) == 1


def test_disconnect_bound_method(signal):
    class Receiver:
        def on(self, **kw):
            return 1

    r = Receiver()
    signal.connect(Sender, r.on)
    signal.disconnect(Sender, r.on)
    assert signal.emit(Sender, value=1) == []


def test_clean_removes_all_listeners_for_sender(signal):
    def a(**kw):
        return 1

    def b(**kw):
        return 2

    signal.connect(Sender, a)
    signal.connect(Sender, b)
    signal.connect(OtherSender, a)
    signal.clean(Sender)
    assert signal.emit(Sender, value=1) == []
    assert len(signal.emit(OtherSender, value=1)) == 1


def test_clean_none_clears_wildcard(signal):
    def a(**kw):
        return 1

    signal.connect(None, a)
    signal.clean(None)
    assert signal.emit(Sender, value=1) == []


def test_clean_unknown_sender_is_noop(signal):
    signal.clean(Sender)
