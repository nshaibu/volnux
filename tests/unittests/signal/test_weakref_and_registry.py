import gc
import weakref

import pytest

from volnux.signal import signals
from volnux.signal.signals import SoftSignal, get_signal

from .conftest import Sender


class TestWeakReferences:
    def test_listener_dropped_when_function_collected(self, signal):
        def handler(**kw):
            return 1

        signal.connect(Sender, handler)
        del handler
        gc.collect()
        assert signal.emit(Sender, value=1) == []
        assert Sender not in signal._listeners

    def test_bound_method_dropped_when_owner_collected(self, signal):
        class Receiver:
            def on(self, **kw):
                return 1

        r = Receiver()
        signal.connect(Sender, r.on)
        assert len(signal.emit(Sender, value=1)) == 1
        del r
        gc.collect()
        assert signal.emit(Sender, value=1) == []

    def test_signal_does_not_keep_listener_alive(self, signal):
        def handler(**kw):
            return 1

        probe = weakref.ref(handler)
        signal.connect(Sender, handler)
        del handler
        gc.collect()
        assert probe() is None

    def test_falsy_listener_is_still_called(self, signal):
        class FalsyListener:
            def __bool__(self):
                return False

            def __call__(self, **kw):
                return "called"

        obj = FalsyListener()
        signal.connect(Sender, obj)
        assert signal.emit(Sender, value=1)[0][1] == "called"

    def test_disconnect_detaches_finalizer(self, signal):
        def handler(**kw):
            return 1

        signal.connect(Sender, handler)
        finalizer = next(iter(signal._listeners[Sender].values()))
        assert finalizer.alive
        signal.disconnect(Sender, handler)
        assert not finalizer.alive

    def test_clean_detaches_finalizers(self, signal):
        def handler(**kw):
            return 1

        signal.connect(Sender, handler)
        finalizer = next(iter(signal._listeners[Sender].values()))
        signal.clean(Sender)
        assert not finalizer.alive

    def test_reconnect_does_not_stack_finalizers(self, signal):
        def handler(**kw):
            return 1

        signal.connect(Sender, handler)
        signal.connect(Sender, handler)
        assert len(signal._listeners[Sender]) == 1

    def test_finalizer_runs_while_lock_held_on_same_thread(self, signal):
        # RLock: a GC-triggered removal inside connect/disconnect must not deadlock.
        def handler(**kw):
            return 1

        signal.connect(Sender, handler)
        ref = next(iter(signal._listeners[Sender]))
        with signal.lock:
            signal._remove_dead_listener(Sender, ref)
        assert Sender not in signal._listeners


class TestRegistry:
    def test_get_signal_returns_signal_instance(self):
        assert get_signal("pipeline_pre_init") is signals.pipeline_pre_init

    def test_get_signal_unknown_returns_none(self):
        assert get_signal("does_not_exist") is None
        assert get_signal("pipeline_pre_init", module_path="other.module") is None

    def test_new_signal_is_registered(self, signal):
        assert get_signal(signal.name) is signal
        assert signal.__instance_import_str__ in SoftSignal.registered_signals()

    def test_registry_does_not_keep_signals_alive(self, make_signal):
        sig = make_signal()
        name = sig.name
        del sig
        gc.collect()
        assert get_signal(name) is None

    @pytest.mark.parametrize(
        "name, args",
        [
            ("pipeline_pre_init", {"args", "kwargs"}),
            ("pipeline_post_init", {"pipeline"}),
            ("event_init", {"event", "init_kwargs"}),
            (
                "event_called",
                {"event", "init_kwargs", "call_kwargs", "hook_type", "result"},
            ),
        ],
    )
    def test_builtin_signals(self, name, args):
        sig = get_signal(name)
        assert sig.name == name
        assert sig._provide_args == args | {"sender", "signal"}
        assert sig.__instance_import_str__ == "volnux.signal.signals." + name

    def test_all_module_level_signals_are_registered(self):
        for attr, value in vars(signals).items():
            if isinstance(value, SoftSignal):
                assert get_signal(value.name) is value, attr

    def test_pickle_roundtrip_drops_listeners(self, signal):
        import pickle

        def handler(**kw):
            return 1

        signal.connect(Sender, handler)
        clone = pickle.loads(pickle.dumps(signal))
        assert clone.name == signal.name and clone._listeners == {}
        assert clone.emit(Sender, value=1) == []
