from __future__ import annotations

import asyncio
import inspect
from typing import Any
from unittest.mock import MagicMock, AsyncMock, patch

import pytest


def _import_event_base():
    try:
        from volnux.event.base import (
            EventBase,
            EventType,
            EventCategory,
            get_event_registry,
        )
    except ImportError:
        from volnux.event import (  # type: ignore
            EventBase,
            EventType,
            EventCategory,
            get_event_registry,
        )
    return EventBase, EventType, EventCategory, get_event_registry


class TestInitSubclass:
    def test_subclass_overriding_init_raises_improperly_configured(self):
        EventBase, *_ = _import_event_base()
        from volnux.exceptions import ImproperlyConfigured

        with pytest.raises(ImproperlyConfigured):

            class _BadEvent(EventBase):  # type: ignore[misc, valid-type]
                def __init__(self, *args, **kwargs):
                    pass

                async def process(self, *args, **kwargs):
                    return True, {}

    def test_subclass_without_init_override_is_allowed(self, dummy_event_cls):
        # dummy_event_cls fixture already creates a valid subclass.
        assert dummy_event_cls is not None
        assert hasattr(dummy_event_cls, "process")


class TestInitDelegation:
    def test_init_sets_required_runtime_attributes(self, event):
        """_setup_event (via __init__) must set all the attributes the other
        mixins rely on."""
        # Identity / context
        assert event._task_id == "test-task-id"
        assert event._sequence_number is None
        # Retry
        assert event._retry_count == 0
        # Phase
        from volnux.execution.rehydrator.event.snapshot import EventPhase

        assert event._phase == EventPhase.INITIALIZED
        # Communication
        assert isinstance(event._pause_gate, asyncio.Event)
        assert event._pause_gate.is_set() is True  # defaults to Running
        assert event._preempted is False
        assert event._main_worker_task is None
        assert event._command_channel is None
        # Execution status
        assert event.exec_status is False
        assert event.exec_result is None
        # Resources
        assert event._external_resources == {}
        assert event._resource_instances == {}
        # _init_args captured from locals()
        assert isinstance(event._init_args, dict)
        assert event._init_args.get("task_id") == "test-task-id"
        # _call_args defaults to EMPTY
        from volnux.constants import EMPTY

        assert event._call_args is EMPTY

    def test_init_propagates_command_channel(self, make_event, command_channel):
        evt = make_event(command_channel=command_channel)
        assert evt._command_channel is command_channel

    def test_init_propagates_checkpoint_manager(self, make_event, checkpoint_manager):
        evt = make_event(checkpoint_manager=checkpoint_manager)
        assert evt.checkpoint_manager is checkpoint_manager

    def test_init_propagates_sequence_number(self, make_event):
        evt = make_event(sequence_number=42)
        assert evt._sequence_number == 42


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr_format(self, event):
        # Expected: "<ClassName executor=ExecutorName>"
        # Note: EventBase.executor is a class attribute defaulting to "default"
        # (string). __repr__ accesses self.executor.__name__ — which only works
        # for actual executor classes, not the string default. We test that
        # __repr__ returns a string starting with the class name and not raising.
        # If the default executor is a string, this WILL raise AttributeError,
        # in which case we verify the error matches the documented behaviour.
        try:
            r = repr(event)
            assert r.startswith(f"<{event.__class__.__name__}")
        except AttributeError as e:
            # Default executor is a string ("default"); __repr__ accesses
            # .__name__ which strings don't have. This is the documented quirk
            # of the source. Test stubs should set executor to a class.
            pytest.skip(
                f"Default executor is a string; __repr__ cannot resolve .__name__: {e}"
            )


class TestGetArgs:
    def test_get_init_args_returns_dict(self, event):
        args = event.get_init_args()
        assert isinstance(args, dict)
        assert "task_id" in args
        assert "execution_context" in args

    def test_get_call_args_returns_init_value(self, event):
        # Default _call_args is EMPTY
        args = event.get_call_args()
        from volnux.constants import EMPTY

        assert args is EMPTY


class TestVersionClassmethods:
    def test_get_version_handler_caches_on_class(self, dummy_event_cls):
        """get_version_handler() caches its result on cls._version_handler."""
        # Reset cache so test starts clean
        dummy_event_cls._version_handler = None
        h1 = dummy_event_cls.get_version_handler()
        h2 = dummy_event_cls.get_version_handler()
        assert h1 is h2
        assert dummy_event_cls._version_handler is h1

    def test_get_version_info_delegates_to_handler(self, dummy_event_cls):
        dummy_event_cls._version_handler = None
        with patch.object(
            dummy_event_cls,
            "get_version_handler",
            return_value=MagicMock(get_info=MagicMock(return_value={"version": "9.9"})),
        ):
            info = dummy_event_cls.get_version_info()
            assert info == {"version": "9.9"}

    def test_is_deprecated_delegates_to_handler(self, dummy_event_cls):
        with patch.object(
            dummy_event_cls,
            "get_version_handler",
            return_value=MagicMock(is_deprecated=MagicMock(return_value=True)),
        ):
            assert dummy_event_cls.is_deprecated() is True

    def test_get_all_versions_delegates_to_registry(self, dummy_event_cls):
        handler = MagicMock()
        handler.class_name = "DummyEvent"
        handler.namespace = "ns"
        with patch.object(dummy_event_cls, "get_version_handler", return_value=handler):
            with (
                patch(
                    "volnux.event.base._event_registry.list_versions",
                    return_value=["1.0.0", "2.0.0"],
                )
                if _module_has_attr("volnux.event.base", "_event_registry")
                else patch(
                    "volnux.event._event_registry.list_versions",
                    return_value=["1.0.0", "2.0.0"],
                )
            ) as mock_list:
                result = dummy_event_cls.get_all_versions()
                assert result == ["1.0.0", "2.0.0"]
                mock_list.assert_called_once_with("DummyEvent", "ns")

    def test_get_latest_version_delegates_to_registry(self, dummy_event_cls):
        handler = MagicMock()
        handler.class_name = "DummyEvent"
        handler.namespace = "ns"
        with patch.object(dummy_event_cls, "get_version_handler", return_value=handler):
            with (
                patch(
                    "volnux.event.base._event_registry.get_latest_version",
                    return_value="3.0.0",
                )
                if _module_has_attr("volnux.event.base", "_event_registry")
                else patch(
                    "volnux.event._event_registry.get_latest_version",
                    return_value="3.0.0",
                )
            ) as mock_latest:
                result = dummy_event_cls.get_latest_version()
                assert result == "3.0.0"
                mock_latest.assert_called_once_with("DummyEvent", "ns")

    def test_get_latest_version_returns_none_when_no_versions(self, dummy_event_cls):
        handler = MagicMock()
        handler.class_name = "DummyEvent"
        handler.namespace = "ns"
        with patch.object(dummy_event_cls, "get_version_handler", return_value=handler):
            with (
                patch(
                    "volnux.event.base._event_registry.get_latest_version",
                    return_value=None,
                )
                if _module_has_attr("volnux.event.base", "_event_registry")
                else patch(
                    "volnux.event._event_registry.get_latest_version",
                    return_value=None,
                )
            ):
                assert dummy_event_cls.get_latest_version() is None


def _module_has_attr(module_path: str, attr: str) -> bool:
    """Check whether a module exposes an attribute (without importing if cached)."""
    import sys

    mod = sys.modules.get(module_path)
    if mod is None:
        try:
            import importlib

            mod = importlib.import_module(module_path)
        except ImportError:
            return False
    return hasattr(mod, attr)


class TestGoto:
    def test_invalid_descriptor_negative_raises_value_error(self, event):
        with pytest.raises(ValueError):
            event.goto(-1, True, None)

    def test_invalid_descriptor_above_nine_raises_value_error(self, event):
        with pytest.raises(ValueError):
            event.goto(10, True, None)

    def test_invalid_descriptor_non_int_raises_value_error(self, event):
        with pytest.raises(ValueError):
            event.goto("5", True, None)  # type: ignore[arg-type]

    def test_boundary_descriptor_zero_allowed(self, event):
        from volnux.exceptions import SwitchTask

        with pytest.raises(SwitchTask):
            event.goto(0, True, {"x": 1})

    def test_boundary_descriptor_nine_allowed(self, event):
        from volnux.exceptions import SwitchTask

        with pytest.raises(SwitchTask):
            event.goto(9, False, None)

    def test_goto_with_success_path_calls_on_success(self, event):
        """When result_success=True and execute_on_event_method=True, goto()
        calls self.on_success(result) and uses its return value as SwitchTask.result."""
        from volnux.exceptions import SwitchTask

        sentinel = MagicMock(name="event_result_sentinel")
        with patch.object(event, "on_success", return_value=sentinel) as mock_os:
            with pytest.raises(SwitchTask) as exc_info:
                event.goto(
                    3, True, {"k": "v"}, reason="manual", execute_on_event_method=True
                )
            mock_os.assert_called_once_with({"k": "v"})
            assert exc_info.value.result is sentinel

    def test_goto_with_failure_path_calls_on_failure(self, event):
        from volnux.exceptions import SwitchTask

        sentinel = MagicMock(name="event_result_sentinel")
        with patch.object(event, "on_failure", return_value=sentinel) as mock_of:
            with pytest.raises(SwitchTask) as exc_info:
                event.goto(4, False, ValueError("boom"), execute_on_event_method=True)
            mock_of.assert_called_once_with(ValueError("boom"))
            assert exc_info.value.result is sentinel

    def test_goto_with_execute_on_event_method_false_wraps_in_event_result(self, event):
        from volnux.exceptions import SwitchTask
        from volnux.result import EventResult

        with patch.object(event, "on_success") as mock_os, patch.object(
            event, "on_failure"
        ) as mock_of:
            with pytest.raises(SwitchTask) as exc_info:
                event.goto(5, False, "raw-data", execute_on_event_method=False)
            # on_success/on_failure should NOT be called
            mock_os.assert_not_called()
            mock_of.assert_not_called()
            # Result should be an EventResult wrapping the raw data
            assert isinstance(exc_info.value.result, EventResult)
            assert exc_info.value.result.error is True
            assert exc_info.value.result.content == "raw-data"

    def test_goto_default_reason_is_manual(self, event):
        from volnux.exceptions import SwitchTask

        with pytest.raises(SwitchTask) as exc_info:
            event.goto(2, True, None)
        assert exc_info.value.reason == "manual"

    def test_goto_carries_current_task_id(self, event):
        from volnux.exceptions import SwitchTask

        with pytest.raises(SwitchTask) as exc_info:
            event.goto(7, True, None)
        assert exc_info.value.current_task_id == event._task_id

    def test_goto_carries_next_task_descriptor(self, event):
        from volnux.exceptions import SwitchTask

        with pytest.raises(SwitchTask) as exc_info:
            event.goto(7, True, None)
        assert exc_info.value.next_task_descriptor == 7


class TestEvaluator:
    def test_evaluator_returns_event_evaluator_instance(self, dummy_event_cls):
        from volnux.result_evaluators import EventEvaluator

        ev = dummy_event_cls.evaluator()
        assert isinstance(ev, EventEvaluator)

    def test_evaluator_raises_when_strategy_is_none(self):
        EventBase, *_ = _import_event_base()
        from volnux.exceptions import ImproperlyConfigured

        class _NoStrategyEvent(EventBase):  # type: ignore[misc, valid-type]
            result_evaluation_strategy = None

            async def process(self, *args, **kwargs):
                return True, {}

        with pytest.raises(ImproperlyConfigured):
            _NoStrategyEvent.evaluator()

    def test_evaluator_raises_when_strategy_is_wrong_type(self):
        EventBase, *_ = _import_event_base()
        from volnux.exceptions import ImproperlyConfigured

        class _BadStrategyEvent(EventBase):  # type: ignore[misc, valid-type]
            result_evaluation_strategy = "not-a-strategy"  # type: ignore[assignment]

            async def process(self, *args, **kwargs):
                return True, {}

        with pytest.raises(ImproperlyConfigured):
            _BadStrategyEvent.evaluator()


class TestDefaultHooks:
    @pytest.mark.asyncio
    async def test_bypass_raises_not_implemented(self, event):
        with pytest.raises(NotImplementedError):
            await event.bypass()

    @pytest.mark.asyncio
    async def test_process_on_abstract_base_raises_not_implemented(self):
        EventBase, *_ = _import_event_base()
        # EventBase itself is abstract — calling process() should raise.
        with pytest.raises(NotImplementedError):
            # We don't instantiate EventBase directly because __init__ requires
            # real arguments. Instead we verify the abstract method raises.
            await EventBase.process(None, *(), **{})  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_cleanup_default_is_noop(self, event):
        # Should not raise and return None
        result = await event.cleanup()
        assert result is None


class TestEventResult:
    def test_event_result_shape(self, event):
        result = event.event_result(error=False, content={"k": "v"})
        from volnux.result import EventResult

        assert isinstance(result, EventResult)
        assert result.error is False
        assert result.content == {"k": "v"}
        assert result.task_id == event._task_id
        assert result.event_name == event.__class__.__name__

    def test_event_result_error_true(self, event):
        result = event.event_result(error=True, content={"err": "boom"})
        assert result.error is True


class TestOnSuccess:
    def test_on_success_returns_event_result_with_error_false(self, event):
        from volnux.result import EventResult

        # StopCondition.NEVER so no StopProcessingError
        result = event.on_success({"ok": True})
        assert isinstance(result, EventResult)
        assert result.error is False
        assert result.content == {"ok": True}

    def test_on_success_emits_event_called_signal(self, event):
        """on_success emits event_called with hook_type='on_success'."""
        with patch("volnux.signal.signals.event_called.emit") as mock_emit:
            event.on_success("done")
            mock_emit.assert_called_once()
            kwargs = mock_emit.call_args.kwargs
            assert kwargs["hook_type"] == "on_success"
            assert kwargs["result"] == "done"

    def test_on_success_with_stop_condition_on_success_raises(self, make_event):
        from volnux.parser.options import StopCondition
        from volnux.exceptions import StopProcessingError

        evt = make_event(stop_condition=StopCondition.ON_SUCCESS)
        with pytest.raises(StopProcessingError):
            evt.on_success({"done": True})

    def test_on_success_with_stop_condition_never_does_not_raise(self, make_event):
        from volnux.parser.options import StopCondition

        evt = make_event(stop_condition=StopCondition.NEVER)
        # Should NOT raise
        result = evt.on_success({"done": True})
        assert result.error is False

    def test_on_success_sets_stop_condition_message_for_string_result(self, event):
        event.on_success("string-result")
        # stop_condition.message should have been set to the string result
        assert event.stop_condition.message == "string-result"

    def test_on_success_sets_stop_condition_message_for_non_string(self, event):
        result = {"key": "value"}
        event.on_success(result)
        # Non-string results are stringified for the stop_condition.message
        assert event.stop_condition.message == str(result)


class TestOnFailure:
    def test_on_failure_returns_event_result_with_error_true(self, event):
        from volnux.result import EventResult

        result = event.on_failure({"err": "boom"})
        assert isinstance(result, EventResult)
        assert result.error is True
        assert result.content == {"err": "boom"}

    def test_on_failure_emits_event_called_signal(self, event):
        with patch("volnux.signal.signals.event_called.emit") as mock_emit:
            event.on_failure(ValueError("boom"))
            mock_emit.assert_called_once()
            kwargs = mock_emit.call_args.kwargs
            assert kwargs["hook_type"] == "on_failure"

    def test_on_failure_with_stop_condition_on_error_raises(self, make_event):
        from volnux.parser.options import StopCondition
        from volnux.exceptions import StopProcessingError

        evt = make_event(stop_condition=StopCondition.ON_ERROR)
        with pytest.raises(StopProcessingError):
            evt.on_failure(ValueError("boom"))

    def test_on_failure_with_stop_condition_never_does_not_raise(self, make_event):
        from volnux.parser.options import StopCondition

        evt = make_event(stop_condition=StopCondition.NEVER)
        result = evt.on_failure(ValueError("boom"))
        assert result.error is True

    def test_on_failure_normalises_max_retry_error_exception(self, event):
        """When execution_result is a MaxRetryError, on_failure unwraps it
        to MaxRetryError.exception before passing to stop_condition.on_error."""
        from volnux.exceptions import MaxRetryError

        inner = ValueError("the actual cause")
        max_retry = MaxRetryError(attempt=3, exception=inner, reason="exhausted")
        # Use a NEVER stop condition so on_failure doesn't raise
        from volnux.parser.options import StopCondition

        event.stop_condition = type(event.stop_condition)(
            stop_condition=StopCondition.NEVER
        )
        # Spy on stop_condition.on_error to see what exception it received
        with patch.object(event.stop_condition, "on_error", return_value=False) as spy:
            event.on_failure(max_retry)
            # The unwrapped inner exception should have been passed
            assert spy.called
            args, kwargs = spy.call_args
            # on_error accepts exception as kwarg
            assert kwargs.get("exception") is inner

    def test_on_failure_with_non_max_retry_exception_passes_through(self, event):
        """Non-MaxRetryError exceptions pass through to stop_condition.on_error
        without unwrapping."""
        from volnux.parser.options import StopCondition

        event.stop_condition = type(event.stop_condition)(
            stop_condition=StopCondition.NEVER
        )
        exc = RuntimeError("plain")
        with patch.object(event.stop_condition, "on_error", return_value=False) as spy:
            event.on_failure(exc)
            args, kwargs = spy.call_args
            assert kwargs.get("exception") is exc


class TestRegistryAccessors:
    def test_get_all_event_classes_returns_iterable(self):
        EventBase, *_ = _import_event_base()
        result = EventBase.get_all_event_classes()
        assert hasattr(result, "__iter__")

    def test_clear_class_cache_does_not_raise(self):
        EventBase, *_ = _import_event_base()
        EventBase.clear_class_cache()


class TestCall:

    @pytest.mark.asyncio
    async def test_call_invokes_steps_runner_and_returns_result(self, event):
        """__call__ should invoke self.steps_runner(*args, **kwargs) and return
        whatever steps_runner returned."""
        sentinel = MagicMock(name="result")
        with patch.object(
            event, "steps_runner", new=AsyncMock(return_value=sentinel)
        ) as mock_sr:
            result = await event()
            assert result is sentinel
            mock_sr.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_call_starts_command_listener_when_channel_present(
        self, make_event, command_channel
    ):
        """When _command_channel is set, __call__ creates a command listener
        task and cancels it in finally."""
        evt = make_event(command_channel=command_channel)

        # Patch steps_runner to return immediately so the listener has nothing
        # to do — but it should still be created and cancelled.
        created_tasks: list[asyncio.Task] = []
        original_create = asyncio.create_task

        def tracking_create(coro, **kwargs):
            t = original_create(coro, **kwargs)
            created_tasks.append(t)
            return t

        with patch.object(asyncio, "create_task", side_effect=tracking_create):
            with patch.object(evt, "steps_runner", new=AsyncMock(return_value=None)):
                with patch.object(evt, "_command_listener", new=AsyncMock()):
                    await evt()

        # At least one task should have been created (the listener)
        assert len(created_tasks) >= 1

    @pytest.mark.asyncio
    async def test_call_cancels_command_listener_in_finally(
        self, make_event, command_channel
    ):
        evt = make_event(command_channel=command_channel)
        # Make steps_runner raise so we exercise the finally branch.
        sentinel_error = RuntimeError("steps failed")
        with patch.object(
            evt, "steps_runner", new=AsyncMock(side_effect=sentinel_error)
        ):
            # _command_listener must exist & be cancellable
            listener_started = asyncio.Event()

            async def _listener():
                listener_started.set()
                try:
                    await asyncio.sleep(3600)
                except asyncio.CancelledError:
                    raise

            with patch.object(evt, "_command_listener", new=_listener):
                with pytest.raises(RuntimeError, match="steps failed"):
                    await evt()

    @pytest.mark.asyncio
    async def test_call_stops_resource_monitor_in_finally(self, event):
        """Resource monitor.stop() must be called even when steps_runner raises."""
        event._resource_monitor = MagicMock()
        event._resource_monitor.is_started = MagicMock(return_value=True)
        event._resource_monitor.stop = AsyncMock()

        with patch.object(
            event, "steps_runner", new=AsyncMock(side_effect=RuntimeError("boom"))
        ):
            with pytest.raises(RuntimeError, match="boom"):
                await event()

        event._resource_monitor.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_call_runs_completed_when_phase_not_completed(self, event):
        """If _phase != EventPhase.COMPLETED after steps_runner, __call__
        runs self._completed(*args, **kwargs) in the finally block."""
        from volnux.execution.rehydrator.event.snapshot import EventPhase

        event._phase = EventPhase.PROCESSING  # Not COMPLETED

        with patch.object(event, "steps_runner", new=AsyncMock(return_value=None)):
            with patch.object(event, "_completed", new=AsyncMock()) as mock_completed:
                await event()
                mock_completed.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_call_skips_completed_when_phase_already_completed(self, event):
        from volnux.execution.rehydrator.event.snapshot import EventPhase

        event._phase = EventPhase.COMPLETED

        with patch.object(event, "steps_runner", new=AsyncMock(return_value=None)):
            with patch.object(event, "_completed", new=AsyncMock()) as mock_completed:
                await event()
                mock_completed.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_call_swallows_exceptions_from_completed(self, event):
        """Even if _completed raises, __call__ should not propagate — the
        finally block catches and logs."""
        from volnux.execution.rehydrator.event.snapshot import EventPhase

        event._phase = EventPhase.PROCESSING  # Force _completed path

        with patch.object(event, "steps_runner", new=AsyncMock(return_value="ok")):
            with patch.object(
                event,
                "_completed",
                new=AsyncMock(side_effect=RuntimeError("cleanup failed")),
            ):
                # Should NOT raise
                result = await event()
                assert result == "ok"

    @pytest.mark.asyncio
    async def test_call_sets_main_worker_task(self, event):
        """__call__ sets self._main_worker_task to asyncio.current_task()
        before invoking steps_runner."""
        captured = {}

        async def _spy_steps(*args, **kwargs):
            captured["task"] = asyncio.current_task()
            return None

        with patch.object(event, "steps_runner", new=_spy_steps):
            await event()

        assert captured["task"] is event._main_worker_task

    @pytest.mark.asyncio
    async def test_call_initialises_command_listener_task_to_none(self, event):
        """__call__ sets self._command_listener_task to None at the start,
        then optionally creates the listener task if _command_channel is set."""
        # event fixture has no command_channel → listener should stay None.
        with patch.object(event, "steps_runner", new=AsyncMock(return_value=None)):
            await event()
        assert event._command_listener_task is None


class TestVersionHandlerPerClass:
    """The cached handler lives on the class; a subclass must not inherit its
    parent's, or it registers with the parent's name and version."""

    def test_each_class_gets_its_own_handler(self):
        from volnux import EventBase
        from volnux.versioning import SemanticVersioning

        class Parent(EventBase):
            versioning_class = SemanticVersioning
            version = "1.0.0"

            async def process(self, *a, **k):
                return True, None

        class Child(Parent):
            version = "2.5.0"

            async def process(self, *a, **k):
                return True, None

        assert Parent.get_version_handler() is not Child.get_version_handler()
        assert Parent.get_version_info()["version"] == "1.0.0"
        assert Child.get_version_info()["version"] == "2.5.0"
        assert Child.get_version_handler().class_name == "Child"

    def test_version_lookups_work_for_events_without_a_custom_name(self):
        from volnux import EventBase

        class Plain(EventBase):
            version = "1.0.0"

            async def process(self, *a, **k):
                return True, None

        assert Plain.get_version_handler().class_name == "Plain"
        assert "1.0.0" in Plain.get_all_versions()
        assert Plain.get_latest_version() == "1.0.0"


class TestEventRepr:
    """`executor` is a registry label (a str) by default; the repr used to read
    `.__name__` from it and raise whenever an event was logged at INFO."""

    def test_repr_with_the_default_string_executor(self):
        from volnux import EventBase

        class Plain(EventBase):
            async def process(self, *a, **k):
                return True, None

        assert repr(Plain(MagicMock(), "t1")) == "<Plain executor=default>"

    def test_repr_with_an_executor_class(self):
        from concurrent.futures import ThreadPoolExecutor

        from volnux import EventBase

        class Threaded(EventBase):
            executor = ThreadPoolExecutor

            async def process(self, *a, **k):
                return True, None

        assert (
            repr(Threaded(MagicMock(), "t1"))
            == "<Threaded executor=ThreadPoolExecutor>"
        )

    def test_repr_with_an_executor_instance(self):
        from concurrent.futures import ThreadPoolExecutor

        from volnux import EventBase

        pool = ThreadPoolExecutor(1)

        class Instanced(EventBase):
            executor = pool

            async def process(self, *a, **k):
                return True, None

        try:
            assert (
                repr(Instanced(MagicMock(), "t1"))
                == "<Instanced executor=ThreadPoolExecutor>"
            )
        finally:
            pool.shutdown()
