import unittest
from concurrent.futures import ProcessPoolExecutor
from unittest.mock import patch, MagicMock

import pytest

from volnux import EventBase
from volnux.decorators import event
from volnux.parser.options import StopCondition
from volnux.result import EventResult
from volnux.task import PipelineTask
from volnux.event.base import EventType
from volnux.exceptions import ImproperlyConfigured, SwitchTask
from volnux.result_evaluators import (
    EventEvaluator,
    ResultEvaluationStrategies,
)


class TestEventBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class WithoutParamEvent(EventBase):
            executor = ProcessPoolExecutor

            def process(self, *args, **kwargs):
                return True, "hello"

        class WithParamEvent(EventBase):
            def process(self, name):
                return True, name

        class ProcessNotImplementedEvent(EventBase):
            pass

        class RaiseErrorEvent(EventBase):
            def process(self, *args, **kwargs):
                raise Exception

        class ProcessReturnFalseEvent(EventBase):
            def process(self, *args, **kwargs):
                return False, "False"

        @event()
        def func_with_no_args(self):
            return True, "function_with_no_args"

        @event()
        def func_with_args(self, name):
            return True, name

        cls.WithoutParamEvent = WithoutParamEvent
        cls.WithParamEvent = WithParamEvent
        cls.ProcessNotImplementedEvent = ProcessNotImplementedEvent
        cls.RaiseErrorEvent = RaiseErrorEvent
        cls.func_with_args = func_with_args
        cls.func_with_no_args = func_with_no_args
        cls.ProcessReturnFalseEvent = ProcessReturnFalseEvent

    def test_get_klasses(self):
        klasses = list(EventBase.get_all_event_classes())

        self.assertTrue(len(klasses) > 0)

    def test_function_base_events_create_class(self):
        task1 = PipelineTask(event=self.func_with_no_args.__name__)
        task2 = PipelineTask(event=self.func_with_args.__name__)

        self.assertTrue(
            issubclass(
                task1.resolve_event_name(self.func_with_no_args.__name__), EventBase
            )
        )
        self.assertTrue(
            issubclass(
                task2.resolve_event_name(self.func_with_args.__name__), EventBase
            )
        )

    # def test_is_multiprocssing(self):
    #     event1 = self.WithParamEvent(None, "1")
    #     event2 = self.WithoutParamEvent(None, "1")
    #
    #     self.assertFalse(event1.is_multiprocessing_executor())
    #     self.assertTrue(event2.is_multiprocessing_executor())

    # def test_multiprocess_executor_set_context(self):
    #     event1 = self.WithoutParamEvent(None, "1")
    #     event2 = self.WithParamEvent(None, "1")
    #
    #     self.assertTrue("mp_context" in event1.get_executor_context())
    #     self.assertTrue("mp_context" not in event2.get_executor_context())

    async def test_on_success_and_on_failure_is_called(self):
        event1 = self.WithoutParamEvent(None, "1")
        event2 = self.RaiseErrorEvent(None, "1")
        with patch("volnux.EventBase.on_success") as f:
            await event1()
            f.assert_called()

        response = await event1()
        self.assertIsInstance(response, EventResult)

        with patch("volnux.EventBase.on_failure") as e:
            await event2()
            e.assert_called()

        response = await event2()
        self.assertIsInstance(response, EventResult)

    def test_instantiate_events_without_process_implementation_throws_exception(self):
        with pytest.raises(TypeError):
            self.ProcessNotImplementedEvent(None, "1")

    async def test_event_has_init_and_call_params(self):
        event1 = self.WithParamEvent({"task": 1}, "1", previous_result="box")
        response = await event1(name="box")
        self.assertIsInstance(response, EventResult)
        self.assertEqual(response.task_id, "1")

    async def test_event_flow_branch_to_on_failure_when_process_return_false(self):
        event1 = self.ProcessReturnFalseEvent(None, "1")
        with patch("volnux.EventBase.on_failure") as f:
            await event1()
            f.assert_called()


class TestEventBaseAdditional(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class SimpleEvent(EventBase):
            def process(self, *args, **kwargs):
                return True, {"message": "ok"}

        class BypassEvent(EventBase):
            def bypass(self):
                return True, {"reason": "skip"}

            def process(self, *args, **kwargs):
                return True, {"message": "should-not-run"}

        class FailureEvent(EventBase):
            def process(self, *args, **kwargs):
                return False, {"message": "failed"}

        class NamedEvent(EventBase):
            name = "named-event"
            namespace = "payments"
            version = "2.0.0"

            def process(self, *args, **kwargs):
                return True, {"message": "named"}

        class InvalidStrategyEvent(EventBase):
            result_evaluation_strategy = "invalid"

            def process(self, *args, **kwargs):
                return True, {"message": "ok"}

        class NoStrategyEvent(EventBase):
            result_evaluation_strategy = None

            def process(self, *args, **kwargs):
                return True, {"message": "ok"}

        cls.SimpleEvent = SimpleEvent
        cls.BypassEvent = BypassEvent
        cls.FailureEvent = FailureEvent
        cls.NamedEvent = NamedEvent
        cls.InvalidStrategyEvent = InvalidStrategyEvent
        cls.NoStrategyEvent = NoStrategyEvent

    def setUp(self):
        self._original_backend_store = EventResult._backend_store
        self._original_backend_config = EventResult._backend_config

        backend = MagicMock()
        backend.connector = MagicMock()
        backend.connector.is_connected.return_value = True
        backend.connector.connect = MagicMock()
        backend.insert = MagicMock()
        backend.update = MagicMock()
        backend.upsert = MagicMock()
        backend.delete = MagicMock()
        backend.reload = MagicMock()
        backend.exists = MagicMock(return_value=True)
        backend.get = MagicMock()
        backend.filter = MagicMock(return_value=[])
        backend.count = MagicMock(return_value=0)

        EventResult._backend_store = backend
        EventResult._backend_config = {"ENGINE": "mock.backend.Store"}

    def tearDown(self):
        EventResult._backend_store = self._original_backend_store
        EventResult._backend_config = self._original_backend_config

    def test_repr_includes_executor_name(self):
        event = self.SimpleEvent(None, "1")

        result = repr(event)

        self.assertIn("SimpleEvent", result)
        self.assertIn("executor=", result)

    def test_event_result_builds_expected_result(self):
        event = self.SimpleEvent(None, "task-1", sequence_number=3)
        event._call_args = {"args": (), "kwargs": {}}

        result = event.event_result(False, {"value": 1})

        self.assertIsInstance(result, EventResult)
        self.assertFalse(result.error)
        self.assertEqual(result.task_id, "task-1")
        self.assertEqual(result.order, 3)
        self.assertEqual(result.event_name, "SimpleEvent")
        self.assertEqual(result.content, {"value": 1})

    def test_goto_raises_value_error_for_non_integer_descriptor(self):
        event = self.SimpleEvent(None, "1")

        with pytest.raises(ValueError, match="Descriptor must be an integer"):
            event.goto("bad", True, {"value": 1})  # type: ignore

    def test_goto_raises_switch_task_with_success_result(self):
        event = self.SimpleEvent(None, "task-1")
        event._call_args = {"args": (), "kwargs": {}}

        with pytest.raises(SwitchTask) as exc:
            event.goto(2, True, {"value": "ok"})

        switch = exc.value
        self.assertEqual(switch.current_task_id, "task-1")
        self.assertEqual(switch.next_task_descriptor, 2)
        self.assertEqual(switch.reason, "manual")
        self.assertIsInstance(switch.result, EventResult)
        self.assertFalse(switch.result.error)

    def test_goto_raises_switch_task_with_manual_wrapped_result(self):
        event = self.SimpleEvent(None, "task-1")
        event._call_args = {"args": (), "kwargs": {}}

        with pytest.raises(SwitchTask) as exc:
            event.goto(4, False, {"value": "bad"}, execute_on_event_method=False)

        switch = exc.value
        self.assertEqual(switch.next_task_descriptor, 4)
        self.assertIsInstance(switch.result, EventResult)
        self.assertTrue(switch.result.error)
        self.assertEqual(switch.result.content, {"value": "bad"})

    def test_evaluator_returns_event_evaluator(self):
        evaluator = self.SimpleEvent.evaluator()

        self.assertIsInstance(evaluator, EventEvaluator)

    def test_evaluator_raises_if_strategy_is_none(self):
        with pytest.raises(
            ImproperlyConfigured, match="No result evaluation strategy specified"
        ):
            self.NoStrategyEvent.evaluator()

    def test_evaluator_raises_if_strategy_is_invalid(self):
        with pytest.raises(
            ImproperlyConfigured, match="is not a valid result evaluation strategy"
        ):
            self.InvalidStrategyEvent.evaluator()

    def test_can_bypass_current_event_default_is_false(self):
        event = self.SimpleEvent(None, "1")

        should_skip, data = event.can_bypass_current_event()

        self.assertFalse(should_skip)
        self.assertIsNone(data)

    async def test_call_bypasses_process_when_bypass_flag_enabled(self):
        event = self.BypassEvent(None, "task-1", run_bypass_event_checks=True)

        with patch.object(event, "process") as process_mock:
            result = await event()

        process_mock.assert_not_called()
        self.assertIsInstance(result, EventResult)
        self.assertFalse(result.error)
        self.assertTrue(result.content["skip_event_execution"])
        self.assertEqual(result.content["data"], {"reason": "skip"})

    async def test_call_routes_false_status_to_on_failure(self):
        event = self.FailureEvent(None, "task-1")

        result = await event()

        self.assertIsInstance(result, EventResult)
        self.assertTrue(result.error)
        self.assertEqual(result.content, {"message": "failed"})

    def test_get_version_handler_is_cached(self):
        fake_handler = MagicMock()

        with patch(
            "volnux.event.base.VersionHandler.from_class", return_value=fake_handler
        ) as mocked:
            self.SimpleEvent._version_handler = None

            first = self.SimpleEvent.get_version_handler()
            second = self.SimpleEvent.get_version_handler()

        self.assertIs(first, fake_handler)
        self.assertIs(second, fake_handler)
        mocked.assert_called_once_with(
            self.SimpleEvent, config_key="DEFAULT_EVENT_VERSIONING"
        )

    def test_get_version_info_delegates_to_handler(self):
        fake_handler = MagicMock()
        fake_handler.get_info.return_value = {
            "version": "1.0.0",
            "namespace": "local",
            "deprecated": False,
        }

        with patch.object(
            self.SimpleEvent, "get_version_handler", return_value=fake_handler
        ):
            result = self.SimpleEvent.get_version_info()

        self.assertEqual(result["version"], "1.0.0")
        fake_handler.get_info.assert_called_once()

    def test_is_deprecated_delegates_to_handler(self):
        fake_handler = MagicMock()
        fake_handler.is_deprecated.return_value = True

        with patch.object(
            self.SimpleEvent, "get_version_handler", return_value=fake_handler
        ):
            result = self.SimpleEvent.is_deprecated()

        self.assertTrue(result)
        fake_handler.is_deprecated.assert_called_once()

    def test_get_all_versions_uses_registry_with_handler_metadata(self):
        fake_handler = MagicMock()
        fake_handler.class_name = "named-event"
        fake_handler.namespace = "payments"

        with patch.object(
            self.NamedEvent, "get_version_handler", return_value=fake_handler
        ), patch(
            "volnux.event.base._event_registry.list_versions",
            return_value=["1.0.0", "2.0.0"],
        ) as registry_mock:
            versions = self.NamedEvent.get_all_versions()

        self.assertEqual(versions, ["1.0.0", "2.0.0"])
        registry_mock.assert_called_once_with("named-event", "payments")

    def test_get_all_versions_uses_explicit_name_when_provided(self):
        fake_handler = MagicMock()
        fake_handler.class_name = "named-event"
        fake_handler.namespace = "payments"

        with patch.object(
            self.NamedEvent, "get_version_handler", return_value=fake_handler
        ), patch(
            "volnux.event.base._event_registry.list_versions",
            return_value=["2.0.0"],
        ) as registry_mock:
            versions = self.NamedEvent.get_all_versions(name="override-name")

        self.assertEqual(versions, ["2.0.0"])
        registry_mock.assert_called_once_with("override-name", "payments")

    def test_get_latest_version_delegates_to_registry(self):
        fake_handler = MagicMock()
        fake_handler.class_name = "named-event"
        fake_handler.namespace = "payments"

        with patch.object(
            self.NamedEvent, "get_version_handler", return_value=fake_handler
        ), patch(
            "volnux.event.base._event_registry.get_latest_version",
            return_value="2.1.0",
        ) as registry_mock:
            version = self.NamedEvent.get_latest_version()

        self.assertEqual(version, "2.1.0")
        registry_mock.assert_called_once_with("named-event", "payments")

    def test_get_direct_subclasses_returns_direct_children(self):
        subclasses = EventBase.get_direct_subclasses()

        self.assertIn(self.SimpleEvent, subclasses)
        self.assertIn(self.NamedEvent, subclasses)

    def test_clear_class_cache_delegates_to_registry(self):
        with patch("volnux.event.base._event_registry.clear") as clear_mock:
            EventBase.clear_class_cache()

        clear_mock.assert_called_once()

    def test_event_type_default_is_other(self):
        self.assertEqual(self.SimpleEvent.event_type, EventType.OTHER)

    def test_default_result_evaluation_strategy_is_configured(self):
        self.assertIs(
            self.SimpleEvent.result_evaluation_strategy,
            ResultEvaluationStrategies.ALL_MUST_SUCCEED,
        )
