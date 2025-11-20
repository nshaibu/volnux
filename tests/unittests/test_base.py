import time
from unittest import TestCase
from unittest.mock import Mock, patch

import pytest

from volnux.base import (EventBase, ExecutorInitializerConfig, RetryPolicy,
                         _RetryMixin)
from volnux.constants import EMPTY, MAX_BACKOFF, MAX_BACKOFF_FACTOR
from volnux.exceptions import MaxRetryError, StopProcessingError, SwitchTask
from volnux.result import EventResult, ResultSet
from volnux.typing import ConfigState


class TestRetryPolicy(TestCase):
    def setUp(self):
        self.retry_policy = RetryPolicy(
            max_attempts=3,
            backoff_factor=0.1,
            max_backoff=1.0,
            retry_on_exceptions=[ValueError],
        )

    def test_retry_policy_init(self):
        self.assertEqual(self.retry_policy.max_attempts, 3)
        self.assertEqual(self.retry_policy.backoff_factor, 0.1)
        self.assertEqual(self.retry_policy.max_backoff, 1.0)
        self.assertEqual(self.retry_policy.retry_on_exceptions, [ValueError])

    def test_retry_policy_defaults(self):
        policy = RetryPolicy()
        self.assertIsNotNone(policy.max_attempts)
        self.assertIsNotNone(policy.backoff_factor)
        self.assertIsNotNone(policy.max_backoff)
        self.assertEqual(policy.retry_on_exceptions, [])


class TestExecutorInitializerConfig(TestCase):
    def test_executor_config_init(self):
        config = ExecutorInitializerConfig(
            max_workers=4, max_tasks_per_child=10, thread_name_prefix="test"
        )
        self.assertEqual(config.max_workers, 4)
        self.assertEqual(config.max_tasks_per_child, 10)
        self.assertEqual(config.thread_name_prefix, "test")

    def test_executor_config_defaults(self):
        config = ExecutorInitializerConfig()
        self.assertIs(config.max_workers, ConfigState.UNSET)
        self.assertIs(config.max_tasks_per_child, ConfigState.UNSET)
        self.assertIs(config.thread_name_prefix, ConfigState.UNSET)


class TestRetryMixin(TestCase):
    def setUp(self):
        class TestClass(_RetryMixin):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

        self.retry_mixin = TestClass()
        self.retry_policy = RetryPolicy(
            max_attempts=3,
            backoff_factor=0.1,
            max_backoff=1.0,
            retry_on_exceptions=[ValueError],
        )
        self.retry_mixin.retry_policy = self.retry_policy

    def test_get_retry_policy(self):
        self.assertEqual(self.retry_mixin.get_retry_policy(), self.retry_policy)

        # Test with dict-based retry policy
        self.retry_mixin.retry_policy = {
            "max_attempts": 2,
            "backoff_factor": 0.2,
            "max_backoff": 2.0,
            "retry_on_exceptions": [KeyError],
        }
        retry_policy = self.retry_mixin.get_retry_policy()
        self.assertEqual(retry_policy.max_attempts, 2)
        self.assertEqual(retry_policy.backoff_factor, 0.2)
        self.assertEqual(retry_policy.max_backoff, 2.0)
        self.assertEqual(retry_policy.retry_on_exceptions, [KeyError])

    def test_config_retry_policy(self):
        self.retry_mixin.config_retry_policy(
            max_attempts=5,
            backoff_factor=0.5,
            max_backoff=2.0,
            retry_on_exceptions=(ValueError, KeyError),
        )
        retry_policy = self.retry_mixin.get_retry_policy()
        self.assertEqual(retry_policy.max_attempts, 5)
        self.assertEqual(retry_policy.backoff_factor, 0.5)
        self.assertEqual(retry_policy.max_backoff, 2.0)
        self.assertEqual(retry_policy.retry_on_exceptions, [ValueError, KeyError])

    def test_get_backoff_time(self):
        self.retry_mixin._retry_count = 1
        self.assertEqual(self.retry_mixin.get_backoff_time(), 0)

        self.retry_mixin._retry_count = 2
        self.assertEqual(self.retry_mixin.get_backoff_time(), 0.2)

        self.retry_mixin._retry_count = 5
        self.assertEqual(
            self.retry_mixin.get_backoff_time(), 1.0
        )  # Capped at max_backoff

    @patch("time.sleep", return_value=None)
    def test_sleep_for_backoff(self, mock_sleep):
        self.retry_mixin._retry_count = 2
        backoff = self.retry_mixin._sleep_for_backoff()
        self.assertEqual(backoff, 0.2)
        mock_sleep.assert_called_once_with(0.2)

    def test_is_retryable(self):
        self.assertTrue(self.retry_mixin.is_retryable(ValueError()))
        self.assertFalse(self.retry_mixin.is_retryable(KeyError()))

    def test_is_exhausted(self):
        self.retry_mixin._retry_count = 3
        self.assertTrue(self.retry_mixin.is_exhausted())

        self.retry_mixin._retry_count = 2
        self.assertFalse(self.retry_mixin.is_exhausted())

    @patch("time.sleep", return_value=None)
    def test_retry_success(self, mock_sleep):
        mock_func = Mock(return_value="success")
        result = self.retry_mixin.retry(mock_func)
        self.assertEqual(result, "success")
        mock_func.assert_called_once()

    @patch("time.sleep", return_value=None)
    def test_retry_with_retries(self, mock_sleep):
        mock_func = Mock(side_effect=[ValueError(), ValueError(), "success"])
        self.retry_mixin._execution_context = Mock()
        self.retry_mixin._task_id = "test_task"

        result = self.retry_mixin.retry(mock_func)
        self.assertEqual(result, "success")
        self.assertEqual(mock_func.call_count, 3)

    @patch("time.sleep", return_value=None)
    def test_retry_exhaustion(self, mock_sleep):
        mock_func = Mock(side_effect=ValueError())
        self.retry_mixin._execution_context = Mock()
        self.retry_mixin._task_id = "test_task"

        with self.assertRaises(MaxRetryError):
            self.retry_mixin.retry(mock_func)
        self.assertEqual(mock_func.call_count, 3)


class TestGotoMethod(TestCase):
    def setUp(self):
        class MockEventBase(EventBase):
            def process(self, *args, **kwargs):
                pass

        self.event = MockEventBase(
            execution_context=Mock(),
            task_id="test_task",
        )
        self.event._call_args = {"arg1": "value1"}
        self.event._init_args = {"init_arg1": "init_value1"}

    @patch("volnux.base.EventBase.on_success")
    @patch("volnux.base.EventBase.on_failure")
    def test_goto_with_success(self, mock_on_failure, mock_on_success):
        mock_on_success.return_value = EventResult(
            error=False,
            content={"key": "value"},
            task_id="test_task",
            event_name="MockEventBase",
            call_params=self.event._call_args,
            init_params=self.event._init_args,
        )

        with self.assertRaises(SwitchTask) as context:
            self.event.goto(
                descriptor=1,
                result_success=True,
                result={"key": "value"},
                reason="manual",
                execute_on_event_method=True,
            )

        switch_task_exception = context.exception
        self.assertEqual(switch_task_exception.current_task_id, "test_task")
        self.assertEqual(switch_task_exception.next_task_descriptor, 1)
        self.assertEqual(switch_task_exception.result.error, False)
        self.assertEqual(switch_task_exception.result.content, {"key": "value"})
        self.assertEqual(switch_task_exception.reason, "manual")
        mock_on_success.assert_called_once_with({"key": "value"})
        mock_on_failure.assert_not_called()

    @patch("volnux.base.EventBase.on_success")
    @patch("volnux.base.EventBase.on_failure")
    def test_goto_with_failure(self, mock_on_failure, mock_on_success):
        mock_on_failure.return_value = EventResult(
            error=True,
            content={"error": "failure"},
            task_id="test_task",
            event_name="MockEventBase",
            call_params=self.event._call_args,
            init_params=self.event._init_args,
        )

        with self.assertRaises(SwitchTask) as context:
            self.event.goto(
                descriptor=2,
                result_success=False,
                result={"error": "failure"},
                reason="manual",
                execute_on_event_method=True,
            )

        switch_task_exception = context.exception
        self.assertEqual(switch_task_exception.current_task_id, "test_task")
        self.assertEqual(switch_task_exception.next_task_descriptor, 2)
        self.assertEqual(switch_task_exception.result.error, True)
        self.assertEqual(switch_task_exception.result.content, {"error": "failure"})
        self.assertEqual(switch_task_exception.reason, "manual")
        mock_on_failure.assert_called_once_with({"error": "failure"})
        mock_on_success.assert_not_called()

    def test_goto_without_execute_on_event_method(self):
        with self.assertRaises(SwitchTask) as context:
            self.event.goto(
                descriptor=3,
                result_success=True,
                result={"key": "value"},
                reason="manual",
                execute_on_event_method=False,
            )

        switch_task_exception = context.exception
        self.assertEqual(switch_task_exception.current_task_id, "test_task")
        self.assertEqual(switch_task_exception.next_task_descriptor, 3)
        self.assertEqual(switch_task_exception.result.error, False)
        self.assertEqual(switch_task_exception.result.content, {"key": "value"})
        self.assertEqual(switch_task_exception.result.task_id, "test_task")
        self.assertEqual(switch_task_exception.result.event_name, "MockEventBase")
        self.assertEqual(
            switch_task_exception.result.call_params, self.event._call_args
        )
        self.assertEqual(
            switch_task_exception.result.init_params, self.event._init_args
        )
        self.assertEqual(switch_task_exception.reason, "manual")

    def test_goto_with_invalid_descriptor(self):
        with self.assertRaises(ValueError):
            self.event.goto(
                descriptor="invalid",
                result_success=True,
                result={"key": "value"},
                reason="manual",
                execute_on_event_method=True,
            )


class TestEventBaseCallMethod(TestCase):
    def setUp(self):
        class MockEventBase(EventBase):
            def process(self, *args, **kwargs):
                return True, {"key": "value"}

        self.event = MockEventBase(
            execution_context=Mock(),
            task_id="test_task",
        )
        self.event._call_args = {"arg1": "value1"}
        self.event._init_args = {"init_arg1": "init_value1"}

    @patch("volnux.base.EventBase.on_success")
    @patch("volnux.base.EventBase.on_failure")
    @patch("volnux.base.EventBase.can_bypass_current_event")
    def test_call_with_bypass(self, mock_can_bypass, mock_on_failure, mock_on_success):
        self.event.run_bypass_event_checks = True
        mock_can_bypass.return_value = (True, {"bypass_data": "value"})
        mock_on_success.return_value = EventResult(
            error=False,
            content={
                "status": 1,
                "skip_event_execution": True,
                "data": {"bypass_data": "value"},
            },
            task_id="test_task",
            event_name="MockEventBase",
            call_params=self.event._call_args,
            init_params=self.event._init_args,
        )

        result = self.event()
        self.assertEqual(result.error, False)
        self.assertEqual(result.content["status"], 1)
        self.assertEqual(result.content["skip_event_execution"], True)
        self.assertEqual(result.content["data"], {"bypass_data": "value"})
        mock_can_bypass.assert_called_once()
        mock_on_success.assert_called_once()
        mock_on_failure.assert_not_called()

    @patch("volnux.base.EventBase.on_success")
    @patch("volnux.base.EventBase.on_failure")
    def test_call_with_successful_execution(self, mock_on_failure, mock_on_success):
        mock_on_success.return_value = EventResult(
            error=False,
            content={"key": "value"},
            task_id="test_task",
            event_name="MockEventBase",
            call_params=self.event._call_args,
            init_params=self.event._init_args,
        )

        result = self.event()
        self.assertEqual(result.error, False)
        self.assertEqual(result.content, {"key": "value"})
        mock_on_success.assert_called_once_with({"key": "value"})
        mock_on_failure.assert_not_called()

    @patch("volnux.base.EventBase.on_success")
    @patch("volnux.base.EventBase.on_failure")
    @patch("volnux.base.EventBase.retry")
    def test_call_with_retry_failure(
        self, mock_retry, mock_on_failure, mock_on_success
    ):
        mock_retry.side_effect = MaxRetryError("Retry failed", exception=Exception())
        mock_on_failure.return_value = EventResult(
            error=True,
            content={"error": "Retry failed"},
            task_id="test_task",
            event_name="MockEventBase",
            call_params=self.event._call_args,
            init_params=self.event._init_args,
        )

        result = self.event()
        self.assertEqual(result.error, True)
        self.assertEqual(result.content, {"error": "Retry failed"})
        mock_retry.assert_called_once()
        mock_on_failure.assert_called_once()
        mock_on_success.assert_not_called()

    @patch("volnux.base.EventBase.on_success")
    @patch("volnux.base.EventBase.on_failure")
    @patch("volnux.base.EventBase.retry")
    def test_call_with_general_exception(
        self, mock_retry, mock_on_failure, mock_on_success
    ):
        mock_retry.side_effect = Exception("General error")
        mock_on_failure.return_value = EventResult(
            error=True,
            content={"error": "General error"},
            task_id="test_task",
            event_name="MockEventBase",
            call_params=self.event._call_args,
            init_params=self.event._init_args,
        )

        result = self.event()
        self.assertEqual(result.error, True)
        self.assertEqual(result.content, {"error": "General error"})
        mock_retry.assert_called_once()
        mock_on_failure.assert_called_once()
        mock_on_success.assert_not_called()
