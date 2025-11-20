import unittest
from typing import List
from unittest.mock import Mock

import pytest

from volnux.result_evaluators import (AllTasksMustSucceedStrategy,
                                      AnyTaskMustSucceedStrategy,
                                      EventEvaluationResult, EventEvaluator,
                                      EventResult,
                                      ExecutionResultEvaluationStrategyBase,
                                      MajorityTasksMustSucceedStrategy,
                                      MinimumSuccessThresholdStrategy,
                                      NoFailuresAllowedStrategy,
                                      PercentageSuccessThresholdStrategy,
                                      ResultEvaluationStrategies)


class TestTaskResult(unittest.TestCase):
    """Test cases for TaskResult data class."""

    def test_task_result_creation(self):
        """Test basic TaskResult creation."""
        result = EventResult(
            task_id="test_1", error=False, content="test_data", event_name="test_event"
        )
        self.assertEqual(result.task_id, "test_1")
        self.assertTrue(result.success)
        self.assertEqual(result.content, "test_data")
        self.assertFalse(result.error)
        self.assertIsNotNone(result.creation_time)

    def test_task_result_with_error(self):
        """Test TaskResult with error information."""
        error = Exception("Test error")
        result = EventResult(
            task_id="test_2", error=True, content=error, event_name="test_event"
        )
        self.assertEqual(result.task_id, "test_2")
        self.assertFalse(result.success)
        self.assertEqual(result.content, error)


class TestEventEvaluationResult(unittest.TestCase):
    """Test cases for EventEvaluationResult."""

    def setUp(self):
        """Set up test data."""
        self.task_results = [
            EventResult(
                task_id="task_1",
                error=False,
                content="test_data",
                event_name="test_event",
            ),
            EventResult(
                task_id="task_2",
                error=True,
                content="test_data",
                event_name="test_event",
            ),
            EventResult(
                task_id="task_3",
                error=False,
                content="test_data",
                event_name="test_event",
            ),
            EventResult(
                task_id="task_4",
                error=False,
                content="test_data",
                event_name="test_event",
            ),
            EventResult(
                task_id="task_5",
                error=True,
                content="test_data",
                event_name="test_event",
            ),
        ]

        self.result = EventEvaluationResult(
            success=True,
            total_tasks=5,
            successful_tasks=3,
            failed_tasks=2,
            strategy_used="Test Strategy",
        )

    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        self.assertEqual(self.result.success_rate, 60.0)

    def test_success_rate_empty_tasks(self):
        """Test success rate with no tasks."""
        empty_result = EventEvaluationResult(
            success=False,
            total_tasks=0,
            successful_tasks=0,
            failed_tasks=0,
            strategy_used="Test",
        )
        self.assertEqual(empty_result.success_rate, 0.0)

    def test_has_partial_success(self):
        """Test partial success detection."""
        self.assertTrue(self.result.has_partial_success)

        # Test all success case
        all_success = EventEvaluationResult(
            success=True,
            total_tasks=3,
            successful_tasks=3,
            failed_tasks=0,
            strategy_used="Test",
        )
        self.assertFalse(all_success.has_partial_success)

        # Test all failure case
        all_failure = EventEvaluationResult(
            success=False,
            total_tasks=3,
            successful_tasks=0,
            failed_tasks=3,
            strategy_used="Test",
        )
        self.assertFalse(all_failure.has_partial_success)


class TestAllTasksMustSucceedStrategy(unittest.TestCase):
    """Test cases for AllTasksMustSucceedStrategy."""

    def setUp(self):
        self.strategy = AllTasksMustSucceedStrategy()

    def test_all_tasks_succeed(self):
        """Test when all tasks succeed."""
        results = [
            EventResult(
                task_id="1", error=False, content="test_data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test_data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=False, content="test_data", event_name="test_event"
            ),
        ]
        self.assertTrue(self.strategy.evaluate(results))

    def test_some_tasks_fail(self):
        """Test when some tasks fail."""
        results = [
            EventResult(
                task_id="1", error=False, content="test_data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=True, content="test_data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=False, content="test_data", event_name="test_event"
            ),
        ]
        self.assertFalse(self.strategy.evaluate(results))

    def test_all_tasks_fail(self):
        """Test when all tasks fail."""
        results = [
            EventResult(
                task_id="1", error=True, content="test_data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=True, content="test_data", event_name="test_event"
            ),
        ]
        self.assertFalse(self.strategy.evaluate(results))

    def test_empty_task_list(self):
        """Test with empty task list."""
        self.assertFalse(self.strategy.evaluate([]))

    def test_strategy_name(self):
        """Test strategy name."""
        self.assertEqual(self.strategy.get_strategy_name(), "All Tasks Must Succeed")


class TestAnyTaskMustSucceedStrategy(unittest.TestCase):
    """Test cases for AnyTaskMustSucceedStrategy."""

    def setUp(self):
        self.strategy = AnyTaskMustSucceedStrategy()

    def test_all_tasks_succeed(self):
        """Test when all tasks succeed."""
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test data", event_name="test_event"
            ),
        ]
        self.assertTrue(self.strategy.evaluate(results))

    def test_some_tasks_succeed(self):
        """Test when some tasks succeed."""
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=True, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=True, content="test data", event_name="test_event"
            ),
        ]
        self.assertTrue(self.strategy.evaluate(results))

    def test_all_tasks_fail(self):
        """Test when all tasks fail."""
        results = [
            EventResult(
                task_id="1", error=True, content="test_data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=True, content="test_data", event_name="test_event"
            ),
        ]
        self.assertFalse(self.strategy.evaluate(results))

    def test_empty_task_list(self):
        """Test with empty task list."""
        self.assertFalse(self.strategy.evaluate([]))

    def test_single_success(self):
        """Test with single successful task."""
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            )
        ]
        self.assertTrue(self.strategy.evaluate(results))

    def test_single_failure(self):
        """Test with single failed task."""
        results = [
            EventResult(
                task_id="1", error=True, content="test data", event_name="test_event"
            )
        ]
        self.assertFalse(self.strategy.evaluate(results))


class TestMajorityTasksMustSucceedStrategy(unittest.TestCase):
    """Test cases for MajorityTasksMustSucceedStrategy."""

    def test_clear_majority_success(self):
        """Test when clear majority succeeds."""
        strategy = MajorityTasksMustSucceedStrategy()
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="4", error=True, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="5", error=True, content="test data", event_name="test_event"
            ),
        ]  # 3 out of 5 succeed (60%)
        self.assertTrue(strategy.evaluate(results))

    def test_clear_majority_failure(self):
        """Test when clear majority fails."""
        strategy = MajorityTasksMustSucceedStrategy()
        results = [
            EventResult(
                task_id="1", error=False, content="test_data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=True, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=True, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="4", error=True, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="5", error=True, content="test data", event_name="test_event"
            ),
        ]  # 1 out of 5 succeed (20%)
        self.assertFalse(strategy.evaluate(results))

    def test_tie_with_tie_breaker_true(self):
        """Test 50/50 split with tie_breaker=True."""
        strategy = MajorityTasksMustSucceedStrategy(tie_breaker=True)
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=True, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="4", error=True, content="test data", event_name="test_event"
            ),
        ]  # 2 out of 4 succeed (50%)
        self.assertTrue(strategy.evaluate(results))

    def test_tie_with_tie_breaker_false(self):
        """Test 50/50 split with tie_breaker=False."""
        strategy = MajorityTasksMustSucceedStrategy(tie_breaker=False)
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=True, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="4", error=True, content="test data", event_name="test_event"
            ),
        ]  # 2 out of 4 succeed (50%)
        self.assertFalse(strategy.evaluate(results))

    def test_empty_task_list(self):
        """Test with empty task list."""
        strategy = MajorityTasksMustSucceedStrategy()
        self.assertFalse(strategy.evaluate([]))

    def test_single_task_success(self):
        """Test with single successful task."""
        strategy = MajorityTasksMustSucceedStrategy()
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            )
        ]
        self.assertTrue(strategy.evaluate(results))

    def test_single_task_failure(self):
        """Test with single failed task."""
        strategy = MajorityTasksMustSucceedStrategy()
        results = [
            EventResult(
                task_id="1", error=True, content="test data", event_name="test_event"
            )
        ]
        self.assertFalse(strategy.evaluate(results))


class TestMinimumSuccessThresholdStrategy(unittest.TestCase):
    """Test cases for MinimumSuccessThresholdStrategy."""

    def test_threshold_met(self):
        """Test when threshold is met."""
        strategy = MinimumSuccessThresholdStrategy(2)
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=True, content="test data", event_name="test_event"
            ),
        ]  # 2 successes, threshold is 2
        self.assertTrue(strategy.evaluate(results))

    def test_threshold_exceeded(self):
        """Test when threshold is exceeded."""
        strategy = MinimumSuccessThresholdStrategy(2)
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=False, content="test data", event_name="test_event"
            ),
        ]  # 3 successes, threshold is 2
        self.assertTrue(strategy.evaluate(results))

    def test_threshold_not_met(self):
        """Test when threshold is not met."""
        strategy = MinimumSuccessThresholdStrategy(3)
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=True, content="test data", event_name="test_event"
            ),
        ]  # 2 successes, threshold is 3
        self.assertFalse(strategy.evaluate(results))

    def test_zero_threshold_empty_list(self):
        """Test zero threshold with empty list."""
        strategy = MinimumSuccessThresholdStrategy(0)
        self.assertTrue(strategy.evaluate([]))

    def test_zero_threshold_with_tasks(self):
        """Test zero threshold with tasks."""
        strategy = MinimumSuccessThresholdStrategy(0)
        results = [
            EventResult(
                task_id="1", error=True, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=True, content="test data", event_name="test_event"
            ),
        ]
        self.assertTrue(strategy.evaluate(results))

    def test_invalid_threshold(self):
        """Test invalid threshold value."""
        with self.assertRaises(ValueError):
            MinimumSuccessThresholdStrategy(-1)

    def test_strategy_name(self):
        """Test strategy name."""
        strategy = MinimumSuccessThresholdStrategy(3)
        self.assertEqual(strategy.get_strategy_name(), "At Least 3 Tasks Must Succeed")


class TestPercentageSuccessThresholdStrategy(unittest.TestCase):
    """Test cases for PercentageSuccessThresholdStrategy."""

    def test_percentage_met(self):
        """Test when percentage threshold is met."""
        strategy = PercentageSuccessThresholdStrategy(60.0)
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="4", error=True, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="5", error=True, content="test data", event_name="test_event"
            ),
        ]  # 3 out of 5 = 60%
        self.assertTrue(strategy.evaluate(results))

    def test_percentage_exceeded(self):
        """Test when percentage threshold is exceeded."""
        strategy = PercentageSuccessThresholdStrategy(50.0)
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="4", error=True, content="test data", event_name="test_event"
            ),
        ]  # 3 out of 4 = 75%
        self.assertTrue(strategy.evaluate(results))

    def test_percentage_not_met(self):
        """Test when percentage threshold is not met."""
        strategy = PercentageSuccessThresholdStrategy(80.0)
        results = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="3", error=True, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="4", error=True, content="test data", event_name="test_event"
            ),
        ]  # 2 out of 4 = 50%
        self.assertFalse(strategy.evaluate(results))

    def test_zero_percentage_empty_list(self):
        """Test 0% threshold with empty list."""
        strategy = PercentageSuccessThresholdStrategy(0.0)
        self.assertTrue(strategy.evaluate([]))

    def test_nonzero_percentage_empty_list(self):
        """Test non-zero percentage with empty list."""
        strategy = PercentageSuccessThresholdStrategy(50.0)
        self.assertFalse(strategy.evaluate([]))

    def test_hundred_percentage(self):
        """Test 100% threshold."""
        strategy = PercentageSuccessThresholdStrategy(100.0)

        # All succeed
        all_success = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="test data", event_name="test_event"
            ),
        ]
        self.assertTrue(strategy.evaluate(all_success))

        # Some fail
        some_fail = [
            EventResult(
                task_id="1", error=False, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=True, content="test data", event_name="test_event"
            ),
        ]
        self.assertFalse(strategy.evaluate(some_fail))

    def test_invalid_percentage_values(self):
        """Test invalid percentage values."""
        with self.assertRaises(ValueError):
            PercentageSuccessThresholdStrategy(-1.0)

        with self.assertRaises(ValueError):
            PercentageSuccessThresholdStrategy(101.0)

    def test_strategy_name(self):
        """Test strategy name."""
        strategy = PercentageSuccessThresholdStrategy(75.5)
        self.assertEqual(strategy.get_strategy_name(), "At Least 75.5% Must Succeed")


class TestNoFailuresAllowedStrategy(unittest.TestCase):
    """Test cases for NoFailuresAllowedStrategy."""

    def setUp(self):
        self.strategy = NoFailuresAllowedStrategy()

    def test_no_failures(self):
        """Test when no tasks fail."""
        results = [
            EventResult(
                task_id="1", error=False, content="Hello world", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=False, content="Hello world", event_name="test_event"
            ),
        ]
        self.assertTrue(self.strategy.evaluate(results))

    def test_some_failures(self):
        """Test when some tasks fail."""
        results = [
            EventResult(
                task_id="1", error=False, content="Test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=True, content="Test Data", event_name="test_event"
            ),
        ]
        self.assertFalse(self.strategy.evaluate(results))

    def test_all_failures(self):
        """Test when all tasks fail."""
        results = [
            EventResult(
                task_id="1", error=True, content="Test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=True, content="Test data", event_name="test_event"
            ),
        ]
        self.assertFalse(self.strategy.evaluate(results))

    def test_empty_task_list(self):
        """Test with empty task list (no failures = success)."""
        self.assertTrue(self.strategy.evaluate([]))


class TestCommonStrategies(unittest.TestCase):
    """Test cases for CommonStrategies convenience class."""

    def test_predefined_strategies(self):
        """Test that predefined strategies are properly instantiated."""
        self.assertIsInstance(
            ResultEvaluationStrategies.ALL_MUST_SUCCEED, AllTasksMustSucceedStrategy
        )
        self.assertIsInstance(
            ResultEvaluationStrategies.ANY_MUST_SUCCEED, AnyTaskMustSucceedStrategy
        )
        self.assertIsInstance(
            ResultEvaluationStrategies.MAJORITY_MUST_SUCCEED,
            MajorityTasksMustSucceedStrategy,
        )
        self.assertIsInstance(
            ResultEvaluationStrategies.NO_FAILURES_ALLOWED, NoFailuresAllowedStrategy
        )

    def test_factory_methods(self):
        """Test factory methods."""
        threshold_strategy = ResultEvaluationStrategies.at_least_n_succeed(3)
        self.assertIsInstance(threshold_strategy, MinimumSuccessThresholdStrategy)
        self.assertEqual(threshold_strategy.minimum_successes, 3)

        percentage_strategy = ResultEvaluationStrategies.at_least_percent_succeed(75.0)
        self.assertIsInstance(percentage_strategy, PercentageSuccessThresholdStrategy)
        self.assertEqual(percentage_strategy.success_percentage, 75.0)


class TestEventEvaluator(unittest.TestCase):
    """Test cases for EventEvaluator."""

    def setUp(self):
        """Set up test data."""
        self.strategy = AllTasksMustSucceedStrategy()
        self.evaluator = EventEvaluator(self.strategy)

        self.task_results = [
            EventResult(
                task_id="task_1", error=False, content="result1", event_name="test_evnt"
            ),
            EventResult(
                task_id="task_2",
                error=True,
                content=Exception("Error"),
                event_name="test_evnt",
            ),
            EventResult(
                task_id="task_3", error=False, content="result3", event_name="test_evnt"
            ),
        ]

    def test_evaluate_success(self):
        """Test evaluation that results in success."""
        evaluator = EventEvaluator(AnyTaskMustSucceedStrategy())
        result = evaluator.evaluate(self.task_results)

        self.assertTrue(result.success)
        self.assertEqual(result.total_tasks, 3)
        self.assertEqual(result.successful_tasks, 2)
        self.assertEqual(result.failed_tasks, 1)
        self.assertEqual(result.strategy_used, "Any Task Must Succeed")

    def test_evaluate_failure(self):
        """Test evaluation that results in failure."""
        result = self.evaluator.evaluate(self.task_results)

        self.assertFalse(result.success)
        self.assertEqual(result.total_tasks, 3)
        self.assertEqual(result.successful_tasks, 2)
        self.assertEqual(result.failed_tasks, 1)
        self.assertEqual(result.strategy_used, "All Tasks Must Succeed")

    def test_evaluate_empty_list(self):
        """Test evaluation with empty task list."""
        result = self.evaluator.evaluate([])

        self.assertFalse(result.success)
        self.assertEqual(result.total_tasks, 0)
        self.assertEqual(result.successful_tasks, 0)
        self.assertEqual(result.failed_tasks, 0)

    def test_change_strategy(self):
        """Test changing evaluation strategy."""
        original_strategy = self.evaluator.strategy
        new_strategy = AnyTaskMustSucceedStrategy()

        self.evaluator.change_strategy(new_strategy)
        self.assertEqual(self.evaluator.strategy, new_strategy)
        self.assertNotEqual(self.evaluator.strategy, original_strategy)


# class TestEventEvaluatorFactory(unittest.TestCase):
#     """Test cases for EventEvaluatorFactory."""
#
#     def test_strict_evaluator(self):
#         """Test strict evaluator creation."""
#         evaluator = EventEvaluatorFactory.strict_evaluator()
#         self.assertIsInstance(evaluator.strategy, AllTasksMustSucceedStrategy)
#
#     def test_lenient_evaluator(self):
#         """Test lenient evaluator creation."""
#         evaluator = EventEvaluatorFactory.lenient_evaluator()
#         self.assertIsInstance(evaluator.strategy, AnyTaskMustSucceedStrategy)
#
#     def test_balanced_evaluator(self):
#         """Test balanced evaluator creation."""
#         evaluator = EventEvaluatorFactory.balanced_evaluator()
#         self.assertIsInstance(evaluator.strategy, MajorityTasksMustSucceedStrategy)
#
#     def test_threshold_evaluator(self):
#         """Test threshold evaluator creation."""
#         evaluator = EventEvaluatorFactory.threshold_evaluator(5)
#         self.assertIsInstance(evaluator.strategy, MinimumSuccessThresholdStrategy)
#         self.assertEqual(evaluator.strategy.minimum_successes, 5)
#
#     def test_percentage_evaluator(self):
#         """Test percentage evaluator creation."""
#         evaluator = EventEvaluatorFactory.percentage_evaluator(80.0)
#         self.assertIsInstance(evaluator.strategy, PercentageSuccessThresholdStrategy)
#         self.assertEqual(evaluator.strategy.success_percentage, 80.0)


class TestIntegrationScenarios(unittest.TestCase):
    """Integration test cases covering real-world scenarios."""

    # def test_batch_processing_scenario(self):
    #     """Test a batch processing scenario where most tasks must succeed."""
    #     # Simulate processing 100 files where at least 95% must succeed
    #     evaluator = EventEvaluatorFactory.percentage_evaluator(95.0)
    #
    #     # 96 successful tasks, 4 failed tasks
    #     task_results = []
    #     for i in range(96):
    #         task_results.append(
    #             EventResult(
    #                 task_id=f"file_{i}",
    #                 error=False,
    #                 content=f"processed_{id}",
    #                 event_name=f"test_{i}_event",
    #             )
    #         )
    #     for i in range(96, 100):
    #         task_results.append(
    #             EventResult(
    #                 task_id=f"file_{i}",
    #                 error=True,
    #                 content=Exception(f"Failed to process file_{i}"),
    #                 event_name="task",
    #             )
    #         )
    #
    #     result = evaluator.evaluate(task_results)
    #
    #     self.assertTrue(result.success)
    #     self.assertEqual(result.success_rate, 96.0)
    #     self.assertTrue(result.has_partial_success)

    def test_critical_system_scenario(self):
        """Test a critical system where no failures are allowed."""
        evaluator = EventEvaluator(NoFailuresAllowedStrategy())

        # All systems operational
        all_success = [
            EventResult(
                task_id="database",
                error=False,
                content="test data",
                event_name="test_event",
            ),
            EventResult(
                task_id="cache",
                error=False,
                content="test data",
                event_name="test_event",
            ),
            EventResult(
                task_id="api_gateway",
                error=False,
                content="test data",
                event_name="test_event",
            ),
            EventResult(
                task_id="auth_service",
                error=False,
                content="test data",
                event_name="test_event",
            ),
        ]

        result = evaluator.evaluate(all_success)
        self.assertTrue(result.success)
        self.assertEqual(result.success_rate, 100.0)

        # One system fails
        one_failure = [
            EventResult(
                task_id="database",
                error=False,
                content="test data",
                event_name="test_event",
            ),
            EventResult(
                task_id="cache",
                error=True,
                content=Exception("Cache connection failed"),
                event_name="test_event",
            ),
            EventResult(
                task_id="api_gateway",
                error=False,
                content="test data",
                event_name="test_event",
            ),
            EventResult(
                task_id="auth_service",
                error=False,
                content="test data",
                event_name="test_event",
            ),
        ]

        result = evaluator.evaluate(one_failure)
        self.assertFalse(result.success)
        self.assertEqual(result.success_rate, 75.0)

    # def test_data_pipeline_scenario(self):
    #     """Test a data pipeline where at least one source must succeed."""
    #     evaluator = EventEvaluatorFactory.lenient_evaluator()
    #
    #     # Multiple data sources, some may fail
    #     pipeline_results = [
    #         EventResult(
    #             task_id="primary_db",
    #             error=True,
    #             content=Exception("Connection timeout"),
    #             event_name="test_event",
    #         ),
    #         EventResult(
    #             task_id="backup_db",
    #             error=False,
    #             content="backup_data",
    #             event_name="test_event",
    #         ),
    #         EventResult(
    #             task_id="cache_layer",
    #             error=True,
    #             content=Exception("Cache miss"),
    #             event_name="test_event",
    #         ),
    #         EventResult(
    #             task_id="external_api",
    #             error=True,
    #             content=Exception("API rate limit"),
    #             event_name="test_event",
    #         ),
    #     ]
    #
    #     result = evaluator.evaluate(pipeline_results)
    #
    #     self.assertTrue(result.success)  # At least backup_db succeeded
    #     self.assertEqual(result.successful_tasks, 1)
    #     self.assertEqual(result.failed_tasks, 3)

    # def test_distributed_computation_scenario(self):
    #     """Test distributed computation where majority consensus is needed."""
    #     evaluator = EventEvaluatorFactory.balanced_evaluator()
    #
    #     # 7 nodes in a distributed system
    #     computation_results = [
    #         EventResult(
    #             task_id="node_1",
    #             error=False,
    #             content="result_A",
    #             event_name="test_event",
    #         ),
    #         EventResult(
    #             task_id="node_2",
    #             error=False,
    #             content="result_A",
    #             event_name="test_event",
    #         ),
    #         EventResult(
    #             task_id="node_3",
    #             error=False,
    #             content="result_A",
    #             event_name="test_event",
    #         ),
    #         EventResult(
    #             task_id="node_4",
    #             error=False,
    #             content="result_A",
    #             event_name="test_event",
    #         ),
    #         EventResult(
    #             task_id="node_5",
    #             error=True,
    #             content=Exception("Network partition"),
    #             event_name="test_event",
    #         ),
    #         EventResult(
    #             task_id="node_6",
    #             error=True,
    #             content=Exception("Hardware failure"),
    #             event_name="test_event",
    #         ),
    #         EventResult(
    #             task_id="node_7",
    #             error=False,
    #             content="result_A",
    #             event_name="test_event",
    #         ),
    #     ]
    #
    #     result = evaluator.evaluate(computation_results)
    #
    #     self.assertTrue(result.success)  # 5 out of 7 succeeded (majority)
    #     self.assertGreater(result.success_rate, 50.0)

    # def test_strategy_comparison(self):
    #     """Test the same task results with different strategies."""
    #     task_results = [
    #         EventResult(
    #             task_id="1", error=False, content="test data", event_name="test_event"
    #         ),
    #         EventResult(
    #             task_id="2", error=True, content="test data", event_name="test_event"
    #         ),
    #         EventResult(
    #             task_id="3", error=False, content="test data", event_name="test_event"
    #         ),
    #         EventResult(
    #             task_id="4", error=True, content="test data", event_name="test_event"
    #         ),
    #         EventResult(
    #             task_id="5", error=False, content="test data", event_name="test_event"
    #         ),
    #     ]  # 3 out of 5 succeed (60%)
    #
    #     # Test all strategies
    #     strategies_and_expected = [
    #         (EventEvaluatorFactory.strict_evaluator(), False),  # All must succeed
    #         (EventEvaluatorFactory.lenient_evaluator(), True),  # Any must succeed
    #         (EventEvaluatorFactory.balanced_evaluator(), True),  # Majority (3/5 > 50%)
    #         (EventEvaluatorFactory.threshold_evaluator(2), True),  # At least 2
    #         (EventEvaluatorFactory.threshold_evaluator(4), False),  # At least 4
    #         (EventEvaluatorFactory.percentage_evaluator(50.0), True),  # At least 50%
    #         (EventEvaluatorFactory.percentage_evaluator(70.0), False),  # At least 70%
    #     ]
    #
    #     for evaluator, expected_success in strategies_and_expected:
    #         result = evaluator.evaluate(task_results)
    #         self.assertEqual(
    #             result.success,
    #             expected_success,
    #             f"Strategy '{result.strategy_used}' should return {expected_success}",
    #         )
    #         self.assertEqual(result.success_rate, 60.0)


# Performance and edge case tests
class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""

    # def test_large_number_of_tasks(self):
    #     """Test with a large number of tasks."""
    #     # Create 10,000 tasks
    #     large_task_list = []
    #     for i in range(10000):
    #         error = i % 3 == 0  # ~66.7% success rate
    #         large_task_list.append(
    #             EventResult(
    #                 task_id=f"task_{i}",
    #                 error=error,
    #                 content="test data {}".format(i),
    #                 event_name=f"task_{i}",
    #             )
    #         )
    #
    #     evaluator = EventEvaluatorFactory.percentage_evaluator(60.0)
    #     result = evaluator.evaluate(large_task_list)
    #
    #     self.assertTrue(result.success)
    #     self.assertEqual(result.total_tasks, 10000)
    #     self.assertAlmostEqual(result.success_rate, 66.7, delta=0.1)

    def test_custom_strategy_implementation(self):
        """Test implementing a custom strategy."""

        class AlwaysSucceedStrategy(ExecutionResultEvaluationStrategyBase):
            def evaluate(self, task_results: List[EventResult]) -> bool:
                return True

            def get_strategy_name(self) -> str:
                return "Always Succeed"

        custom_strategy = AlwaysSucceedStrategy()
        evaluator = EventEvaluator(custom_strategy)

        # Even with all failures, should succeed
        all_failures = [
            EventResult(
                task_id="1", error=True, content="test data", event_name="test_event"
            ),
            EventResult(
                task_id="2", error=True, content="test data", event_name="test_event"
            ),
        ]
        result = evaluator.evaluate(all_failures)

        self.assertTrue(result.success)
        self.assertEqual(result.strategy_used, "Always Succeed")
