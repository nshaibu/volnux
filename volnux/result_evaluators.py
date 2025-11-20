import logging
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass

from .result import EventResult

__all__ = [
    "EventEvaluationResult",
    "AllTasksMustSucceedStrategy",
    "NoFailuresAllowedStrategy",
    "AnyTaskMustSucceedStrategy",
    "MajorityTasksMustSucceedStrategy",
    "MinimumSuccessThresholdStrategy",
    "PercentageSuccessThresholdStrategy",
    "ResultEvaluationStrategies",
    "EventEvaluator",
    "ExecutionResultEvaluationStrategyBase",
]


if typing.TYPE_CHECKING:
    from .result import ResultSet


T = typing.TypeVar("T", typing.List[EventResult], "ResultSet")


@dataclass
class EventEvaluationResult:
    success: bool
    total_tasks: int
    successful_tasks: int
    failed_tasks: int
    strategy_used: str

    @property
    def success_rate(self) -> float:
        """Returns the success rate as a percentage."""
        if self.total_tasks == 0:
            return 0.0
        return (self.successful_tasks / self.total_tasks) * 100

    @property
    def has_partial_success(self) -> bool:
        """Returns True if some but not all tasks succeeded."""
        return 0 < self.successful_tasks < self.total_tasks


class ExecutionResultEvaluationStrategyBase(ABC):
    @abstractmethod
    def evaluate(self, task_results: T) -> bool:
        """
        Evaluate whether the event should be considered successful.

        Args:
            task_results: List of individual task results

        Returns:
            bool: True if the event meets the success criteria
        """
        pass

    @abstractmethod
    def get_strategy_name(self) -> str:
        """Return a human-readable name for this strategy."""
        pass


class AllTasksMustSucceedStrategy(ExecutionResultEvaluationStrategyBase):
    """Event succeeds only if ALL tasks succeed."""

    def evaluate(self, task_results: T) -> bool:
        if not task_results:
            return False
        return all(result.success for result in task_results)

    def get_strategy_name(self) -> str:
        return "All Tasks Must Succeed"


class AnyTaskMustSucceedStrategy(ExecutionResultEvaluationStrategyBase):
    """Event succeeds if ANY task succeeds."""

    def evaluate(self, task_results: T) -> bool:
        if not task_results:
            return False
        return any(result.success for result in task_results)

    def get_strategy_name(self) -> str:
        return "Any Task Must Succeed"


class MajorityTasksMustSucceedStrategy(ExecutionResultEvaluationStrategyBase):
    """Event succeeds if majority of tasks succeed."""

    def __init__(self, tie_breaker: bool = True):
        """
        Args:
            tie_breaker: What to return when exactly 50% succeed (for even number of tasks)
        """
        self.tie_breaker = tie_breaker

    def evaluate(self, task_results: T) -> bool:
        if not task_results:
            return False

        successful_count = sum(1 for result in task_results if result.success)
        total_count = len(task_results)

        if successful_count > total_count / 2:
            return True
        elif successful_count < total_count / 2:
            return False
        else:
            # Exactly 50% - use tie breaker
            return self.tie_breaker

    def get_strategy_name(self) -> str:
        return f"Majority Must Succeed (tie_breaker={self.tie_breaker})"


class MinimumSuccessThresholdStrategy(ExecutionResultEvaluationStrategyBase):
    """Event succeeds if at least N tasks succeed."""

    def __init__(self, minimum_successes: int):
        if minimum_successes < 0:
            raise ValueError("minimum_successes must be non-negative")
        self.minimum_successes = minimum_successes

    def evaluate(self, task_results: T) -> bool:
        if not task_results and self.minimum_successes == 0:
            return True

        successful_count = sum(1 for result in task_results if result.success)
        return successful_count >= self.minimum_successes

    def get_strategy_name(self) -> str:
        return f"At Least {self.minimum_successes} Tasks Must Succeed"


class PercentageSuccessThresholdStrategy(ExecutionResultEvaluationStrategyBase):
    """Event succeeds if at least X% of tasks succeed."""

    def __init__(self, success_percentage: float):
        if not 0 <= success_percentage <= 100:
            raise ValueError("success_percentage must be between 0 and 100")
        self.success_percentage = success_percentage

    def evaluate(self, task_results: T) -> bool:
        if not task_results:
            return self.success_percentage == 0

        successful_count = sum(1 for result in task_results if result.success)
        actual_percentage = (successful_count / len(task_results)) * 100
        return actual_percentage >= self.success_percentage

    def get_strategy_name(self) -> str:
        return f"At Least {self.success_percentage}% Must Succeed"


class NoFailuresAllowedStrategy(ExecutionResultEvaluationStrategyBase):
    """Event succeeds if NO tasks fail (but allows empty task list)."""

    def evaluate(self, task_results: T) -> bool:
        # Empty list is considered success (no failures)
        return all(result.success for result in task_results)

    def get_strategy_name(self) -> str:
        return "No Failures Allowed"


class ResultEvaluationStrategies:
    ALL_MUST_SUCCEED = AllTasksMustSucceedStrategy()
    ANY_MUST_SUCCEED = AnyTaskMustSucceedStrategy()
    MAJORITY_MUST_SUCCEED = MajorityTasksMustSucceedStrategy()
    NO_FAILURES_ALLOWED = NoFailuresAllowedStrategy()

    @staticmethod
    def at_least_n_succeed(n: int) -> MinimumSuccessThresholdStrategy:
        return MinimumSuccessThresholdStrategy(n)

    @staticmethod
    def at_least_percent_succeed(
        percentage: float,
    ) -> PercentageSuccessThresholdStrategy:
        return PercentageSuccessThresholdStrategy(percentage)

    def __getattr__(self, name: str) -> ExecutionResultEvaluationStrategyBase:
        """
        Dynamic attribute access for parametrized evaluation strategies.
        Supports magic strategy access where parameters can be passed by appending them
        to the strategy name with an underscore separator.

        Examples:
            - PercentageSuccessThresholdStrategy_75
            - MinimumSuccessThresholdStrategy_5
            - PercentageSuccessThresholdStrategy_85.5
            - MinimumSuccessThresholdStrategy_2.5

        Args:
            name: The attribute name, potentially with parameters

        Returns:
            An instance of the requested evaluation strategy

        Raises:
            AttributeError: If the strategy is not found or parameters are invalid
            ValueError: If the factor cannot be parsed as an integer
        """
        # Check if this is parametrized strategy access
        if "_" in name:
            parts = name.split("_")
            if len(parts) == 2:
                evaluator_name, factor_str = parts

                try:
                    factor = float(factor_str)
                except ValueError:
                    raise AttributeError(
                        f"Invalid factor '{factor_str}' in '{name}'. Factor must be a number."
                    )

                evaluator_name_lower = evaluator_name.lower()

                parametrized_strategies = {
                    PercentageSuccessThresholdStrategy.__name__.lower(): PercentageSuccessThresholdStrategy,
                    MinimumSuccessThresholdStrategy.__name__.lower(): MinimumSuccessThresholdStrategy,
                }

                strategy_class = parametrized_strategies.get(evaluator_name_lower)
                if strategy_class:
                    try:
                        return strategy_class(factor)
                    except Exception as e:
                        raise AttributeError(
                            f"Failed to create {evaluator_name} with factor {factor}: {e}"
                        )

        # If not a known parametrized strategy, fall through to default behavior
        try:
            return self.__getattribute__(name)
        except AttributeError:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )


class EventEvaluator:
    def __init__(self, strategy: ExecutionResultEvaluationStrategyBase):
        """
        Main class for evaluating event outcomes based on task results.
        Args:
            strategy: The evaluation strategy to use
        """
        self.strategy = strategy

    def evaluate(self, task_results: T) -> EventEvaluationResult:
        """
        Evaluate the event outcome based on task results.
        Args:
            task_results: List of individual task results
        Returns:
            EventEvaluationResult: Detailed evaluation result
        """
        successful_tasks = sum(1 for result in task_results if result.success)
        failed_tasks = len(task_results) - successful_tasks

        success = self.strategy.evaluate(task_results)

        return EventEvaluationResult(
            success=success,
            total_tasks=len(task_results),
            successful_tasks=successful_tasks,
            failed_tasks=failed_tasks,
            strategy_used=self.strategy.get_strategy_name(),
        )

    def change_strategy(
        self, new_strategy: ExecutionResultEvaluationStrategyBase
    ) -> None:
        """Change the evaluation strategy."""
        self.strategy = new_strategy
