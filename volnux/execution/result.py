import asyncio
import logging
import typing
from contextlib import contextmanager

from volnux.exceptions import StopProcessingError, SwitchTask
from volnux.result import EventResult, ResultSet, ResultStream
from volnux.result_evaluators import (
    EventEvaluationResult,
    EventEvaluator,
    ExecutionResultEvaluationStrategyBase,
)

logger = logging.getLogger(__name__)

_CONTROL_FLOW_EXCEPTIONS = (StopProcessingError, SwitchTask)


class ResultProcessor:
    """Handles result processing and aggregation with configurable evaluation strategies."""

    def __init__(
        self, strategy: typing.Optional[ExecutionResultEvaluationStrategyBase] = None
    ) -> None:
        """
        Initialize the ResultProcessor.

        Args:
            strategy: Optional event evaluator for result evaluation.
        """
        self._evaluator = EventEvaluator(strategy=strategy)  # type: ignore

    def change_strategy(
        self, strategy: ExecutionResultEvaluationStrategyBase
    ) -> "ResultProcessor":
        """
        Change the evaluation strategy.

        Args:
            strategy: New event evaluator to use.

        Returns:
            Self for method chaining.
        """
        self._evaluator.change_strategy(strategy)
        return self

    @staticmethod
    async def process_futures(
        futures: typing.Sequence[asyncio.Future],
    ) -> typing.Tuple[ResultSet, ResultSet]:
        """
        Process futures and separate successful results from errors.

        Args:
            futures: Sequence of asyncio futures to process.

        Returns:
            Tuple of (successful_results, errors).
        """
        results = ResultSet()
        errors = ResultSet()

        completed = await asyncio.gather(*futures, return_exceptions=True)

        for result in completed:
            ResultProcessor._route_result(result, results, errors)

        return results, errors

    @staticmethod
    def _route_result(
        result: typing.Any, results: ResultSet, errors: ResultSet
    ) -> None:
        """
        Route a single resolved future value into results/errors.

        A future can resolve to a collection (e.g. the aggregated output of
        a multitask/parallel dispatch) rather than a single EventResult —
        those are flattened here instead of being dropped.
        """
        if isinstance(result, Exception):
            errors.add(result)
        elif isinstance(result, (list, tuple, ResultSet)):
            for item in result:
                ResultProcessor._route_result(item, results, errors)
        else:
            if not isinstance(result, EventResult):
                logger.warning(
                    "Unexpected future result type %s; adding as-is",
                    type(result).__name__,
                )
            results.add(result)

    @contextmanager
    def _temporary_strategy(
        self, strategy: typing.Optional[ExecutionResultEvaluationStrategyBase]
    ) -> typing.Generator[None, None, None]:
        """
        Context manager for temporarily changing evaluation strategy.

        Args:
            strategy: Temporary strategy to use, or None to keep current.

        Yields:
            None
        """
        if strategy is None:
            yield
            return

        original_strategy = self._evaluator.strategy
        try:
            self._evaluator.change_strategy(strategy)
            yield
        finally:
            self._evaluator.change_strategy(original_strategy)

    def evaluate_execution(
        self,
        results: ResultSet,
        strategy: typing.Optional[ExecutionResultEvaluationStrategyBase] = None,
    ) -> EventEvaluationResult:
        """
        Evaluate execution results using the specified or current strategy.

        Args:
            results: ResultSet to evaluate.
            strategy: Optional temporary strategy to use for this evaluation.

        Returns:
            EventEvaluationResult containing the evaluation outcome.
        """
        with self._temporary_strategy(strategy):
            return self._evaluator.evaluate(results)

    @staticmethod
    async def process_errors(
        errors: ResultSet,
    ) -> ResultSet:
        """
        Evaluate error results using the specified or current strategy.

        Args:
            errors: ResultSet of errors to evaluate.

        Returns:
            ResultSet of processed error EventResults.

        """
        results = ResultSet()
        for error in errors:
            if not isinstance(error, _CONTROL_FLOW_EXCEPTIONS):
                params = getattr(error, "params", {})
                event_name = params.get("event_name", "unknown")

                result = EventResult(
                    error=True,
                    content=(
                        error.to_dict() if hasattr(error, "to_dict") else str(error)
                    ),
                    task_id=params.get("task_id"),
                    event_name=event_name,
                )

                results.add(result)

        return results
