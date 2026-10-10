import asyncio
import typing

from volnux.result_evaluators import EventEvaluationResult

from .status import ExecutionStatus

if typing.TYPE_CHECKING:
    from .context import ExecutionContext


# Module-level JSON-safe type whitelist
_JSON_SAFE_TYPES = (str, int, float, bool, type(None))


def is_json_serializable(value) -> bool:
    """Check if a value is JSON-serializable (type-based, no trial serialization)."""
    if isinstance(value, _JSON_SAFE_TYPES):
        return True
    if isinstance(value, (list, tuple)):
        return all(is_json_serializable(item) for item in value)
    if isinstance(value, dict):
        return all(
            isinstance(k, str) and is_json_serializable(v) for k, v in value.items()
        )
    return False


def evaluate_context_execution_results(
    context: "ExecutionContext",
) -> typing.Optional[EventEvaluationResult]:
    """
    Evaluate the result of execution tasks

    Args:
        context (ExecutionContext): Context of execution of the tasks

    Returns:
        typing.Optional[EventEvaluationResult]: Summary of the execution results
    """
    if context.status != ExecutionStatus.COMPLETED:
        return None
    evaluator = context.get_result_evaluator()
    if evaluator is None:
        return None
    return evaluator.evaluate(context.results)
