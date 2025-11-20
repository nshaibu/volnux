import asyncio
import typing

from volnux.result_evaluators import EventEvaluationResult

from .state_manager import ExecutionStatus

if typing.TYPE_CHECKING:
    from .context import ExecutionContext


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
    context_state = context.state
    if context_state.status != ExecutionStatus.COMPLETED:
        return None
    evaluator = context.get_result_evaluator()
    if evaluator is None:
        return None
    return evaluator.evaluate(context_state.results)
