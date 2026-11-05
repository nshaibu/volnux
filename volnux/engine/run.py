from volnux.parser.protocols import TaskType
from volnux.execution.pipeline import Pipeline

from .base import EngineExecutionResult, EngineResult
from .default_engine import DefaultWorkflowEngine

# Deprecated: kept so that code importing it still works. A run does NOT use it:
# an engine holds the state of one run (queues, counters, child engines), so runs
# that share an engine corrupt each other.
_default_engine = DefaultWorkflowEngine(strict_mode=True)


async def run_workflow(
    root_task: TaskType,
    pipeline: Pipeline,
) -> EngineResult:
    """Run a workflow on an engine of its own (concurrent runs are independent)."""
    engine = DefaultWorkflowEngine(strict_mode=True)
    result = await engine.execute(root_task, pipeline)

    if result.status == EngineExecutionResult.FAILED and result.error:
        raise result.error
    return result
