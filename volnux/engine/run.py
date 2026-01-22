from volnux.parser.protocols import TaskType
from volnux.pipeline import Pipeline

from .base import EngineExecutionResult, EngineResult
from .default_engine import DefaultWorkflowEngine

# Global default engine instance
_default_engine = DefaultWorkflowEngine(strict_mode=True)


async def run_workflow(
    root_task: TaskType,
    pipeline: Pipeline,
) -> EngineResult:
    result = await _default_engine.execute(root_task, pipeline)

    if result.status == EngineExecutionResult.FAILED and result.error:
        raise result.error
    return result
