from volnux.parser.protocols import TaskType
from volnux.pipeline import Pipeline

from .base import EngineExecutionResult
from .default_engine import DefaultWorkflowEngine

# Global default engine instance
_default_engine = DefaultWorkflowEngine(strict_mode=True)


def run_workflow(
    root_task: TaskType,
    pipeline: Pipeline,
) -> None:
    result = _default_engine.execute(root_task, pipeline)

    if result.status == EngineExecutionResult.FAILED and result.error:
        raise result.error
