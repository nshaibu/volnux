import typing
from enum import Enum
from abc import ABC, abstractmethod
from dataclasses import dataclass

from volnux.pipeline import Pipeline
from volnux.parser.protocols import TaskType
from volnux.execution.context import ExecutionContext


class EngineExecutionResult(Enum):
    COMPLETED = "completed"
    TERMINATED_EARLY = "terminated_early"
    FAILED = "failed"


class TaskNode(typing.NamedTuple):
    task: TaskType
    previous_context: typing.Optional[ExecutionContext] = None


@dataclass
class EngineResult:
    status: EngineExecutionResult
    final_context: typing.Optional[ExecutionContext] = None
    error: typing.Optional[Exception] = None
    tasks_processed: int = 0


class WorkflowEngine(ABC):
    """
    Abstract interface for workflow execution engines.

    Engines are responsible for orchestrating the execution flow:
    - Task traversal strategy (iterative, recursive, async, etc.)
    - Work queue management
    - Task scheduling and ordering
    - Flow control (loops, branches, parallelism)

    Engines delegate actual task execution, metrics, and hooks to ExecutionContext and Coordinator.
    """

    @abstractmethod
    def execute(
        self,
        root_task: TaskType,
        pipeline: Pipeline,
    ) -> EngineResult:
        """
        Execute a workflow starting from the root task.

        The engine orchestrates the flow but delegates execution to contexts.
        All metrics, hooks, and task execution are handled by ExecutionContext
        and the Coordinator.

        Args:
            root_task: The entry point task for the workflow
            pipeline: The pipeline containing workflow configuration and state

        Returns:
            EngineResult with high-level execution status
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """
        Get the engine name/identifier.

        Returns:
            Human-readable engine name
        """
        pass
