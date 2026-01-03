import typing
import logging
from enum import Enum
from abc import ABC, abstractmethod
from dataclasses import dataclass

from volnux.pipeline import Pipeline
from volnux.parser.protocols import TaskType
from volnux.mixins import ObjectIdentityMixin
from volnux.execution.context import ExecutionContext
from volnux.execution.rehydrator.checkpoint import AutoCheckpointer


logger = logging.getLogger(__name__)


class EngineExecutionResult(Enum):
    COMPLETED = "completed"
    TERMINATED_EARLY = "terminated_early"
    FAILED = "failed"


class CheckPointFrequency(str, Enum):
    PER_TASK = "per_task"  # Before each task execution
    PERIODIC = "periodic"  # Only on timer (from checkpointer)
    ON_STATE_CHANGE = "on_state_change"  # On status changes


class TaskNode(typing.NamedTuple):
    task: TaskType
    previous_context: typing.Optional[ExecutionContext] = None


@dataclass
class EngineResult:
    status: EngineExecutionResult
    final_context: typing.Optional[ExecutionContext] = None
    error: typing.Optional[Exception] = None
    tasks_processed: int = 0


class WorkflowEngine(ObjectIdentityMixin, ABC):
    """
    Abstract interface for workflow execution engines.

    Engines are responsible for orchestrating the execution flow:
    - Task traversal strategy (iterative, recursive, async, etc.)
    - Work queue management
    - Task scheduling and ordering
    - Flow control (loops, branches, parallelism)

    Engines delegate actual task execution, metrics, and hooks to ExecutionContext and Coordinator.
    """

    def __init__(
        self, enable_checkpointing: bool = False, checkpoint_interval: float = 5.0
    ) -> None:
        ObjectIdentityMixin.__init__(self)

        self._tasks_processed: int = 0

        # task queue
        self.queue: typing.Optional[typing.Deque[TaskNode]] = None
        self._current_task_node: typing.Optional[TaskNode] = None

        # Auto checking
        self._checkpointer: typing.Optional[AutoCheckpointer] = None
        self._checkpoint_frequency = None

    @abstractmethod
    async def execute(
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

    def enable_checkpointing(
        self,
        checkpointer: "AutoCheckpointer",
        checkpoint_frequency: CheckPointFrequency = CheckPointFrequency.PER_TASK,
    ):
        """
        Enable automatic checkpointing for this engine.

        Args:
            checkpointer: The checkpointer instance
            checkpoint_frequency: When to checkpoint
        """
        self._checkpointer = checkpointer
        self._checkpoint_frequency = checkpoint_frequency

    async def _checkpoint_before_task(
        self, context: "ExecutionContext", task_node: "TaskNode"
    ):
        """
        Checkpoint before executing a task (idempotency support).

        This is called by the engine before context.dispatch().
        """
        if not self._checkpointer:
            return

        self._current_task_node = task_node

        # Create checkpoint
        if self._checkpoint_frequency == "per_task":
            await context.persist(self._checkpointer.state_store)
            logger.debug(f"Checkpointed before task: {task_node.task.event}")

    async def _checkpoint_after_task(self, context: "ExecutionContext", success: bool):
        """
        Checkpoint after task completion.

        Args:
            context: The execution context
            success: Whether task completed successfully
        """
        if not self._checkpointer:
            return

        self._tasks_processed += 1

        # Clear current task
        self._current_task_node = None

        # Checkpoint if configured
        if self._checkpoint_frequency in [
            CheckPointFrequency.PER_TASK,
            CheckPointFrequency.ON_STATE_CHANGE,
        ]:
            await context.persist(self._checkpointer.state_store)
