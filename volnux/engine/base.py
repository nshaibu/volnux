import typing
import logging
import uuid
from enum import Enum
from abc import ABC, abstractmethod
from dataclasses import dataclass

from .checkpoint_config import CheckPointConfig, CheckPointFrequency
from volnux.execution.pipeline import Pipeline
from volnux.parser.protocols import TaskType
from volnux.execution.context import ExecutionContext
from volnux.execution.rehydrator.checkpoint_manager import VolnuxCheckPointManager

logger = logging.getLogger(__name__)


class EngineExecutionResult(Enum):
    COMPLETED = "completed"
    TERMINATED_EARLY = "terminated_early"
    SUSPENDED = "suspended"
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


class SubgraphErrorStrategy(Enum):
    """How parent engines handle child execution errors."""

    BUBBLE_UP = "bubble"  # Re-raise exception, crash parent
    TREAT_AS_FAILURE = "failure"  # Map to FAILED status, follow on_failure
    ISOLATE = "isolate"  # Log error, continue parent execution


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

    def __init__(
        self,
        enable_checkpointing: bool = False,
        checkpoint_config: typing.Optional[CheckPointConfig] = None,
        parent_engine: typing.Optional["WorkflowEngine"] = None,
    ) -> None:
        self.tasks_processed: int = 0
        self.current_task_node: typing.Optional["TaskNode"] = None
        self.final_context: typing.Optional["ExecutionContext"] = None

        # Sub-engine hierarchy
        self.parent_engine = parent_engine
        self.child_engines: typing.List["WorkflowEngine"] = []
        self._engine_id = uuid.uuid4().hex[:8]

        # Error handling strategy (only used by sub-engines)
        self._error_strategy: SubgraphErrorStrategy = (
            SubgraphErrorStrategy.TREAT_AS_FAILURE
        )

        self._checkpointer: typing.Optional[VolnuxCheckPointManager] = None
        self.checkpoint_config: typing.Optional[CheckPointConfig] = None

        if enable_checkpointing:
            self.checkpoint_config = (
                checkpoint_config if checkpoint_config else CheckPointConfig()
            )

    def is_root_engine(self) -> bool:
        """Check if this is the top-level engine."""
        return self.parent_engine is None

    def get_root_engine(self) -> "WorkflowEngine":
        """Traverse up to find the root engine."""
        engine = self
        while engine.parent_engine is not None:
            engine = engine.parent_engine
        return engine

    def get_engine_depth(self) -> int:
        """Return nesting level (0 = root)."""
        depth = 0
        engine = self
        while engine.parent_engine is not None:
            depth += 1
            engine = engine.parent_engine
        return depth

    def get_all_child_engines(self) -> typing.List["WorkflowEngine"]:
        """Recursively collect all descendant engines."""
        all_children = []
        for child in self.child_engines:
            all_children.append(child)
            all_children.extend(child.get_all_child_engines())
        return all_children

    @abstractmethod
    async def spawn_sub_engine(
        self,
        root_task: "TaskType",
        pipeline: "Pipeline",
        error_strategy: SubgraphErrorStrategy = SubgraphErrorStrategy.TREAT_AS_FAILURE,
    ) -> "WorkflowEngine":
        """
        Spawn a child engine for {} grouping or nested workflow execution.

        The child inherits the parent's checkpoint manager (workflows share checkpoints).
        State isolation is automatic via ExecutionContext.spawn_child().

        Args:
            root_task: Entry point for the sub-workflow
            pipeline: Pipeline context (shared across parent/child)
            error_strategy: How to handle child exceptions:
                - BUBBLE_UP: Re-raise child errors, crash parent
                - TREAT_AS_FAILURE: Map errors to FAILED status, follow on_failure
                - ISOLATE: Log error, continue parent execution

        Returns:
            Configured child engine instance
        """
        pass

    @property
    @abstractmethod
    def task_queue(self) -> typing.Any:
        """
        Primary engine queue used for active task scheduling.

        Concrete engines decide the backing implementation.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def sink_queue(self) -> typing.Any:
        """
        Queue used for deferred/sink tasks.

        Concrete engines decide the backing implementation.
        """
        raise NotImplementedError()

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
        """
        pass

    def get_name(self) -> str:
        """
        Get the engine name/identifier.

        Returns:
            Human-readable engine name
        """
        return self.__class__.__name__

    def enable_checkpointing(
        self,
        checkpointer: "VolnuxCheckPointManager",
        checkpoint_frequency: "CheckPointFrequency" = CheckPointFrequency.PER_TASK,
    ):
        """
        Enable automatic checkpointing for this engine.
        """
        self._checkpointer = checkpointer
        self._checkpoint_frequency = checkpoint_frequency

    async def _checkpoint_before_task(
        self, context: "ExecutionContext", task_node: "TaskNode"
    ):
        """
        Checkpoint before executing a task (idempotency support).
        """
        if not self._checkpointer:
            return

        self.current_task_node = task_node

        if self._checkpoint_frequency == CheckPointFrequency.PER_TASK:
            await context.persist()
            logger.debug(f"Checkpointed before task: {task_node.task.event}")

    async def _checkpoint_after_task(self, context: "ExecutionContext", success: bool):
        """
        Checkpoint after task completion.
        """
        if not self._checkpointer:
            return

        self.tasks_processed += 1
        self.current_task_node = None

        if self._checkpoint_frequency in [
            CheckPointFrequency.PER_TASK,
            CheckPointFrequency.ON_STATE_CHANGE,
        ]:
            await context.persist()
            logger.debug(
                f"Checkpointed after task: {self.tasks_processed} tasks processed"
            )
