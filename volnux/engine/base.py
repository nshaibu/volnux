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

        # task id -> event rehydrated from a checkpoint, to run in place of the fresh
        # event that task would otherwise build (see stage_resumed_event)
        self._resumed_events: typing.Dict[str, typing.Any] = {}

        # Streams wired across sub-engines (see StreamFlow). Held by the root engine
        # and reached through ``get_root_engine()``: the streaming events that are
        # running, by task id (so a failed consumer can ask its producer to stop),
        # and the producers whose plain result must be published as a one-chunk stream.
        self._stream_events: typing.Dict[str, typing.Any] = {}
        self._stream_plain_producers: typing.Set[str] = set()

        self._checkpointer: typing.Optional[VolnuxCheckPointManager] = None
        # True when this engine built the manager itself (and must stop it);
        # a manager handed in through enable_checkpointing() belongs to the caller.
        self._owns_checkpointer: bool = False
        self.checkpoint_config: typing.Optional[CheckPointConfig] = None
        self._checkpoint_frequency: CheckPointFrequency = CheckPointFrequency.PER_TASK

        if enable_checkpointing:
            self.checkpoint_config = (
                checkpoint_config
                if checkpoint_config
                else CheckPointConfig.from_settings()
            )
            self._checkpoint_frequency = self.checkpoint_config.policy.frequency

    def is_root_engine(self) -> bool:
        """Check if this is the top-level engine."""
        return self.parent_engine is None

    @property
    def checkpoint_manager(self) -> typing.Optional[VolnuxCheckPointManager]:
        """The checkpoint manager in use (the root engine's, for sub-engines)."""
        return self._checkpointer

    def get_root_engine(self) -> "WorkflowEngine":
        """Traverse up to find the root engine."""
        engine = self
        while engine.parent_engine is not None:
            engine = engine.parent_engine
        return engine

    def register_stream_event(self, task_id: str, event: typing.Any) -> None:
        self.get_root_engine()._stream_events[task_id] = event

    def forget_stream_event(self, task_id: str) -> None:
        self.get_root_engine()._stream_events.pop(task_id, None)

    def get_stream_event(self, task_id: str) -> typing.Optional[typing.Any]:
        return self.get_root_engine()._stream_events.get(task_id)

    def mark_plain_stream_producers(self, task_ids: typing.Iterable[str]) -> None:
        """Producers that are not generators but feed a stream consumer elsewhere."""
        self.get_root_engine()._stream_plain_producers.update(task_ids)

    def publishes_plain_stream(self, task_id: str) -> bool:
        return task_id in self.get_root_engine()._stream_plain_producers

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

    def stage_resumed_event(self, task_id: str, event: typing.Any) -> None:
        """Stage an event rehydrated from a checkpoint for ``task_id``.

        When the engine reaches that task it runs this event (with its state, phase
        and injected results) instead of building a fresh one. Call it before
        ``resume()``.
        """
        self._resumed_events[task_id] = event

    @abstractmethod
    async def resume(self, pipeline: Pipeline) -> EngineResult:
        """
        Continue a workflow from where a rehydrated engine stopped.

        The engine comes from ``LazyRehydrator.resume_from``: its work queue holds
        the interrupted task first, then whatever was still to do, and its sink
        queue the deferred sinks. The interrupted task runs again (with any event
        staged through ``stage_resumed_event``) and the workflow proceeds as an
        uninterrupted run would.

        Returns the result of the resumed run: it may complete, fail, or suspend
        again.
        """
        pass

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
        self._owns_checkpointer = False
        self._checkpoint_frequency = checkpoint_frequency

    async def _start_checkpointing(self) -> None:
        """Root engine only: make sure a manager exists and is running.

        The manager is an asyncio component (two tasks on the running loop), not a
        process. When checkpointing is enabled and the caller supplied none, the
        engine builds one from ``checkpoint_config`` and owns its lifetime.
        Sub-engines never start or stop it; they share the root's.
        """
        if not self.is_root_engine():
            return
        if self._checkpointer is None and self.checkpoint_config is not None:
            self._checkpointer = VolnuxCheckPointManager.from_config(
                self.checkpoint_config
            )
            self._owns_checkpointer = True
        if self._checkpointer is not None:
            await self._checkpointer.start()

    async def _stop_checkpointing(self) -> None:
        """Root engine only: persist what is queued and stop a manager we built.

        Runs on every way out of ``execute()`` (completed, failed, suspended,
        terminated early), so queued snapshots are never lost.
        """
        if not self.is_root_engine() or self._checkpointer is None:
            return
        manager = self._checkpointer
        try:
            await manager.flush()
        except Exception:
            logger.warning("Failed to flush the checkpoint manager", exc_info=True)
        if self._owns_checkpointer:
            try:
                await manager.stop()
            except Exception:
                logger.warning("Failed to stop the checkpoint manager", exc_info=True)
            self._checkpointer = None
            self._owns_checkpointer = False

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
            await context.save()
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
            await context.save()
            logger.debug(
                f"Checkpointed after task: {self.tasks_processed} tasks processed"
            )
