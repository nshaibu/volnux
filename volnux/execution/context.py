import asyncio
import weakref
import logging
import time
import datetime
import typing
from collections import deque
from dataclasses import dataclass, field

from pydantic_mini import Attrib, BaseModel, MiniAnnotated
from pydantic_mini.exceptions import ValidationError as PydanticMiniError

from volnux.mixins import ObjectIdentityMixin
from volnux.parser.operator import PipeType
from volnux.parser.options import ResultEvaluationStrategy
from volnux.parser.protocols import TaskType
from volnux.pipeline import Pipeline
from volnux.result import EventResult, ResultSet
from volnux.result_evaluators import EventEvaluator, ResultEvaluationStrategies
from volnux.signal.signals import (
    event_execution_aborted,
    event_execution_cancelled,
    event_execution_failed,
)
from volnux.task import PipelineTask, PipelineTaskGrouping
from volnux.concurrency.async_utils import to_thread

from .state_manager import ExecutionState, ExecutionStatus, StateManager

if typing.TYPE_CHECKING:
    from volnux.engine.base import WorkflowEngine
    from volnux.execution.rehydrator.snapshot import ContextSnapshot

logger = logging.getLogger(__name__)


@dataclass
class ExecutionMetrics:
    """Execution timing and statistics"""

    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time if self.end_time else 0.0


def preformat_task_profile(
    task_profiles: typing.Union[
        TaskType, typing.List[TaskType], typing.Deque[TaskType]
    ],
) -> typing.Deque[TaskType]:
    if isinstance(task_profiles, (PipelineTask, PipelineTaskGrouping)):
        return deque([task_profiles]) # type: ignore
    elif isinstance(task_profiles, (list, tuple)):
        return deque(task_profiles)
    elif isinstance(task_profiles, deque):
        return task_profiles
    # TODO: descriptive error message
    raise PydanticMiniError("invalid task format")  # type: ignore


class ExecutionContext(ObjectIdentityMixin, BaseModel):
    """
    Represents the execution context for a particular event in the pipeline.

    This class encapsulates the necessary data and state associated with
    executing an event, such as the task being processed and the pipeline
    it belongs to.

    Individual events executed concurrently must acquire the "conditional_variable"
    before they can make any changes to the execution context. This ensures that only one
    event can modify the context at a time, preventing race conditions and ensuring thread safety.

    Attributes:
        task_profiles: The specific PipelineTask that is being executed.
        pipeline: The Pipeline that orchestrates the execution of the task.

    Details:
        Represents the execution context of the pipeline as a bidirectional (doubly-linked) list.
        Each node corresponds to an event's execution context, allowing traversal both forward and backward
        through the pipeline's events.

        You can filter contexts by event name using the `filter_by_event` method.

        The context is iterable in the forward direction, so you can loop through it like this:
            for context in pipeline.start():
                pass

        To access specific ends of the context queue:
        - Use `get_head_context()` to retrieve the head (starting context).
        - Use `get_tail_context()` to retrieve the tail (ending context).

        Reverse traversal can be done by walking backward from the tail using the linked structure.
    """

    task_profiles: MiniAnnotated[
        typing.Deque[TaskType],
        Attrib(pre_formatter=preformat_task_profile),
    ]
    pipeline: Pipeline
    metrics: ExecutionMetrics = field(default_factory=lambda: ExecutionMetrics())

    # Horizontal Links (Linked list)
    previous_context: typing.Optional["ExecutionContext"] = None
    next_context: typing.Optional["ExecutionContext"] = None

    # Vertical Links (The Tree)
    parent_context: typing.Optional["ExecutionContext"] = None
    child_contexts: typing.List["ExecutionContext"] = field(default_factory=list)

    _state_manager: typing.ClassVar[typing.Optional["StateManager"]] = None

    # Weak reference to the engine (not persisted)
    _engine_ref: typing.Optional[weakref.ReferenceType] = None

    # Workflow identifier for grouping contexts
    workflow_id: str = None

    # Checkpoint data for idempotency
    _task_checkpoint: typing.Optional[typing.Dict[str, typing.Any]] = None

    class Config:
        disable_typecheck = True
        disable_all_validation = True

    def __model_init__(
        self, *args: typing.Tuple[typing.Any], **kwargs: typing.Dict[str, typing.Any]
    ) -> None:
        from .state_manager import ExecutionState, ExecutionStatus, StateManager

        super().__init__(*args, **kwargs)  # type: ignore

        # Initialize shared state manager
        if self.__class__._state_manager is None:
            self.__class__._state_manager = StateManager()

        # Create state in shared memory with its own lock
        initial_state = ExecutionState(ExecutionStatus.PENDING)
        self._state_manager.create_state(self.state_id, initial_state)

    @property
    def state_id(self) -> str:
        return self.id

    @property
    def state(self) -> "ExecutionState":
        """
        Get the current state from shared memory.
        """
        return self.get_state_manager().get_state(self.state_id)

    @property
    async def state_async(self) -> "ExecutionState":
        """
        Async version of getting current state from shared memory.
        """
        return await self.get_state_manager().get_state_async(self.state_id)

    def spawn_child(self, task_profiles: typing.Deque[TaskType]) -> "ExecutionContext":
        """
        Factory method to create a nested context (a branch in the tree).
        Ensures the parent remains 'alive' by holding a reference.
        """
        # Child inherit the same StateManager but get a unique state_id
        child = ExecutionContext(
            task_profiles=task_profiles, # type: ignore
            pipeline=self.pipeline, # type: ignore
            parent_context=self,  # type: ignore
        )
        self.child_contexts.append(child)  # Link Down
        return child

    @property
    def is_root(self) -> bool:
        return self.parent_context is None

    @property
    def is_leaf(self) -> bool:
        return len(self.child_contexts) == 0

    def get_root_context(self) -> "ExecutionContext":
        """Climb the tree to find the absolute start of the orchestration."""
        current = self
        while current.parent_context:
            current = current.parent_context
        return current

    def get_depth(self) -> int:
        """Calculates nesting level for directive validation."""
        depth = 0
        current = self
        while current.parent_context:
            depth += 1
            current = current.parent_context
        return depth

    async def _evaluate_group_finality(self):
        """
        Internal check: Is every child context in this subtree finished?
        """
        all_done = True
        for child in self.child_contexts:
            child_state = await child.state_async
            if child_state.status not in [
                ExecutionStatus.COMPLETED,
                ExecutionStatus.FAILED,
            ]:
                all_done = False
                break

        if all_done:
            # The 'Super-Task' is now officially complete
            await self.update_status_async(ExecutionStatus.COMPLETED)

    def get_state_manager(self) -> StateManager:
        """
        Get context state manager

        Returns:
            The state manager for this context
        """
        if self.__class__._state_manager is None:
            state_manager = StateManager()
            initial_state = ExecutionState(ExecutionStatus.PENDING)
            state_manager.create_state(self.state_id, initial_state)
            self.__class__._state_manager = state_manager
            return state_manager
        return self._state_manager

    def update_status(self, new_status: "ExecutionStatus") -> None:
        self.get_state_manager().update_status(self.state_id, new_status)

    async def update_status_async(self, new_status: "ExecutionStatus") -> None:
        await self.get_state_manager().update_status_async(self.state_id, new_status)

        # If this child is done, signal the parent to check its 'Group' status
        if new_status == ExecutionStatus.COMPLETED and self.parent_context:
            await self.parent_context._evaluate_group_finality()

    def add_error(self, error: Exception) -> None:
        self.get_state_manager().append_error(self.state_id, error)

    async def add_error_async(self, error: Exception) -> None:
        await self.get_state_manager().append_error_async(self.state_id, error)

    def add_result(self, result: EventResult) -> None:
        self.get_state_manager().append_result(self.state_id, result)

    async def add_result_async(self, result: EventResult) -> None:
        await self.get_state_manager().append_result_async(self.state_id, result)

    def cancel(self) -> None:
        """
        Cancel execution - only locks THIS context.
        Other contexts continue running unaffected.
        """
        self.get_state_manager().update_status(self.state_id, ExecutionStatus.CANCELLED)
        # Emit event
        event_execution_cancelled.emit(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.CANCELLED,
        )

    async def cancel_async(self) -> None:
        """
        Async version of cancel execution - only locks THIS context.
        Other contexts continue running unaffected.
        """
        await self.get_state_manager().update_status_async(
            self.state_id, ExecutionStatus.CANCELLED
        )
        # Emit event
        await event_execution_cancelled.emit_async(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.CANCELLED,
        )

    def abort(self) -> None:
        """
        Abort execution - only locks THIS context.
        Other contexts continue running unaffected.
        """
        self.get_state_manager().update_status(self.state_id, ExecutionStatus.ABORTED)
        # Emit event
        event_execution_aborted.emit(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.ABORTED,
        )

    async def abort_async(self) -> None:
        """
        Async version of abort execution - only locks THIS context.
        Other contexts continue running unaffected.
        """
        await self.get_state_manager().update_status_async(
            self.state_id, ExecutionStatus.ABORTED
        )
        # Emit event
        await event_execution_aborted.emit_async(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.ABORTED,
        )

    def failed(self) -> None:
        """
        Mark the execution context as failed.
        """
        self.get_state_manager().update_status(self.state_id, ExecutionStatus.FAILED)
        # Emit event
        event_execution_failed.emit(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.FAILED,
        )

    async def failed_async(self) -> None:
        """
        Async version of marking the execution context as failed.
        """
        await self.get_state_manager().update_status_async(
            self.state_id, ExecutionStatus.FAILED
        )
        # Emit event
        await event_execution_failed.emit_async(
            sender=self.__class__,
            task_profiles=self.get_task_profiles().copy(),
            execution_context=self,
            state=ExecutionStatus.FAILED,
        )

    def get_state_snapshot(self) -> "ExecutionState":
        """Get a thread-safe copy of current state."""
        return self.state

    def bulk_update(
        self,
        status: typing.Optional["ExecutionStatus"] = None,
        errors: typing.Optional[typing.Sequence[Exception]] = None,
        results: typing.Optional[typing.Sequence[EventResult]] = None,
    ) -> None:
        """Efficient bulk update"""
        state = self.state
        if status is not None:
            state.status = status
        if errors is not None:
            state.errors.extend(errors)  # type: ignore
        if results is not None:
            state.results.extend(results)
        self.get_state_manager().update_state(self.state_id, state)  # type: ignore

    async def bulk_update_async(
        self,
        status: typing.Optional["ExecutionStatus"] = None,
        errors: typing.Optional[typing.Sequence[Exception]] = None,
        results: typing.Optional[typing.Sequence[EventResult]] = None,
    ) -> None:
        """Async version of efficient bulk update"""
        state = await self.state_async
        if status is not None:
            state.status = status
        if errors is not None:
            state.errors.extend(errors)  # type: ignore
        if results is not None:
            state.results.extend(results)
        await self.get_state_manager().update_state_async(self.state_id, state)

    async def update_aggregated_result(self, result: "EventResult") -> None:
        state = await self.state_async
        state.aggregated_result = result
        self.get_state_manager().update_state(self.state_id, state)

    def __iter__(self) -> typing.Generator["ExecutionContext", typing.Any, None]:
        current: typing.Optional["ExecutionContext"] = self
        while current is not None:
            yield current
            current = current.next_context

    def __hash__(self) -> int:
        return hash(self.id)

    async def dispatch(
        self, timeout: typing.Optional[float] = None
    ) -> typing.Tuple[typing.Any, typing.Any]:
        """
        Dispatch the task associated with this execution context.
        Args:
            timeout: Optional dispatch timeout
        Returns:
            SwitchRequest if task switching is requested, else None.
        Raises:
            RuntimeError: If called from within an existing event loop
            Exception: If execution fails
        """
        from .coordinator import ExecutionCoordinator

        coordinator = ExecutionCoordinator(execution_context=self, timeout=timeout)

        try:
            return await coordinator.execute_async()
        except Exception as e:
            logger.error(
                f"{self.pipeline.__class__.__name__} : {str(self.task_profiles)} : {str(e)}"
            )
            raise

    def get_task_profiles(self) -> typing.Deque["TaskType"]:
        task_profiles = self.task_profiles

        return typing.cast(typing.Deque["TaskType"], task_profiles)

    def is_multitask(self) -> bool:
        return len(self.get_task_profiles()) > 1

    def get_head_context(self) -> "ExecutionContext":
        """
        Returns the execution context head of the execution context.
        :return: ExecutionContext head of the execution context.
        """
        current = self
        while current.previous_context:
            current = current.previous_context
        return current

    def get_latest_context(self) -> "ExecutionContext":
        """
        Returns the latest execution context.
        :return: ExecutionContext
        """
        current = self.get_head_context()
        while current.next_context:
            current = current.next_context
        return current

    def get_tail_context(self) -> "ExecutionContext":
        """
        Returns the tail context of the execution context.
        :return: ExecutionContext
        """
        return self.get_latest_context()

    def filter_by_event(self, event_name: str) -> ResultSet:
        """
        Filters the execution context based on the event name.
        :param event_name: Case-insensitive event name.
        :return: ResultSet with the filtered execution context.
        """
        head = self.get_head_context()
        event = ""  # PipelineTask.resolve_event_name(event_name)
        result = ResultSet()

        def filter_condition(context: ExecutionContext, term: str) -> bool:
            task_profiles = context.task_profiles

        for context in head:
            if event in [task.event for task in context.task_profiles]:
                result.add(context)
        return result

    def get_decision_task_profile(
        self,
    ) -> typing.Optional[TaskType]:
        """
        Retrieves task profile for use in making decisions.

        This method examines the list of task profiles to identify the last task in
        a pipeline. If there is only one task profile, it returns that profile directly.
        For multiple task profiles, it iterates through each profile and checks the
        pointer type associated with the event.

        Specifically, it looks for a task profile whose pointer type indicates parallelism
        (PipeType.PARALLELISM) and ensures that its on-success pipe type is not parallelism.

        This helps in identifying the last task in a sequence when tasks are executed in parallel
        followed by a task that depends on the success of those parallel tasks.

        Returns:
            PipelineTask: The last task profile in the chain or the single task profile
                           if only one exists.
        """
        task_profiles = self.get_task_profiles()
        if len(task_profiles) == 1:
            return task_profiles[0]

        for task_profile in task_profiles:
            pointer_to_task = task_profile.get_pointer_to_task()
            if (
                pointer_to_task == PipeType.PARALLELISM
                and task_profile.condition_node.on_success_pipe != PipeType.PARALLELISM
            ):
                return task_profile
        return None

    def get_result_evaluator(self) -> typing.Optional["EventEvaluator"]:
        """
        Retrieves the result evaluation strategy from the task profile.

        This method first identifies the relevant task profile using the
        `get_decision_task_profile` method. If a task profile is found,
        it then accesses the associated condition node to retrieve the
        result evaluation strategy.

        Returns:
            EventEvaluator: The result evaluation strategy associated with the
            task profile, or None if no task profile is found.
        """
        task_profile = self.get_decision_task_profile()
        if task_profile:
            if task_profile.options and task_profile.options.is_configured(
                "result_evaluation_strategy"
            ):
                # resolve evaluation strategy from options
                try:
                    evaluator_strategy = typing.cast(
                        ResultEvaluationStrategy,
                        task_profile.options.result_evaluation_strategy,
                    )
                    strategy = getattr(
                        ResultEvaluationStrategies,
                        evaluator_strategy.name,
                    )
                except AttributeError as e:
                    logger.warning(
                        f"Error resolving result evaluation strategy from options: {e}"
                    )
                    strategy = None

                if strategy:
                    return EventEvaluator(strategy=strategy)

            return task_profile.get_event_class().evaluator()
        return None

    def cleanup(self) -> None:
        """Clean up shared memory resources"""
        self.get_state_manager().release_state(self.state_id)

    def set_engine(self, engine: "WorkflowEngine") -> None:
        """Associate this context with its execution engine"""
        self._engine_ref = weakref.ref(engine)

    def get_engine(self) -> typing.Optional["WorkflowEngine"]:
        """Get the associated engine if still alive"""
        if self._engine_ref:
            return self._engine_ref()
        return None

    async def create_snapshot(self) -> "ContextSnapshot":
        """
        Create a serializable snapshot of the current state.
        This is the core method for persistence.

        This should be called by the engine at strategic checkpoints:
        - Before executing each task
        - After task completion
        - On status changes
        """
        from .state_manager import ExecutionStatus
        from volnux.execution.rehydrator.snapshot import TraversalSnapshot, ContextSnapshot
        from volnux.execution.rehydrator.serializer import StateSerializer

        state = await self.state_async

        # Get the engine to capture its queue state
        engine = self.get_engine()
        sink_nodes = []
        tasks_processed = 0
        current_task_info = (None, None, None)

        if engine:
            # Capture the current task being processed
            # The engine should expose this via a getter
            current_task_info = self._extract_current_task_from_engine(engine)

            # Capture sink queue
            if hasattr(engine, "sink_queue"):
                sink_nodes = [
                    StateSerializer.serialize_task(task) for task in engine.sink_queue
                ]

            # Get task counter
            tasks_processed = engine.tasks_processed

        current_task_id, current_event_name, _ = current_task_info

        # Serialize task queue
        traversal = TraversalSnapshot(
            current_task_id=current_task_id,
            current_task_event_name=current_event_name,
            current_task_checkpoint=self._task_checkpoint,
            queue_snapshot=[
                StateSerializer.serialize_task(task) for task in self.task_profiles
            ],
            queue_index=0,
            total_queue_size=len(self.task_profiles),
            sink_nodes=sink_nodes,
            tasks_processed=tasks_processed,
            is_multitask_context=self.is_multitask(),
        )

        # Get pipeline reference
        pipeline_id, pipeline_class_path = StateSerializer.serialize_pipeline_ref(
            self.pipeline
        )

        # Serialize errors and results
        errors = [StateSerializer.serialize_exception(e) for e in state.errors]
        results = [StateSerializer.serialize_result(r) for r in state.results]

        snapshot = ContextSnapshot(
            state_id=self.state_id,
            workflow_id=self.workflow_id or f"workflow_{self.get_root_context().id}",
            parent_id=self.parent_context.state_id if self.parent_context else None,
            child_ids=[child.state_id for child in self.child_contexts],
            depth=self.get_depth(),
            previous_context_id=(
                self.previous_context.state_id if self.previous_context else None
            ),
            next_context_id=(self.next_context.state_id if self.next_context else None),
            traversal=traversal,
            pipeline_id=pipeline_id,
            pipeline_class_path=pipeline_class_path,
            status=state.status.value,
            errors=errors,
            results=results,
            metrics={
                "start_time": self.metrics.start_time,
                "end_time": self.metrics.end_time,
                "duration": self.metrics.duration,
            },
            snapshot_timestamp=datetime.datetime.now().timestamp(),
        )

        return snapshot

    def _extract_current_task_from_engine(
        self, engine: "WorkflowEngine"
    ) -> typing.Tuple[
        typing.Optional[str], typing.Optional[str], typing.Optional[dict]
    ]:
        """
        Extract the current task being executed from the engine.

        Returns:
            Tuple of (task_id, event_name, checkpoint_data)
        """
        # The engine's queue structure is: deque[TaskNode]
        # We need to peek at what's currently being processed

        # If engine tracks the current task explicitly
        node = engine.current_task_node
        if node and node.task:
                return (
                    getattr(node.task, "id", None),
                    node.task.event,
                    self._task_checkpoint,
                )

        # Peek at the front of the queue
        # if hasattr(engine, "queue") and engine.queue:
        #     node = engine.queue[0]  # Peek without removing
        #     if node and node.task:
        #         return (
        #             getattr(node.task, "id", None),
        #             node.task.event,
        #             self._task_checkpoint,
        #         )

        return None, None, None

    async def persist(self) -> None:
        """Persist current state"""
        snapshot = await self.create_snapshot()
        await snapshot.save_async()
        logger.debug(f"Persisted context {self.state_id}")

    def set_task_checkpoint(self, checkpoint_data: dict) -> None:
        """
        Set checkpoint data for the current task (idempotency support).

        Args:
            checkpoint_data: Arbitrary data marking progress within a task
        """
        self._task_checkpoint = checkpoint_data

    def __del__(self) -> None:
        """Ensure cleanup on garbage collection"""
        try:
            if hasattr(self, "state_id") and self.state_id:
                if self._state_manager:
                    self._state_manager.release_state(self.state_id)
        except:
            pass  # Ignore errors during cleanup
