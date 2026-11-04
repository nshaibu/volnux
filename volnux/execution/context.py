import asyncio
import weakref
import logging
import time
import typing
from collections import deque
from dataclasses import dataclass, field

from formax import (
    Attrib,
    BaseModel,
    MiniAnnotated,
    ValidationFlags,
    InitStrategy,
    ValidationError as FormaxValidationError,
)

from volnux.mixins import KeyValueStoreIntegrationMixin
from volnux.exceptions import StopProcessingError, SwitchTask, SuspendTask
from volnux.execution.status import ExecutionStatus
from volnux.parser.operator import PipeType
from volnux.parser.options import ResultEvaluationStrategy
from volnux.parser.protocols import TaskType
from volnux.execution.pipeline import Pipeline
from volnux.result import EventResult, ResultSet
from volnux.result_evaluators import EventEvaluator, ResultEvaluationStrategies
from volnux.signal.signals import (
    SoftSignal,
    event_execution_aborted,
    event_execution_cancelled,
    event_execution_failed,
    event_execution_paused,
    event_execution_suspended,
)
from volnux.task import PipelineTask, PipelineTaskGrouping
from volnux.context import get_current_node_id, get_current_project_id
from volnux.backends.fields import (
    FKConfig,
    ForeignKeyField,
    ListConfig,
    ListField,
    ObjectConfig,
    ObjectField,
    OnDelete,
)
from volnux.execution.codecs import (
    deserialize_errors,
    deserialize_pipeline,
    deserialize_tasks,
    serialize_errors,
    serialize_pipeline,
    serialize_tasks,
)

if typing.TYPE_CHECKING:
    from volnux.engine.base import WorkflowEngine
    from volnux.execution.rehydrator.checkpoint_manager import VolnuxCheckPointManager

logger = logging.getLogger(__name__)

# Statuses after which a context will not run again. A parent group is
# re-evaluated whenever one of its children reaches one of these.
TERMINAL_STATUSES = frozenset(
    {
        ExecutionStatus.COMPLETED,
        ExecutionStatus.FAILED,
        ExecutionStatus.CANCELLED,
        ExecutionStatus.ABORTED,
    }
)

# Child statuses that count as "finished" for group finality.
_GROUP_DONE_STATUSES = frozenset({ExecutionStatus.COMPLETED, ExecutionStatus.FAILED})


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
    """Normalise a single task, list/tuple or deque of tasks into a deque."""
    if isinstance(task_profiles, deque):
        return task_profiles
    if isinstance(task_profiles, (list, tuple)):
        return deque(task_profiles)
    if isinstance(task_profiles, (PipelineTask, PipelineTaskGrouping)):
        return deque([task_profiles])  # type: ignore
    raise FormaxValidationError(  # type: ignore
        "task_profiles must be a PipelineTask, PipelineTaskGrouping, or a "
        f"list/tuple/deque of them, got {type(task_profiles).__name__}"
    )


class ExecutionContext(KeyValueStoreIntegrationMixin, BaseModel):
    """
    Represents the execution context for a particular event in the pipeline.

    This class encapsulates the necessary data and state associated with
    executing an event, such as the task being processed and the pipeline
    it belongs to.

    :ivar task_profiles: The specific tasks being executed within the pipeline.
    :ivar pipeline: The pipeline that orchestrates the execution of the task.
    :ivar metrics: Execution metrics for monitoring and evaluating performance.
    :ivar previous_context: The preceding context in a doubly-linked list structure.
    :ivar next_context: The succeeding context in a doubly-linked list structure.
    :ivar parent_context: The parent context in a tree structure, used for hierarchical task management.
    :ivar child_contexts: The child contexts in a tree structure, representing branches of execution.

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

        Contexts compare and hash by identity (``id``), never by field values.
    """

    task_profiles: ObjectField[
        typing.Deque[TaskType],
        ObjectConfig(
            coerce=preformat_task_profile,
            serializer=serialize_tasks,
            deserializer=deserialize_tasks,
        ),
    ]
    pipeline: ObjectField[
        Pipeline,
        ObjectConfig(serializer=serialize_pipeline, deserializer=deserialize_pipeline),
    ]
    metrics: ObjectField[
        ExecutionMetrics, ObjectConfig(default_factory=ExecutionMetrics)
    ]

    # project identity
    node_id: MiniAnnotated[str, Attrib(default_factory=lambda: get_current_node_id())]
    project_id: MiniAnnotated[
        str, Attrib(default_factory=lambda: get_current_project_id())
    ]

    # Horizontal Links (Linked list)
    previous_context: ForeignKeyField[
        typing.Optional["ExecutionContext"], FKConfig(on_delete=OnDelete.SET_NULL)
    ]
    next_context: ForeignKeyField[
        typing.Optional["ExecutionContext"], FKConfig(on_delete=OnDelete.SET_NULL)
    ]

    # Vertical Links (The Tree)
    parent_context: ForeignKeyField[
        typing.Optional["ExecutionContext"], FKConfig(on_delete=OnDelete.SET_NULL)
    ]
    child_contexts: ListField["ExecutionContext", ListConfig(unique_items=True)]

    # Workflow identifier for grouping contexts
    workflow_id: MiniAnnotated[typing.Optional[str], Attrib(default=None)]
    workflow_name: MiniAnnotated[typing.Optional[str], Attrib(default=None)]

    # Hot execution state. Persisted with the context itself through
    # KeyValueStoreIntegrationMixin so it can be shared across workers/machines.
    status: ObjectField[ExecutionStatus, ObjectConfig(default=ExecutionStatus.PENDING)]
    errors: ObjectField[
        typing.List[Exception],
        ObjectConfig(
            default_factory=list,
            serializer=serialize_errors,
            deserializer=deserialize_errors,
        ),
    ]
    # Results are their own records; the context holds references to them.
    results: ListField[EventResult, ListConfig(unique_items=True)]

    # Usually used by Reduce meta-event
    aggregated_result: ForeignKeyField[
        typing.Optional[EventResult], FKConfig(on_delete=OnDelete.SET_NULL)
    ]

    # Checkpoint data for idempotency
    task_checkpoint: ObjectField[typing.Optional[typing.Dict[str, typing.Any]]]

    # Opt-in: when True, mutators persist to the configured backend after
    # every change. Default False keeps the common one-context-per-task case
    # off the backend hot path; call sites that need cross-process visibility
    # (or explicit persist()/checkpointing) can still save on demand.
    persist_state: bool = False

    # Runtime-only (leading underscore: never persisted)
    _context_lock: asyncio.Lock = field(
        default_factory=lambda: asyncio.Lock()
    )  # Protects concurrent append ops
    _engine_ref: typing.Optional[weakref.ReferenceType] = None
    # Strong refs to fire-and-forget saves scheduled by sync mutators
    _pending_saves: typing.Set[asyncio.Task] = field(default_factory=set)

    class Config:
        validation = ValidationFlags.COERCE
        init_strategy = InitStrategy.DATACLASS

    # Identity

    def __eq__(self, other: object) -> bool:
        # Field-wise dataclass equality would compare the parent/child graph
        # (and locks); two contexts are the same only if they are the same node.
        if not isinstance(other, ExecutionContext):
            return NotImplemented
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    @property
    def state_id(self) -> str:
        return self.id

    # Persistence

    async def save(
        self, force_insert: bool = False, ttl: typing.Optional[int] = None
    ) -> None:
        # Results are separate records that the context only references.
        await asyncio.gather(
            *(
                result.save()
                for result in (*self.results, self.aggregated_result)
                if hasattr(result, "save")
            )
        )
        await super().save(force_insert=force_insert, ttl=ttl)

    def _maybe_persist(self) -> None:
        """Persist from sync code paths (no-op unless ``persist_state``).

        ``save()`` is async: inside a running loop it is scheduled as a task,
        otherwise it runs to completion before returning.
        """
        if not self.persist_state:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.save())
            return
        task = loop.create_task(self.save())
        self._pending_saves.add(task)
        task.add_done_callback(self._on_save_done)

    def _on_save_done(self, task: "asyncio.Task") -> None:
        self._pending_saves.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error(
                "Background save of context %s failed: %s",
                self.id,
                task.exception(),
            )

    async def _maybe_persist_async(self) -> None:
        if self.persist_state:
            await self.save()

    def set_task_checkpoint(self, checkpoint_data: dict) -> None:
        """
        Set checkpoint data for the current task (idempotency support).

        Args:
            checkpoint_data: Arbitrary data marking progress within a task
        """
        self.task_checkpoint = checkpoint_data

    def get_task_checkpoint(self) -> typing.Optional[typing.Dict[str, typing.Any]]:
        """Checkpoint data recorded for the current task, if any."""
        return self.task_checkpoint

    # Control-flow requests raised by events

    def _find_error(
        self, error_type: typing.Type[Exception]
    ) -> typing.Optional[Exception]:
        # Exact type match: these sentinels are distinct control-flow signals
        for err in self.errors:
            if type(err) is error_type:
                return err
        return None

    def get_stop_processing_request(self) -> typing.Optional[Exception]:
        """Check for StopProcessingError in errors"""
        return self._find_error(StopProcessingError)

    def get_switch_request(self) -> typing.Optional[Exception]:
        """Check for SwitchTask in errors"""
        return self._find_error(SwitchTask)

    def get_suspension_request(self) -> typing.Optional[Exception]:
        """Check for a SuspendTask in errors.

        Subclasses count (``ExternalCommunicationSuspensionRequest`` is the usual
        one); the other sentinels are matched exactly.
        """
        for err in self.errors:
            if isinstance(err, SuspendTask):
                return err
        return None

    # Construction and tree

    @classmethod
    async def create_context(
        cls,
        workflow_id: typing.Optional[str],
        workflow_name: typing.Optional[str],
        task_profiles: typing.Union[TaskType, typing.Iterable[TaskType]],
        pipeline: Pipeline,
        metrics: typing.Optional[ExecutionMetrics] = None,
        previous_context: typing.Optional["ExecutionContext"] = None,
        next_context: typing.Optional["ExecutionContext"] = None,
        parent_context: typing.Optional["ExecutionContext"] = None,
        child_contexts: typing.Optional[typing.Iterable["ExecutionContext"]] = None,
        persist_state: bool = False,
    ) -> "ExecutionContext":
        """
        Creates an ExecutionContext instance.

        :param workflow_id: Unique identifier of the workflow.
        :param workflow_name: Name of the workflow.
        :param task_profiles: A single task, or a list/tuple/deque of tasks to be executed
            as part of this context.
        :param pipeline: Pipeline object managing the execution flow and dependencies.
        :param metrics: (Optional) Metrics object; a fresh one (started now) is created if omitted.
        :param previous_context: (Optional) Previous ExecutionContext in the workflow chain.
        :param next_context: (Optional) Next ExecutionContext in the workflow chain.
        :param parent_context: (Optional) The immediate parent ExecutionContext, if nested.
        :param child_contexts: (Optional) Child ExecutionContexts derived from this context.
        :param persist_state: (Optional) Whether mutators should persist hot state to the
            configured backend after every change. Defaults to False.
        :return: The new ExecutionContext.
        """
        return cls(
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            task_profiles=task_profiles,
            pipeline=pipeline,
            metrics=metrics if metrics is not None else ExecutionMetrics(),
            previous_context=previous_context,
            next_context=next_context,
            parent_context=parent_context,
            child_contexts=list(child_contexts) if child_contexts else [],
            persist_state=persist_state,
        )

    async def spawn_child(
        self, task_profiles: typing.Union[TaskType, typing.Iterable[TaskType]]
    ) -> "ExecutionContext":
        """
        Creates a child ExecutionContext linked under this context (shares this
        context's pipeline, workflow identity and persistence mode).

        :param task_profiles: A single task or a collection of tasks for the child.
        :return: The newly created child context.
        """
        child = await type(self).create_context(
            task_profiles=task_profiles,
            pipeline=self.pipeline,
            parent_context=self,
            workflow_id=self.workflow_id,
            workflow_name=self.workflow_name,
            persist_state=self.persist_state,
        )

        async with self._context_lock:
            self.child_contexts.append(child)  # Link Down
        return child

    @property
    def is_root(self) -> bool:
        return self.parent_context is None

    @property
    def is_leaf(self) -> bool:
        return len(self.child_contexts) == 0

    def _walk(self, attr: str) -> typing.Iterator["ExecutionContext"]:
        """Follow ``attr`` links from (and excluding) self, stopping on cycles."""
        seen = {id(self)}
        current = getattr(self, attr)
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            yield current
            current = getattr(current, attr)

    def get_root_context(self) -> "ExecutionContext":
        """Climb the tree to find the absolute start of the orchestration."""
        root = self
        for root in self._walk("parent_context"):
            pass
        return root

    def get_depth(self) -> int:
        """Calculates nesting level for directive validation."""
        return sum(1 for _ in self._walk("parent_context"))

    def get_head_context(self) -> "ExecutionContext":
        """
        Returns the execution context head of the execution context.
        :return: ExecutionContext head of the execution context.
        """
        head = self
        for head in self._walk("previous_context"):
            pass
        return head

    def get_latest_context(self) -> "ExecutionContext":
        """
        Returns the latest (tail) execution context.
        :return: ExecutionContext
        """
        tail = self
        for tail in self._walk("next_context"):
            pass
        return tail

    def execution_failed(self) -> bool:
        """True if this context ended without completing: failed, aborted or
        cancelled."""
        return self.status in (
            ExecutionStatus.FAILED,
            ExecutionStatus.ABORTED,
            ExecutionStatus.CANCELLED,
        )

    def get_tail_context(self) -> "ExecutionContext":
        """
        Returns the tail context of the execution context.
        :return: ExecutionContext
        """
        return self.get_latest_context()

    def __iter__(self) -> typing.Iterator["ExecutionContext"]:
        yield self
        yield from self._walk("next_context")

    def filter_by_event(self, event_name: str) -> typing.List["ExecutionContext"]:
        """
        Returns every context in this chain (head to tail) that runs a task for
        the given event.
        :param event_name: Case-insensitive event name.
        :return: The matching contexts, in chain order.
        """
        wanted = event_name.lower()

        def runs_event(context: "ExecutionContext") -> bool:
            for task in context.get_task_profiles():
                get_name = getattr(task, "get_event_name", None)
                if callable(get_name) and str(get_name()).lower() == wanted:
                    return True
            return False

        return [context for context in self.get_head_context() if runs_event(context)]

    # Status transitions

    async def _evaluate_group_finality(self) -> None:
        """
        Internal check: Is every child context in this subtree finished?

        Reads child_contexts under _context_lock, the same lock spawn_child()
        appends under — otherwise a concurrently-spawning sibling whose child
        hasn't been registered yet is invisible to this snapshot, and the
        group can be marked complete before that child ever ran.
        """
        if self.status in TERMINAL_STATUSES:
            return  # Never overwrite an outcome this context already reached

        async with self._context_lock:
            all_done = all(
                child.status in _GROUP_DONE_STATUSES for child in self.child_contexts
            )

        if all_done:
            # The 'Super-Task' is now officially complete
            await self.update_status_async(ExecutionStatus.COMPLETED)

    async def _notify_parent(self) -> None:
        if self.status in TERMINAL_STATUSES and self.parent_context is not None:
            await self.parent_context._evaluate_group_finality()

    def update_status(self, new_status: "ExecutionStatus") -> None:
        """Sync status update. Does not re-evaluate the parent group; use
        ``update_status_async`` from async code."""
        self.status = new_status
        self._maybe_persist()

    async def update_status_async(self, new_status: "ExecutionStatus") -> None:
        self.status = new_status
        await self._maybe_persist_async()
        # If this child is done, signal the parent to check its 'Group' status
        await self._notify_parent()

    def add_error(self, error: Exception) -> None:
        self.errors.append(error)
        self._maybe_persist()

    async def add_error_async(self, error: Exception) -> None:
        self.errors.append(error)
        await self._maybe_persist_async()

    def get_results(self) -> "ResultSet[EventResult]":
        """The results of this context as a ``ResultSet``."""
        return ResultSet[EventResult](list(self.results))

    def _add_results(self, results: typing.Iterable[EventResult]) -> None:
        # results are a set: adding one that is already present is a no-op
        known = {getattr(item, "id", item) for item in self.results}
        for result in results:
            if getattr(result, "id", result) not in known:
                self.results.append(result)
                known.add(getattr(result, "id", result))

    def add_result(self, result: EventResult) -> None:
        self._add_results([result])
        self._maybe_persist()

    async def add_result_async(self, result: EventResult) -> None:
        self._add_results([result])
        await self._maybe_persist_async()

    def _apply_bulk(
        self,
        status: typing.Optional["ExecutionStatus"],
        errors: typing.Optional[typing.Sequence[Exception]],
        results: typing.Optional[typing.Sequence[EventResult]],
    ) -> None:
        if status is not None:
            self.status = status
        if errors is not None:
            self.errors.extend(errors)
        if results is not None:
            self._add_results(results)

    def bulk_update(
        self,
        status: typing.Optional["ExecutionStatus"] = None,
        errors: typing.Optional[typing.Sequence[Exception]] = None,
        results: typing.Optional[typing.Sequence[EventResult]] = None,
    ) -> None:
        """Efficient bulk update (single persist)"""
        self._apply_bulk(status, errors, results)
        self._maybe_persist()

    async def bulk_update_async(
        self,
        status: typing.Optional["ExecutionStatus"] = None,
        errors: typing.Optional[typing.Sequence[Exception]] = None,
        results: typing.Optional[typing.Sequence[EventResult]] = None,
    ) -> None:
        """Async version of efficient bulk update (single persist)"""
        self._apply_bulk(status, errors, results)
        await self._maybe_persist_async()
        if status is not None:
            await self._notify_parent()

    async def update_aggregated_result(self, result: "EventResult") -> None:
        self.aggregated_result = result
        await self._maybe_persist_async()

    def _signal_kwargs(self, status: ExecutionStatus) -> typing.Dict[str, typing.Any]:
        return {
            "sender": self.__class__,
            "task_profiles": self.get_task_profiles().copy(),
            "execution_context": self,
            "state": status,
        }

    def _transition(self, status: ExecutionStatus, signal: SoftSignal) -> None:
        self.update_status(status)
        signal.emit(**self._signal_kwargs(status))

    async def _transition_async(
        self, status: ExecutionStatus, signal: SoftSignal
    ) -> None:
        await self.update_status_async(status)
        await signal.emit_async(**self._signal_kwargs(status))

    def cancel(self) -> None:
        """Cancel execution - only mutates THIS context."""
        self._transition(ExecutionStatus.CANCELLED, event_execution_cancelled)

    async def cancel_async(self, propagate: bool = True) -> None:
        """Async version of cancel - only mutates THIS context's status.

        With ``propagate`` (the default) the tasks that are running right now
        are told to cancel as well, through their control channel.
        """
        flow = getattr(self, "_active_flow", None)
        if propagate and flow is not None:
            await flow.cancel()
        await self._transition_async(
            ExecutionStatus.CANCELLED, event_execution_cancelled
        )

    # The flow executing this context right now is kept in the instance attribute
    # ``_active_flow``: set by the coordinator for the duration of a dispatch,
    # runtime-only. It has no class-level declaration on purpose: formax turns
    # every class attribute of a model into a persisted field.

    async def pause_tasks(self) -> int:
        """Ask the running tasks to hold before their next step.

        Unlike ``suspended_async`` this does not change the context status
        (SUSPENDED means "waiting on something external" and ends the run).
        Returns how many tasks received the command.
        """
        flow = getattr(self, "_active_flow", None)
        return await flow.pause() if flow is not None else 0

    async def resume_tasks(self) -> int:
        """Release tasks held by ``pause_tasks``."""
        flow = getattr(self, "_active_flow", None)
        return await flow.resume() if flow is not None else 0

    def abort(self) -> None:
        """Abort execution - only mutates THIS context."""
        self._transition(ExecutionStatus.ABORTED, event_execution_aborted)

    async def abort_async(self) -> None:
        """Async version of abort - only mutates THIS context."""
        await self._transition_async(ExecutionStatus.ABORTED, event_execution_aborted)

    def failed(self) -> None:
        """Mark the execution context as failed."""
        self._transition(ExecutionStatus.FAILED, event_execution_failed)

    async def failed_async(self) -> None:
        """Async version of marking the execution context as failed."""
        await self._transition_async(ExecutionStatus.FAILED, event_execution_failed)

    async def paused_async(self) -> None:
        """Mark the execution context as paused (preempted, to be re-queued)."""
        await self._transition_async(ExecutionStatus.PAUSED, event_execution_paused)

    async def suspended_async(self) -> None:
        """Mark the context suspended: waiting on something external, checkpointed
        and released. Distinct from PAUSED."""
        await self._transition_async(
            ExecutionStatus.SUSPENDED, event_execution_suspended
        )

    # Execution

    async def dispatch(
        self, timeout: typing.Optional[float] = None
    ) -> typing.Tuple[typing.Any, typing.Any]:
        """
        Dispatch the task associated with this execution context.
        Args:
            timeout: Optional dispatch timeout
        Returns:
            The coordinator's execution result.
        Raises:
            Exception: If execution fails
        """
        from .coordinator import ExecutionCoordinator

        coordinator = ExecutionCoordinator(execution_context=self, timeout=timeout)

        try:
            return await coordinator.execute_async()
        except Exception as e:
            logger.error(
                "%s : %s : %s",
                self.pipeline.__class__.__name__,
                self.task_profiles,
                e,
            )
            raise

    def get_task_profiles(self) -> typing.Deque["TaskType"]:
        return typing.cast(typing.Deque["TaskType"], self.task_profiles)

    def is_multitask(self) -> bool:
        return len(self.get_task_profiles()) > 1

    def get_decision_task_profile(
        self,
    ) -> typing.Optional[TaskType]:
        """
        Retrieves the task profile that decides how this context's result is evaluated.

        A single task profile is returned directly. For multiple task profiles
        (a parallel group), returns the last task of the parallel chain: the
        one entered via PipeType.PARALLELISM whose success pipe does not
        continue the parallelism.

        :return: The deciding task profile, or None if none qualifies.
        """
        task_profiles = self.get_task_profiles()
        if len(task_profiles) == 1:
            return task_profiles[0]

        for task_profile in task_profiles:
            if (
                task_profile.get_pointer_to_task() == PipeType.PARALLELISM
                and task_profile.condition_node.on_success_pipe != PipeType.PARALLELISM
            ):
                return task_profile
        return None

    def get_result_evaluator(self) -> typing.Optional["EventEvaluator"]:
        """
        Retrieves the result evaluation strategy for the deciding task profile.

        A ``result_evaluation_strategy`` configured on the task's options takes
        precedence; otherwise the event class's own evaluator is used.

        Returns:
            EventEvaluator, or None if no deciding task profile is found.
        """
        task_profile = self.get_decision_task_profile()
        if task_profile is None:
            return None

        options = task_profile.options
        if options and options.is_configured("result_evaluation_strategy"):
            try:
                evaluator_strategy = typing.cast(
                    ResultEvaluationStrategy, options.result_evaluation_strategy
                )
                strategy = getattr(ResultEvaluationStrategies, evaluator_strategy.name)
            except AttributeError as e:
                logger.warning(
                    "Error resolving result evaluation strategy from options: %s", e
                )
                strategy = None

            if strategy:
                return EventEvaluator(strategy=strategy)

        return task_profile.get_event_class().evaluator()

    # Engine association

    def set_engine(self, engine: "WorkflowEngine") -> None:
        """Associate this context with its execution engine"""
        self._engine_ref = weakref.ref(engine)

    def get_engine(self) -> typing.Optional["WorkflowEngine"]:
        """Get the associated engine if still alive"""
        if self._engine_ref:
            return self._engine_ref()
        return None

    def set_resumed_events(self, events: typing.Dict[str, typing.Any]) -> None:
        """Hand this context events rehydrated from a checkpoint, by task id.

        When the flow later needs the event for such a task it uses the rehydrated
        one (with its state, phase and injected results) instead of building a
        fresh event. Runtime-only: set on the instance, never persisted.
        """
        self._resumed_events = dict(events)

    def take_resumed_event(self, task_id: str) -> typing.Optional[typing.Any]:
        """The rehydrated event for ``task_id``, handed over once."""
        events = getattr(self, "_resumed_events", None)
        if not events:
            return None
        return events.pop(task_id, None)

    def get_checkpoint_manager(self) -> typing.Optional["VolnuxCheckPointManager"]:
        """The checkpoint manager of this run, owned by the root engine.

        ``None`` when the run has no engine or checkpointing is not enabled. Events
        created for this context receive it so their snapshots are persisted.
        """
        engine = self.get_engine()
        if engine is None:
            return None
        return engine.get_root_engine().checkpoint_manager
