"""Lazy rehydrator for workflow execution trees.

Rehydrates a workflow tree lazily, one node at a time, using LazyContextProxy
placeholders for neighbours. Only the branches that execution actually walks
into ever get materialised — avoids loading the entire fractal tree up front.
"""

import typing
import logging
import threading
import weakref

from volnux.exceptions import ObjectDoesNotExist

from .deserializer import StateDeserializer
from .snapshot import (
    ContextSnapshot,
    TraversalSnapshot,
    TaskSnapshot,
    QueueTaskTemplate,
)
from volnux.import_utils import import_string as import_class

if typing.TYPE_CHECKING:
    from volnux.pipeline import Pipeline
    from volnux.parser.protocols import TaskType
    from volnux.engine.base import WorkflowEngine, TaskNode
    from volnux.execution.context import ExecutionContext


logger = logging.getLogger(__name__)


class LazyContextProxy:
    """A transparent virtual proxy standing in for a not-yet-rehydrated context.

    A proxy is assigned to the ``previous_context``, ``parent_context`` and each
    entry of ``child_contexts`` of the node that was eagerly rehydrated. The real
    :class:`ExecutionContext` it represents is only materialised the first time an
    attribute is accessed on the proxy (lazy/on-demand rehydration), at which point
    it grafts itself into the live tree and forwards everything to the real object.

    The proxy keeps just enough metadata (the ``state_id`` and a back-reference to
    the rehydrator) to fetch and reconstruct its target on demand.
    """

    # Attributes that live on the proxy itself and must NOT be forwarded.
    __slots__ = ("_state_id", "_rehydrator", "_resolved", "_lock", "__weakref__")

    def __init__(self, state_id: str, rehydrator: "LazyRehydrator"):
        object.__setattr__(self, "_state_id", state_id)
        object.__setattr__(self, "_rehydrator", rehydrator)
        object.__setattr__(self, "_resolved", None)
        object.__setattr__(self, "_lock", threading.Lock())

    @property
    def state_id(self) -> str:
        """Available without triggering rehydration (used for re-linking)."""
        return object.__getattribute__(self, "_state_id")

    def is_resolved(self) -> bool:
        return object.__getattribute__(self, "_resolved") is not None

    def _resolve(self) -> "ExecutionContext":
        """Materialise (rehydrate) the real context, caching the result."""
        resolved = object.__getattribute__(self, "_resolved")
        if resolved is not None:
            return resolved

        lock = object.__getattribute__(self, "_lock")
        with lock:
            resolved = object.__getattribute__(self, "_resolved")
            if resolved is not None:
                return resolved

            rehydrator = object.__getattribute__(self, "_rehydrator")
            state_id = object.__getattribute__(self, "_state_id")

            logger.debug("Lazily rehydrating context %s", state_id)
            resolved = rehydrator.rehydrate_node(state_id)
            object.__setattr__(self, "_resolved", resolved)
            return resolved

    def __getattr__(self, name: str) -> typing.Any:
        # __getattr__ is only called when normal lookup fails, so this never
        # shadows the real proxy attributes/methods defined above.
        return getattr(self._resolve(), name)

    def __setattr__(self, name: str, value: typing.Any) -> None:
        if name in self.__slots__:
            object.__setattr__(self, name, value)
            return
        setattr(self._resolve(), name, value)

    def __iter__(self):
        return iter(self._resolve())

    def __hash__(self) -> int:
        return hash(self.state_id)

    def __eq__(self, other: typing.Any) -> bool:
        other_id = getattr(other, "state_id", None)
        return other_id is not None and other_id == self.state_id

    def __repr__(self) -> str:
        status = "resolved" if self.is_resolved() else "lazy"
        return f"<LazyContextProxy {self.state_id} [{status}]>"


class LazyRehydrator:
    """Rehydrates a workflow tree lazily, one node at a time.

    Strategy:
      1. Fully rehydrate the *current* node (the one we want to resume) and
         kick-start execution from it.
      2. Replace its ``previous_context``, ``parent_context`` and
         ``child_contexts`` with :class:`LazyContextProxy` placeholders.
      3. Each placeholder rehydrates its referenced context only when first
         accessed, recursively applying the same lazy grafting.

    This avoids loading the entire (potentially huge) fractal tree up front; only
    the branches that execution actually walks into ever get materialised.
    """

    def __init__(
        self,
        deserializer: StateDeserializer = StateDeserializer,
        pipeline_registry: typing.Optional[dict] = None,
    ):
        self.deserializer = deserializer
        # Instance-level registry — avoids cross-instance mutation.
        self.pipeline_registry: typing.Dict[str, typing.Any] = dict(
            pipeline_registry or {}
        )
        # Shared cache so a context fetched via two different links resolves once.
        self._cache: weakref.WeakValueDictionary[str, "ExecutionContext"] = (
            weakref.WeakValueDictionary()
        )

    async def resume_from(
        self,
        state_id: str,
        engine_class: typing.Optional[typing.Type["WorkflowEngine"]] = None,
    ) -> typing.Tuple["ExecutionContext", typing.Optional["WorkflowEngine"]]:
        """Rehydrate ``state_id`` eagerly and lazily wire its neighbours.

        Returns the live current context (ready to execute) plus an engine
        rebound to it, if an ``engine_class`` is provided.
        """
        context = await self.rehydrate_node(state_id)

        engine: typing.Optional["WorkflowEngine"] = None
        if engine_class is not None:
            snapshot = await self._load_snapshot(state_id)
            engine = await self._rehydrate_engine(engine_class, snapshot, context)
            context.set_engine(engine)

        return context, engine

    async def rehydrate_node(self, state_id: str) -> "ExecutionContext":
        """Fully rehydrate a single context and attach lazy proxies for neighbours.

        Cached: re-requesting the same ``state_id`` returns the same instance, so
        a proxy resolving back into an already-live node re-uses it.
        """
        cached = self._cache.get(state_id)
        if cached is not None:
            return cached

        snapshot = await self._load_snapshot(state_id)
        context = await self._build_context(snapshot)
        # Register before wiring neighbours to break potential cycles.
        self._cache[state_id] = context

        self._attach_lazy_links(context, snapshot)
        return context

    async def _load_snapshot(self, state_id: str) -> ContextSnapshot:
        """Fetch a ContextSnapshot from the store and ensure it's an object."""
        try:
            raw = await ContextSnapshot.get_async(state_id)
        except ObjectDoesNotExist:
            raise RuntimeError(
                f"LazyRehydrator: no snapshot found for state_id='{state_id}'. "
                f"The checkpoint may have expired, been evicted, or the state_id "
                f"is invalid. Check the persistence backend and TTL configuration."
            )
        if isinstance(raw, ContextSnapshot):
            return raw
        # Backend returned a raw dict — delegate reconstruction to the deserializer.
        return self.deserializer.deserialize_context(raw)

    async def _build_context(self, snapshot: ContextSnapshot) -> "ExecutionContext":
        from collections import deque
        from volnux.execution.context import ExecutionContext

        pipeline = self._reconstruct_pipeline(
            snapshot.pipeline_id, snapshot.pipeline_class_path
        )

        task_profiles = deque(
            await self._rebuild_tasks(snapshot.traversal.task_queue_snapshot)
        )

        context = ExecutionContext(task_profiles=task_profiles, pipeline=pipeline)
        context.change_object_id(snapshot.state_id)
        context.workflow_id = snapshot.workflow_id

        await self._restore_execution_state(context, snapshot)

        if snapshot.traversal.current_task_checkpoint:
            context.set_task_checkpoint(snapshot.traversal.current_task_checkpoint)

        return context

    def _reconstruct_pipeline(
        self,
        pipeline_id: str,
        class_path: str,
        pipeline_kwargs: typing.Optional[typing.Dict] = None,
    ) -> "Pipeline":
        if pipeline_id in self.pipeline_registry:
            return self.pipeline_registry[pipeline_id]
        pipeline_class = import_class(class_path)
        pipeline = pipeline_class(**(pipeline_kwargs or {}))
        self.pipeline_registry[pipeline_id] = pipeline
        return pipeline

    async def _rebuild_tasks(
        self, queue_snapshot: typing.List[QueueTaskTemplate]
    ) -> typing.List["TaskType"]:
        tasks: typing.List["TaskType"] = []
        for queue_task in queue_snapshot:
            task = await self._rebuild_task(queue_task)
            if task is not None:
                tasks.append(task)
        return tasks

    async def _rebuild_task(
        self, queue_task: QueueTaskTemplate
    ) -> typing.Optional["TaskType"]:
        from volnux.task import PipelineTask

        task_id = queue_task.get("task_id")
        if not task_id:
            return None

        try:
            task_snapshot = await TaskSnapshot.get_async(task_id)
        except Exception as e:
            logger.warning("Failed to load TaskSnapshot '%s': %s", task_id, e)
            return None

        if not isinstance(task_snapshot, TaskSnapshot):
            task_snapshot = self.deserializer.deserialize_task(task_snapshot)

        event_class = import_class(task_snapshot.event_class_import_path)
        return PipelineTask(event=event_class)

    async def _restore_execution_state(
        self, context: "ExecutionContext", snapshot: ContextSnapshot
    ) -> None:
        from volnux.execution.status import ExecutionStatus
        from volnux.result import ResultSet
        from ..event.event_result_serializer import EXEC_RESULT_SERIALIZER

        results = ResultSet(
            [
                await EXEC_RESULT_SERIALIZER.deserialize_exec_result(r)
                for r in snapshot.results
            ]
        )

        context.status = ExecutionStatus(snapshot.status)
        context.errors = []
        context.results = results
        context.aggregated_result = getattr(snapshot, "aggregated_result", None)
        # Establish a live KV record under the (already identity-swapped)
        # context id, now that construction itself no longer touches the
        # backend.
        await context.save_async()

        metrics = snapshot.metrics or {}
        if "start_time" in metrics:
            context.metrics.start_time = metrics["start_time"]
        if "end_time" in metrics:
            context.metrics.end_time = metrics["end_time"]

    def _attach_lazy_links(
        self, context: "ExecutionContext", snapshot: ContextSnapshot
    ) -> None:
        """Wire neighbours as proxies (or live nodes if already cached)."""
        # Horizontal: previous (next is left for forward traversal to re-link)
        if snapshot.previous_context_id:
            context.previous_context = self._link_for(snapshot.previous_context_id)
        if snapshot.next_context_id:
            context.next_context = self._link_for(snapshot.next_context_id)

        # Vertical: parent + children
        if snapshot.parent_id:
            context.parent_context = self._link_for(snapshot.parent_id)

        context.child_contexts = [
            self._link_for(child_id) for child_id in (snapshot.child_ids or [])
        ]

    def _link_for(self, state_id: str):
        """Return a live context if already materialised, else a lazy proxy."""
        cached = self._cache.get(state_id)
        if cached is not None:
            return cached
        return LazyContextProxy(state_id=state_id, rehydrator=self)

    async def _rehydrate_engine(
        self,
        engine_class: typing.Type["WorkflowEngine"],
        snapshot: ContextSnapshot,
        context: "ExecutionContext",
    ) -> "WorkflowEngine":
        engine = engine_class(enable_checkpointing=True)
        traversal = snapshot.traversal
        engine.tasks_processed = traversal.tasks_processed
        engine.current_task_node = await self._rebuild_current_task_node(
            traversal, context
        )
        return engine

    async def _rebuild_current_task_node(
        self, traversal: TraversalSnapshot, context: "ExecutionContext"
    ) -> typing.Optional["TaskNode"]:
        from volnux.engine.base import TaskNode

        if not traversal.current_task:
            return None
        task = await self._rebuild_task(traversal.current_task)
        if task is None:
            return None
        return TaskNode(task=task, previous_context=context)
