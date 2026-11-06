"""Rehydrator for workflow execution trees.

An ``ExecutionContext`` is persisted as itself (see ``ExecutionContext.save``),
so rehydrating one is just loading it back: its fields restore themselves.
What cannot be stored as data is re-attached here: the pipeline (looked up by
id and class path) and the neighbouring contexts and results, which come back
as lazy ``ForeignKey`` references and are resolved only when execution walks
into them. The engine's own position lives in ``EngineStateSnapshot``.
"""

import typing
import logging
import weakref

from volnux.backends.fields.foreign_key import ForeignKey
from volnux.exceptions import ObjectDoesNotExist
from volnux.execution.codecs import PipelineReference
from volnux.import_utils import import_string as import_class

from .deserializer import StateDeserializer
from .snapshot import EngineStateSnapshot, TaskSnapshot, QueueTaskTemplate

if typing.TYPE_CHECKING:
    from volnux.pipeline import Pipeline
    from volnux.parser.protocols import TaskType
    from volnux.engine.base import WorkflowEngine, TaskNode
    from volnux.execution.context import ExecutionContext


logger = logging.getLogger(__name__)


class LazyRehydrator:
    """Rehydrates a workflow tree one node at a time.

    Strategy:
      1. Load the *current* node (the one to resume) and re-attach its pipeline.
      2. Its ``previous_context``, ``next_context``, ``parent_context`` and
         ``child_contexts`` stay lazy ``ForeignKey`` references.
      3. ``resolve_links`` replaces them with live contexts, each loaded at most
         once through a shared cache, when execution needs to walk there.

    This avoids loading the entire (potentially huge) fractal tree up front; only
    the branches that execution actually walks into ever get materialised.
    """

    LINKS = ("previous_context", "next_context", "parent_context")

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
        """Rehydrate ``state_id`` and, optionally, an engine bound to it.

        Returns the live current context (ready to execute) plus an engine
        rebound to it, if an ``engine_class`` is provided.
        """
        context = await self.rehydrate_node(state_id)

        engine: typing.Optional["WorkflowEngine"] = None
        if engine_class is not None:
            snapshot = await self._load_engine_state(state_id)
            engine = await self._rehydrate_engine(engine_class, snapshot, context)
            context.set_engine(engine)

        return context, engine

    async def rehydrate_node(self, state_id: str) -> "ExecutionContext":
        """Load a single context and re-attach its pipeline.

        Cached: re-requesting the same ``state_id`` returns the same instance, so
        a link resolving back into an already-live node re-uses it.
        """
        from volnux.execution.context import ExecutionContext

        cached = self._cache.get(state_id)
        if cached is not None:
            return cached

        try:
            context = await ExecutionContext.get(state_id)
        except ObjectDoesNotExist:
            raise RuntimeError(
                f"LazyRehydrator: no context found for state_id='{state_id}'. "
                f"The checkpoint may have expired, been evicted, or the state_id "
                f"is invalid. Check the persistence backend and TTL configuration."
            )

        self._attach_pipeline(context)
        self._cache[state_id] = context
        return context

    async def resolve_links(self, context: "ExecutionContext") -> "ExecutionContext":
        """Replace the lazy neighbours of ``context`` with live contexts."""
        for name in self.LINKS:
            reference = getattr(context, name)
            if isinstance(reference, ForeignKey):
                setattr(context, name, await self.rehydrate_node(reference.id))

        context.child_contexts = [
            (
                await self.rehydrate_node(child.id)
                if isinstance(child, ForeignKey)
                else child
            )
            for child in context.child_contexts
        ]
        return context

    async def resolve_results(self, context: "ExecutionContext") -> "ExecutionContext":
        """Load the result records ``context`` references."""
        context.results = [
            await result.resolve_async() if isinstance(result, ForeignKey) else result
            for result in context.results
        ]
        if isinstance(context.aggregated_result, ForeignKey):
            context.aggregated_result = await context.aggregated_result.resolve_async()
        return context

    def _attach_pipeline(self, context: "ExecutionContext") -> None:
        reference = context.pipeline
        if isinstance(reference, PipelineReference):
            context.pipeline = self._reconstruct_pipeline(
                reference.id, reference.class_path, reference.fields
            )

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
        # the same run keeps the same identity (events and governance correlate on it)
        pipeline.change_object_id(pipeline_id)
        self.pipeline_registry[pipeline_id] = pipeline
        return pipeline

    async def _load_engine_state(self, state_id: str) -> EngineStateSnapshot:
        """Fetch the EngineStateSnapshot of a context and ensure it's an object."""
        try:
            raw = await EngineStateSnapshot.get(state_id)
        except ObjectDoesNotExist:
            raise RuntimeError(
                f"LazyRehydrator: no engine state found for state_id='{state_id}'."
            )
        if isinstance(raw, EngineStateSnapshot):
            return raw
        return self.deserializer.deserialize_engine_state(raw)

    async def _rebuild_task(
        self, queue_task: QueueTaskTemplate
    ) -> typing.Optional["TaskType"]:
        """Rebuild a queued task, with its whole graph, from its ``TaskSnapshot``."""
        task_id = queue_task.get("task_id")
        if not task_id:
            return None

        try:
            task_snapshot = await TaskSnapshot.get(task_id)
        except Exception as e:
            logger.warning("Failed to load TaskSnapshot '%s': %s", task_id, e)
            return None

        if not isinstance(task_snapshot, TaskSnapshot):
            task_snapshot = self.deserializer.deserialize_task(task_snapshot)

        return self.deserializer.rebuild_task(task_snapshot.dump("dict"))

    async def _rebuild_node(
        self, queue_task: QueueTaskTemplate
    ) -> typing.Optional["TaskNode"]:
        """A queue entry back as a ``TaskNode``: the task and the context it follows."""
        from volnux.engine.base import TaskNode

        task = await self._rebuild_task(queue_task)
        if task is None:
            return None
        previous_id = queue_task.get("previous_context_id")
        previous = await self.rehydrate_node(previous_id) if previous_id else None
        return TaskNode(task=task, previous_context=previous)

    async def _rehydrate_engine(
        self,
        engine_class: typing.Type["WorkflowEngine"],
        snapshot: EngineStateSnapshot,
        context: "ExecutionContext",
    ) -> "WorkflowEngine":
        """An engine standing where the snapshotted one stopped.

        Its work queue holds what is left to do, the interrupted task first, and
        its sink queue the deferred sinks; ``engine.resume()`` runs from there.
        """
        engine = engine_class(enable_checkpointing=True)
        # The interrupted task was already counted when it was first taken from the
        # queue and will be counted again when it re-runs; count it once.
        engine.tasks_processed = max(
            snapshot.tasks_processed - (1 if snapshot.current_task else 0), 0
        )

        current = (
            await self._rebuild_node(snapshot.current_task)
            if snapshot.current_task
            else None
        )
        engine.current_task_node = current
        if current is not None:
            engine.task_queue.append(current)
        for queued in snapshot.task_queue_snapshot:
            node = await self._rebuild_node(queued)
            if node is not None:
                engine.task_queue.append(node)
        for sink in snapshot.sink_queue_snapshot:
            task = await self._rebuild_task(sink)
            if task is not None:
                engine.sink_queue.append(task)
        return engine
