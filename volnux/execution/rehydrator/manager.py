import asyncio
import json
import logging
import pickle
import typing
import weakref
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class RehydrationManager:
    """
    Orchestrates the recovery of the entire execution tree from persistent storage.

    This implements the "Recursive Grafting" strategy:
    1. Reconstruct contexts in depth-order (parents before children)
    2. Rebuild horizontal links (doubly-linked list)
    3. Rebuild vertical links (tree structure)
    4. Reconnect engines to contexts
    5. Resume execution from checkpoints
    """

    def __init__(
        self,
        state_store: PersistentStateStore,
        pipeline_registry: typing.Optional[dict] = None,
    ):
        self.state_store = state_store
        self.pipeline_registry = pipeline_registry or {}
        self._context_cache: typing.Dict[str, "ExecutionContext"] = {}

    async def resume_workflow(
        self, workflow_id: str, engine_class: typing.Optional[typing.Type] = None
    ) -> typing.Optional[
        typing.Tuple["ExecutionContext", "CheckpointedWorkflowEngine"]
    ]:
        """
        Main entry point: Reconstruct and resume a workflow.

        Args:
            workflow_id: The workflow to resume
            engine_class: Engine class to instantiate (defaults to CheckpointedWorkflowEngine)

        Returns:
            Tuple of (root_context, engine) ready to continue execution
        """
        logger.info(f"Starting workflow rehydration for {workflow_id}")

        # Step 1: Fetch all snapshots, ordered by depth
        snapshots = await self.state_store.get_active_snapshots(
            workflow_id, statuses=["RUNNING", "PENDING"]  # Only resume active workflows
        )

        if not snapshots:
            logger.warning(f"No active snapshots found for workflow {workflow_id}")
            return None

        logger.info(f"Found {len(snapshots)} contexts to rehydrate")

        # Step 2: Reconstruct all contexts
        root_context = None
        for snapshot in snapshots:
            context = await self._rebuild_context(snapshot)
            self._context_cache[context.state_id] = context

            if context.is_root:
                root_context = context

        if not root_context:
            logger.error("No root context found in snapshots")
            return None

        # Step 3: Rebuild horizontal links
        await self._rebuild_horizontal_links(snapshots)

        # Step 4: Rebuild vertical links
        await self._rebuild_vertical_links(snapshots)

        # Step 5: Create and configure the engine
        if engine_class is None:
            engine_class = CheckpointedWorkflowEngine

        engine = engine_class(
            enable_checkpointing=True,
            checkpoint_store=self.state_store,
            strict_mode=True,
        )

        # Step 6: Restore engine state from root context's snapshot
        root_snapshot = next(
            s for s in snapshots if s.state_id == root_context.state_id
        )

        # Rebuild the engine's work queue
        engine_queue = self._rebuild_engine_queue(root_snapshot, root_context)
        sink_queue = self._rebuild_sink_queue(root_snapshot)

        # Restore engine state
        engine.resume_from_snapshot(
            tasks_processed=root_snapshot.traversal.tasks_processed,
            current_queue=engine_queue,
            sink_queue=sink_queue,
        )

        # Attach engine to all contexts
        for context in self._context_cache.values():
            context.set_engine(engine)

        logger.info(f"Workflow {workflow_id} successfully rehydrated")
        return root_context, engine

    async def _rebuild_context(self, snapshot: ContextSnapshot) -> "ExecutionContext":
        """
        Reconstruct an ExecutionContext from a snapshot.

        Note: Links (horizontal/vertical) are set separately.
        """
        from volnux.execution_context import ExecutionContext

        # Reconstruct pipeline instance
        pipeline = self._reconstruct_pipeline(
            snapshot.pipeline_id, snapshot.pipeline_class_path
        )

        # Reconstruct task queue
        task_profiles = deque(
            [
                self._deserialize_task(task_data)
                for task_data in snapshot.traversal.queue_snapshot
            ]
        )

        # Create context with basic fields
        context = ExecutionContext(
            task_profiles=task_profiles,
            pipeline=pipeline,
        )

        # Override state_id to maintain continuity
        context._id = snapshot.state_id

        # Set workflow ID
        context.workflow_id = snapshot.workflow_id

        # Restore execution state
        await self._restore_execution_state(context, snapshot)

        # Restore checkpoint
        if snapshot.traversal.current_task_checkpoint:
            context.set_task_checkpoint(snapshot.traversal.current_task_checkpoint)

        return context

    def _reconstruct_pipeline(self, pipeline_id: str, class_path: str) -> "Pipeline":
        """
        Reconstruct the Pipeline instance.

        Options:
        1. Use pipeline_registry for pre-configured instances
        2. Dynamically import and instantiate the class
        """
        # Try registry first
        if pipeline_id in self.pipeline_registry:
            return self.pipeline_registry[pipeline_id]

        # Dynamic import
        module_path, class_name = class_path.rsplit(".", 1)
        module = __import__(module_path, fromlist=[class_name])
        pipeline_class = getattr(module, class_name)

        # Instantiate with default constructor
        return pipeline_class()

    def _deserialize_task(self, task_data: dict) -> "TaskType":
        """
        Reconstruct a PipelineTask from serialized data.

        This requires access to the event registry or dynamic import.
        """
        from volnux.task import PipelineTask
        from volnux.parser.operator import PipeType

        # Reconstruct event class
        event_class_path = task_data["event_class_path"]
        module_path, class_name = event_class_path.rsplit(".", 1)
        module = __import__(module_path, fromlist=[class_name])
        event_class = getattr(module, class_name)

        # Reconstruct task
        task = PipelineTask(event=task_data["event_name"])
        # Note: Full reconstruction would need options and condition_node
        # This is simplified for the example

        return task

    async def _restore_execution_state(
        self, context: "ExecutionContext", snapshot: ContextSnapshot
    ) -> None:
        """
        Restore the ExecutionState from snapshot.
        """
        from .state_manager import ExecutionState, ExecutionStatus

        # Deserialize errors
        # Note: We can't reconstruct full Exception objects, so we store as strings
        errors = []  # Store as logged errors, not Exception objects

        # Deserialize results
        results = [self._deserialize_result(r) for r in snapshot.results]

        aggregated_result = (
            self._deserialize_result(snapshot.aggregated_result)
            if snapshot.aggregated_result
            else None
        )

        # Update state manager
        state = ExecutionState(
            status=ExecutionStatus[snapshot.status],
            errors=errors,
            results=results,
            aggregated_result=aggregated_result,
        )

        context.get_state_manager().update_state(context.state_id, state)

        # Restore metrics
        context.metrics.start_time = snapshot.metrics["start_time"]
        context.metrics.end_time = snapshot.metrics["end_time"]

    def _deserialize_result(self, result_data: dict) -> "EventResult":
        """Reconstruct EventResult from dict"""
        from volnux.result import EventResult

        return EventResult(
            data=result_data["data"],
            # Restore other fields as needed
        )

    async def _rebuild_horizontal_links(
        self, snapshots: typing.List[ContextSnapshot]
    ) -> None:
        """
        Reconstruct the doubly-linked list (previous_context <-> next_context).
        """
        for snapshot in snapshots:
            context = self._context_cache[snapshot.state_id]

            if snapshot.previous_context_id:
                prev_context = self._context_cache.get(snapshot.previous_context_id)
                if prev_context:
                    context.previous_context = prev_context
                    prev_context.next_context = context

            if snapshot.next_context_id:
                next_context = self._context_cache.get(snapshot.next_context_id)
                if next_context:
                    context.next_context = next_context

    async def _rebuild_vertical_links(
        self, snapshots: typing.List[ContextSnapshot]
    ) -> None:
        """
        Reconstruct the tree structure (parent_context <-> child_contexts).
        """
        for snapshot in snapshots:
            context = self._context_cache[snapshot.state_id]

            if snapshot.parent_id:
                parent = self._context_cache.get(snapshot.parent_id)
                if parent:
                    context.parent_context = parent
                    if context not in parent.child_contexts:
                        parent.child_contexts.append(context)

    async def _rehydrate_engines(
        self, snapshots: typing.List[ContextSnapshot], engine_factory: typing.Callable
    ) -> None:
        """
        Create engine instances and attach them to contexts.
        Restores the internal queue state for resumption.
        """
        for snapshot in snapshots:
            context = self._context_cache[snapshot.state_id]

            # Create engine for this context level
            engine = engine_factory()

            # Restore engine's work queue from snapshot
            engine.queue = self._rebuild_engine_queue(snapshot, context)

            # Restore sink queue
            engine.sink_queue = self._rebuild_sink_queue(snapshot)

            # Attach engine to context
            context.set_engine(engine)

            logger.debug(f"Rehydrated engine for context {snapshot.state_id}")

    def _rebuild_engine_queue(
        self, snapshot: ContextSnapshot, context: ExecutionContext
    ) -> typing.Deque["TaskNode"]:
        """
        Reconstruct the engine's work queue (TaskNode deque).

        The engine uses TaskNode objects containing:
        - task: The PipelineTask to execute
        - previous_context: The context from prior execution

        Strategy:
        1. If current_task exists, it goes at the front (was interrupted)
        2. Remaining tasks in queue_snapshot follow in order
        """
        from collections import deque

        queue = deque()

        # If a task was mid-execution, put it back at the front
        if snapshot.traversal.current_task_id:
            current_task = self._find_task_by_id(
                snapshot.traversal.current_task_id, snapshot.traversal.queue_snapshot
            )
            if current_task:
                # Find the previous context in the horizontal chain
                prev_ctx = (
                    self._context_cache.get(snapshot.previous_context_id)
                    if snapshot.previous_context_id
                    else None
                )

                queue.append(TaskNode(task=current_task, previous_context=prev_ctx))

        # Add remaining tasks from the snapshot
        # Note: Exclude the current_task if it was already added
        for task_data in snapshot.traversal.queue_snapshot:
            task = self._deserialize_task(task_data)

            # Skip if this is the current task (already in queue)
            if (
                snapshot.traversal.current_task_id
                and task_data.get("task_id") == snapshot.traversal.current_task_id
            ):
                continue

            # Use the appropriate previous_context for each queued task
            # In most cases, they'll reference the current context being restored
            queue.append(TaskNode(task=task, previous_context=context))

        return queue

    def _rebuild_sink_queue(
        self, snapshot: ContextSnapshot
    ) -> typing.Deque["TaskType"]:
        """
        Reconstruct the engine's sink node queue.

        Sink nodes are deferred for execution after the main workflow.
        """
        from collections import deque

        sink_queue = deque()

        # If sink node data was captured in the snapshot
        # (This would require extending ContextSnapshot to include sink_nodes)
        # For now, return empty queue - sink nodes will be re-collected

        return sink_queue

    def _find_task_by_id(
        self, task_id: str, task_list: typing.List[dict]
    ) -> typing.Optional["TaskType"]:
        """Find a task in the serialized task list by ID"""
        for task_data in task_list:
            if task_data.get("task_id") == task_id:
                return self._deserialize_task(task_data)
        return None

    async def checkpoint_workflow(self, root_context: "ExecutionContext") -> None:
        """
        Persist all contexts in a workflow tree.

        This should be called periodically during execution.
        """
        for context in self._iterate_tree(root_context):
            await context.persist(self.state_store)

    def _iterate_tree(
        self, root: "ExecutionContext"
    ) -> typing.Generator["ExecutionContext", None, None]:
        """
        Traverse the entire tree (depth-first).
        """
        yield root
        for child in root.child_contexts:
            yield from self._iterate_tree(child)
