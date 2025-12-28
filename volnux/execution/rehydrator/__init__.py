"""
Volnux Rehydration System - Complete Implementation
====================================================

Implements resilient state recovery for the Doubly-Linked Tree architecture
with Redis persistence backend.
"""

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


# ============================================================================
# 0. TYPE DEFINITIONS
# ============================================================================


@dataclass
class TaskNode:
    """
    Represents a task in the engine's work queue.

    Attributes:
        task: The PipelineTask to execute
        previous_context: Context from the previous execution step
    """

    task: "TaskType"
    previous_context: typing.Optional["ExecutionContext"] = None


# ============================================================================
# 1. SERIALIZATION LAYER
# ============================================================================

# ============================================================================
# 2. REDIS PERSISTENCE LAYER
# ============================================================================


# ============================================================================
# 3. EXECUTION CONTEXT EXTENSIONS
# ============================================================================


class RehydrableExecutionContext:
    """
    Mixin to add rehydration capabilities to ExecutionContext.
    This should be mixed into the existing ExecutionContext class.
    """

    # Weak reference to the engine (not persisted)
    _engine_ref: typing.Optional[weakref.ReferenceType] = None

    # Workflow identifier for grouping contexts
    workflow_id: str = None

    # Checkpoint data for idempotency
    _task_checkpoint: typing.Optional[dict] = None


# ============================================================================
# 4. REHYDRATION MANAGER
# ============================================================================


# ============================================================================
# 5. AUTOMATIC CHECKPOINTING
# ============================================================================


# ============================================================================
# 6. USAGE EXAMPLE
# ============================================================================


async def example_usage():
    """
    Demonstrates the complete rehydration workflow.
    """

    # Initialize persistence layer
    store = PersistentStateStore("redis://localhost:6379/0")
    await store.connect()

    # Scenario 1: Normal execution with checkpointing
    # ------------------------------------------------

    from volnux.pipeline import Pipeline
    from volnux.execution_context import ExecutionContext

    # Create workflow
    pipeline = Pipeline()
    context = ExecutionContext(
        task_profiles=deque([...]), pipeline=pipeline  # Your tasks
    )
    context.workflow_id = "my_workflow_123"

    # Enable auto-checkpointing
    checkpointer = AutoCheckpointer(store)
    checkpointer.register_context(context)
    await checkpointer.start()

    try:
        # Execute workflow
        result = context.dispatch()

    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        # State is already checkpointed

    finally:
        await checkpointer.stop()

    # Scenario 2: Recovery after crash
    # --------------------------------

    # Create rehydration manager
    rehydrator = RehydrationManager(store)

    # Resume the workflow
    recovered_context = await rehydrator.resume_workflow(
        workflow_id="my_workflow_123", engine_factory=lambda: DefaultIterativeEngine()
    )

    if recovered_context:
        # Continue execution from checkpoint
        result = recovered_context.dispatch()
        print(f"Workflow resumed and completed: {result}")

    # Cleanup
    await store.cleanup_workflow("my_workflow_123")
    await store.disconnect()


# ============================================================================
# 7. ENGINE INTEGRATION EXTENSIONS
# ============================================================================


class EngineCheckpointMixin:
    """
    Mixin to add checkpointing capabilities to DefaultWorkflowEngine.

    This tracks execution state needed for rehydration without
    modifying the core engine logic significantly.
    """

    def __init_checkpoint_state__(self):
        """Initialize checkpoint tracking (call in engine __init__)"""
        self._tasks_processed: int = 0
        self._current_task_node: typing.Optional["TaskNode"] = None
        self._checkpointer: typing.Optional[AutoCheckpointer] = None


class CheckpointedWorkflowEngine(DefaultWorkflowEngine):
    """
    Extended DefaultWorkflowEngine with built-in checkpointing support.

    This is the recommended engine for production use with rehydration.

    Usage:
        engine = CheckpointedWorkflowEngine(
            enable_checkpointing=True,
            checkpoint_store=store
        )
        result = await engine.execute_async(root_task, pipeline)
    """

    def __init__(
        self,
        enable_debug_logging: bool = False,
        strict_mode: bool = True,
        enable_checkpointing: bool = False,
        checkpoint_store: typing.Optional[PersistentStateStore] = None,
        checkpoint_interval: float = 5.0,
    ):
        super().__init__(enable_debug_logging, strict_mode)

        # Checkpoint tracking
        self._tasks_processed: int = 0
        self._current_task_node: typing.Optional["TaskNode"] = None
        self._checkpointer: typing.Optional[AutoCheckpointer] = None

        # Initialize checkpointing if requested
        if enable_checkpointing and checkpoint_store:
            self._checkpointer = AutoCheckpointer(
                checkpoint_store, checkpoint_interval=checkpoint_interval
            )

    async def execute_async(
        self,
        root_task: "TaskType",
        pipeline: "Pipeline",
    ) -> "EngineResult":
        """
        Async version of execute() with checkpoint support.

        This is the main entry point for checkpoint-enabled workflows.
        """
        if self._checkpointer:
            await self._checkpointer.start()

        try:
            return await self._execute_with_checkpoints(root_task, pipeline)
        finally:
            if self._checkpointer:
                await self._checkpointer.stop()

    async def _execute_with_checkpoints(
        self,
        root_task: "TaskType",
        pipeline: "Pipeline",
    ) -> "EngineResult":
        """
        Core execution loop with checkpoint integration.

        This mirrors the original execute() but adds checkpoint hooks.
        """
        if not root_task:
            return EngineResult(
                status=EngineExecutionResult.COMPLETED, tasks_processed=0
            )

        # Initialize queues
        self.queue: typing.Deque["TaskNode"] = deque()
        self.queue.append(TaskNode(root_task, None))
        self.sink_queue: typing.Deque["TaskType"] = deque()

        final_context: typing.Optional[ExecutionContext] = None
        execution_error: typing.Optional[Exception] = None

        try:
            while self.queue:
                executable_node = self.queue.popleft()
                self._current_task_node = executable_node

                try:
                    # Detect parallelism
                    parallel_tasks = self._detect_parallel_tasks(executable_node.task)

                    # Build context
                    execution_context = self._build_context(
                        task=executable_node.task,
                        pipeline=pipeline,
                        previous_context=executable_node.previous_context,
                        parallel_tasks=parallel_tasks,
                        sink_queue=self.sink_queue,
                    )

                    # Attach engine to context
                    execution_context.set_engine(self)

                    # Register with checkpointer
                    if self._checkpointer:
                        self._checkpointer.register_context(execution_context)

                    final_context = execution_context

                    # CHECKPOINT: Before task execution (idempotency point)
                    if self._checkpointer:
                        await execution_context.persist(self._checkpointer.state_store)

                    # Execute task
                    execution_context.dispatch()
                    execution_state = execution_context.state

                    # CHECKPOINT: After successful execution
                    if self._checkpointer:
                        await execution_context.persist(self._checkpointer.state_store)

                    self._tasks_processed += 1
                    self._current_task_node = None

                    # Check for early termination
                    if self._should_terminate(execution_state):
                        status = self._map_termination_status(execution_state.status)
                        return EngineResult(
                            status=status,
                            final_context=final_context,
                            tasks_processed=self._tasks_processed,
                        )

                    # Handle task switching
                    switched = self._handle_task_switch(
                        task=executable_node.task,
                        execution_state=execution_state,
                        previous_context=executable_node.previous_context,
                        queue=self.queue,
                    )
                    if switched:
                        continue

                    # Determine next task
                    next_task = self._resolve_next_task(
                        executable_node.task, execution_context
                    )

                    # Schedule next task
                    if next_task:
                        self.queue.appendleft(TaskNode(next_task, execution_context))

                except Exception as e:
                    logger.error(f"Error processing task: {e}", exc_info=True)
                    execution_error = e

                    # CHECKPOINT: On error
                    if self._checkpointer and final_context:
                        await final_context.persist(self._checkpointer.state_store)

                    if self.strict_mode:
                        return EngineResult(
                            status=EngineExecutionResult.FAILED,
                            final_context=final_context,
                            error=e,
                            tasks_processed=self._tasks_processed,
                        )

            # Process sink nodes
            self._drain_sink_nodes(self.sink_queue, pipeline)

            # CHECKPOINT: Final state
            if self._checkpointer and final_context:
                await final_context.persist(self._checkpointer.state_store)

        except Exception as e:
            logger.exception("Fatal error during workflow execution")
            return EngineResult(
                status=EngineExecutionResult.FAILED,
                final_context=final_context,
                error=e,
                tasks_processed=self._tasks_processed,
            )

        return EngineResult(
            status=EngineExecutionResult.COMPLETED,
            final_context=final_context,
            tasks_processed=self._tasks_processed,
        )

    def resume_from_snapshot(
        self,
        tasks_processed: int,
        current_queue: typing.Deque["TaskNode"],
        sink_queue: typing.Deque["TaskType"],
    ):
        """
        Restore engine state from rehydrated snapshot.

        Called by RehydrationManager after reconstructing contexts.

        Args:
            tasks_processed: Number of tasks completed before crash
            current_queue: Reconstructed work queue
            sink_queue: Reconstructed sink queue
        """
        self._tasks_processed = tasks_processed
        self.queue = current_queue
        self.sink_queue = sink_queue
        logger.info(
            f"Engine resumed with {len(current_queue)} queued tasks, "
            f"{tasks_processed} already processed"
        )


# ============================================================================
# 8. INTEGRATION HELPERS
# ============================================================================


class RehydrationIntegration:
    """
    Helper class to integrate rehydration into existing ExecutionContext.

    Usage:
        Add these methods to your ExecutionContext class or use as mixin.
    """

    @staticmethod
    def enhance_execution_context():
        """
        Monkey-patch or mixin these capabilities into ExecutionContext.
        """
        from volnux.execution_context import ExecutionContext

        # Add workflow_id attribute
        if not hasattr(ExecutionContext, "workflow_id"):
            ExecutionContext.workflow_id = None

        # Add engine reference
        if not hasattr(ExecutionContext, "_engine_ref"):
            ExecutionContext._engine_ref = None
            ExecutionContext.set_engine = RehydrableExecutionContext.set_engine
            ExecutionContext.get_engine = RehydrableExecutionContext.get_engine

        # Add snapshot creation
        ExecutionContext.create_snapshot = RehydrableExecutionContext.create_snapshot
        ExecutionContext.persist = RehydrableExecutionContext.persist

        # Add checkpoint support
        ExecutionContext._task_checkpoint = None
        ExecutionContext.set_task_checkpoint = (
            RehydrableExecutionContext.set_task_checkpoint
        )


if __name__ == "__main__":
    # Run example
    asyncio.run(example_usage())
