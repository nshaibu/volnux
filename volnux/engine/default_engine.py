import asyncio
import logging
import typing
from collections import deque

from volnux.exceptions import TaskSwitchingError
from volnux.execution.context import ExecutionContext
from volnux.execution.status import ExecutionStatus
from volnux.parser.operator import PipeType
from volnux.parser.protocols import GroupingStrategy, TaskType
from volnux.execution.pipeline import Pipeline
from volnux.execution.rehydrator.checkpoint_manager import VolnuxCheckPointManager
from volnux.execution.utils import evaluate_context_execution_results
from volnux.task.group import PipelineTaskGrouping
from .base import (
    CheckPointConfig,
    CheckPointFrequency,
    EngineExecutionResult,
    EngineResult,
    TaskNode,
    WorkflowEngine,
    SubgraphErrorStrategy,
)

logger = logging.getLogger(__name__)


class DefaultWorkflowEngine(WorkflowEngine):
    """
    Default iterative workflow execution engine.

    This engine uses a queue-based traversal strategy to execute
    workflows without recursion, preventing stack overflow issues.

    Orchestration Strategy:
    - Queue-based task scheduling (LIFO for depth-first behavior)
    - Parallel task detection and grouping
    - Conditional branching evaluation
    - Dynamic task switching support
    - Deferred sink node execution
    - Context chaining for execution history
    - Opt-in checkpointing via base class hooks

    The engine focuses solely on flow control and delegates:
    - Task execution -> ExecutionContext
    - Metrics collection -> ExecutionContext
    - Hook invocation -> ExecutionContext
    - Error handling -> ExecutionContext
    """

    def __init__(
        self,
        enable_checkpointing: bool = False,
        checkpoint_config: typing.Optional[CheckPointConfig] = None,
        enable_debug_logging: bool = False,
        strict_mode: bool = True,
        parent_engine: typing.Optional["DefaultWorkflowEngine"] = None,
    ):
        """
        Initialize the default workflow engine.

        Args:
            enable_checkpointing: If True, enables checkpoint persistence
            checkpoint_config: Optional checkpoint configuration; defaults
                to CheckPointConfig() when enable_checkpointing is True
            enable_debug_logging: If True, enables detailed flow logging
            strict_mode: If True, stops execution on first error; if False,
                attempts to continue
        """
        super().__init__(
            enable_checkpointing=enable_checkpointing,
            checkpoint_config=checkpoint_config,
            parent_engine=parent_engine,
        )
        self._task_queue: typing.Deque[TaskNode] = deque()
        self._sink_queue: typing.Deque[TaskType] = deque()
        self.enable_debug_logging = enable_debug_logging
        self.strict_mode = strict_mode

        if enable_checkpointing and self._checkpointer is None:
            logger.warning(
                "[Engine] enable_checkpointing=True only stores checkpoint_config; "
                "no persistence will happen until enable_checkpointing() is called "
                "with a VolnuxCheckPointManager instance."
            )

    async def spawn_sub_engine(
        self,
        root_task: TaskType,
        pipeline: Pipeline,
        error_strategy: SubgraphErrorStrategy = SubgraphErrorStrategy.TREAT_AS_FAILURE,
    ) -> "DefaultWorkflowEngine":
        """
        Spawn a child engine for {} grouping or nested workflow execution.

        The child:
        - Inherits the parent's checkpoint manager (workflows share checkpoints)
        - Gets automatic state isolation via ExecutionContext.spawn_child()
        - Uses the specified error propagation strategy
        """
        # A sub-engine never builds its own checkpoint manager — it only ever
        # inherits the parent's, via enable_checkpointing() below.
        child = DefaultWorkflowEngine(
            enable_debug_logging=self.enable_debug_logging,
            strict_mode=self.strict_mode,
            parent_engine=self,
        )

        # Inherit checkpointer (workflows share checkpoint backends)
        if self._checkpointer:
            child.enable_checkpointing(self._checkpointer, self._checkpoint_frequency)

        child._error_strategy = error_strategy
        self.child_engines.append(child)

        if self.enable_debug_logging:
            logger.debug(
                f"[Engine:{self._engine_id}] Spawned child engine "
                f"[{child._engine_id}] at depth {self.get_engine_depth() + 1} "
                f"(error_strategy={error_strategy.value})"
            )

        return child

    @property
    def task_queue(self) -> typing.Deque[TaskNode]:
        """Primary engine queue used for active task scheduling."""
        return self._task_queue

    @property
    def sink_queue(self) -> typing.Deque[TaskType]:
        """Queue used for deferred/sink tasks."""
        return self._sink_queue

    def get_name(self) -> str:
        return "DefaultIterativeEngine"

    def _resolve_sub_engine_error(
        self,
        sub_engine: "DefaultWorkflowEngine",
        error: Exception,
        error_strategy: SubgraphErrorStrategy,
    ) -> EngineResult:
        """
        Map a sub-engine failure to an EngineResult per error_strategy.

        Raises the original error for BUBBLE_UP instead of returning.
        """
        if error_strategy == SubgraphErrorStrategy.BUBBLE_UP:
            raise error
        elif error_strategy == SubgraphErrorStrategy.ISOLATE:
            logger.error(f"Sub-engine failed (isolated): {error}", exc_info=True)
            # Return a synthetic success result to continue parent
            return EngineResult(
                status=EngineExecutionResult.COMPLETED,
                final_context=None,
                tasks_processed=sub_engine.tasks_processed,
            )
        else:  # TREAT_AS_FAILURE
            # Return failed status so parent can follow on_failure branch
            return EngineResult(
                status=EngineExecutionResult.FAILED,
                final_context=sub_engine.final_context,
                error=error,
                tasks_processed=sub_engine.tasks_processed,
            )

    async def execute_subgraph(
        self,
        root_task: TaskType,
        pipeline: Pipeline,
        error_strategy: SubgraphErrorStrategy = SubgraphErrorStrategy.TREAT_AS_FAILURE,
    ) -> EngineResult:
        """
        Convenience method for executing a subgraph with a fresh sub-engine.

        Common pattern: spawn → execute → handle errors → merge task counts.
        """
        sub_engine = await self.spawn_sub_engine(
            root_task=root_task,
            pipeline=pipeline,
            error_strategy=error_strategy,
        )

        try:
            result = await sub_engine.execute(root_task, pipeline)

            # Merge task counts back to parent
            self.tasks_processed += sub_engine.tasks_processed

            if self.enable_debug_logging:
                logger.debug(
                    f"[Engine:{self._engine_id}] Sub-engine [{sub_engine._engine_id}] "
                    f"completed: {result.status.value}, "
                    f"processed {sub_engine.tasks_processed} tasks"
                )

            return result

        except Exception as e:
            self.tasks_processed += sub_engine.tasks_processed
            return self._resolve_sub_engine_error(sub_engine, e, error_strategy)

    async def execute_subgraphs(
        self,
        root_tasks: typing.Sequence[TaskType],
        pipeline: Pipeline,
        error_strategy: SubgraphErrorStrategy = SubgraphErrorStrategy.TREAT_AS_FAILURE,
    ) -> typing.List[EngineResult]:
        """
        Execute multiple subgraphs concurrently, one fresh sub-engine per root task.

        Same spawn → execute → handle errors → merge task counts pattern as
        execute_subgraph(), but all sub-engines run concurrently via
        asyncio.gather() instead of one at a time.
        """
        children = [
            await self.spawn_sub_engine(
                root_task=task, pipeline=pipeline, error_strategy=error_strategy
            )
            for task in root_tasks
        ]

        raw_results = await asyncio.gather(
            *(
                child.execute(task, pipeline)
                for child, task in zip(children, root_tasks)
            ),
            return_exceptions=True,
        )

        # Merge every child's task count first — gather() already ran all of
        # them to completion, so a BUBBLE_UP raise below must not cause any
        # child's contribution to silently go unmerged.
        results: typing.List[EngineResult] = []
        first_bubble_error: typing.Optional[Exception] = None

        for child, raw in zip(children, raw_results):
            self.tasks_processed += child.tasks_processed

            if isinstance(raw, Exception):
                if error_strategy == SubgraphErrorStrategy.BUBBLE_UP:
                    if first_bubble_error is None:
                        first_bubble_error = raw
                    continue
                result = self._resolve_sub_engine_error(child, raw, error_strategy)
            else:
                result = raw

            results.append(result)

            if self.enable_debug_logging:
                logger.debug(
                    f"[Engine:{self._engine_id}] Sub-engine [{child._engine_id}] "
                    f"finished: {result.status.value}, "
                    f"processed {child.tasks_processed} tasks"
                )

        if first_bubble_error is not None:
            raise first_bubble_error

        return results

    async def _execute_task_grouping(
        self,
        task: "PipelineTaskGrouping",
        pipeline: Pipeline,
    ) -> typing.Tuple[typing.Optional[EngineResult], typing.Optional[TaskType]]:
        """
        Run every chain of a MULTIPATH_CHAINS grouping concurrently.

        Returns (early_exit_result, next_task):
        - early_exit_result is set (next_task is None) when a chain came
          back SUSPENDED — the whole workflow must stop and wait, mirroring
          how _should_terminate handles a directly-paused task.
        - Otherwise next_task follows the group's own condition_node,
          taking the failure branch if any chain failed and the success
          branch otherwise — matching _evaluate_conditional_branch's
          success/failure selection.
        """
        results = await self.execute_subgraphs(task.chains, pipeline)

        for result in results:
            if result.status == EngineExecutionResult.SUSPENDED:
                if self.enable_debug_logging:
                    logger.debug(
                        f"[Engine:{self._engine_id}] Task grouping suspended "
                        f"(one of {len(task.chains)} chains is paused)"
                    )
                # Use the engine's own cumulative counters/context, matching
                # every other early-return in execute() — execute_subgraphs()
                # already merged every chain's tasks_processed into self.
                return (
                    EngineResult(
                        status=EngineExecutionResult.SUSPENDED,
                        final_context=result.final_context or self.final_context,
                        tasks_processed=self.tasks_processed,
                    ),
                    None,
                )

        any_failed = any(
            result.status == EngineExecutionResult.FAILED for result in results
        )

        next_task = (
            task.condition_node.on_failure_event
            if any_failed
            else task.condition_node.on_success_event
        )

        if self.enable_debug_logging:
            branch = "failure" if any_failed else "success"
            logger.debug(
                f"[Engine:{self._engine_id}] Task grouping "
                f"({len(task.chains)} chains) branch: {branch}"
            )

        return None, next_task

    async def execute(
        self,
        root_task: TaskType,
        pipeline: Pipeline,
    ) -> EngineResult:
        """
        Execute a workflow using iterative queue-based traversal.

        Flow:
        1. Reset queues and counters for fresh execution
        2. Initialize a work queue with a root task
        3. Process tasks from queue (LIFO via appendleft/popleft)
        4. Detect parallelism and group parallel tasks
        5. Create execution context and chain to previous
        6. Checkpoint before a task (if checkpointing is enabled)
        7. Delegate execution to context (context handles hooks/metrics)
        8. Checkpoint after a task (if checkpointing is enabled)
        9. Evaluate execution state for early termination
        10. Handle dynamic task switching
        11. Evaluate conditionals and determine the next task
        12. Schedule the next task in the queue
        13. Process deferred sink nodes

        Args:
            root_task: The workflow entry point
            pipeline: The pipeline with configuration

        Returns:
            EngineResult with execution status and final context
        """
        if not root_task:
            if self.enable_debug_logging:
                logger.debug("[Engine] No root task provided")
            return EngineResult(
                status=EngineExecutionResult.COMPLETED, tasks_processed=0
            )

        # Start the checkpoint manager's background worker/monitor tasks once,
        # at root-engine startup only — sub-engines share the same manager and
        # must never start (or stop) it themselves. start() is idempotent.
        if self.is_root_engine() and self._checkpointer is not None:
            await self._checkpointer.start()

        # Reset state for fresh execution
        self.task_queue.clear()
        self.sink_queue.clear()
        self.tasks_processed = 0
        self.final_context = None
        self.current_task_node = None

        # Seed the work queue
        self.task_queue.append(TaskNode(root_task, None))

        try:
            while self.task_queue:
                executable_node = self.task_queue.popleft()
                self.tasks_processed += 1

                if self.enable_debug_logging:
                    logger.debug(f"[Engine] Processing task: {executable_node.task}")

                try:
                    task = executable_node.task

                    # {} grouping: MULTIPATH_CHAINS fans out into concurrent
                    # sub-engines instead of the normal single-context
                    # dispatch below. SINGLE_CHAIN falls through unchanged —
                    # it's just executed normally like any other task.
                    if (
                        isinstance(task, PipelineTaskGrouping)
                        and task.strategy == GroupingStrategy.MULTIPATH_CHAINS
                    ):
                        # This path bypasses _build_context(), so replicate
                        # its sink-node collection for consistency.
                        if task.sink_node:
                            self.sink_queue.append(task.sink_node)

                        early_result, next_task = await self._execute_task_grouping(
                            task, pipeline
                        )
                        if early_result is not None:
                            return early_result
                        if next_task:
                            self.task_queue.appendleft(
                                TaskNode(next_task, executable_node.previous_context)
                            )
                        continue

                    # Detect parallelism
                    parallel_tasks = self._detect_parallel_tasks(task)

                    # Build execution context
                    execution_context = await self._build_context(
                        task=task,
                        pipeline=pipeline,
                        previous_context=executable_node.previous_context,
                        parallel_tasks=parallel_tasks,
                    )

                    self.final_context = execution_context

                    # Checkpoint before task execution (idempotency support)
                    await self._checkpoint_before_task(
                        execution_context, executable_node
                    )

                    # Dispatch task profiles for execution
                    await execution_context.dispatch()

                    # Checkpoint after task completion
                    await self._checkpoint_after_task(
                        execution_context,
                        success=execution_context.status == ExecutionStatus.COMPLETED,
                    )

                    if self._should_terminate(execution_context):
                        status = self._map_termination_status(execution_context.status)
                        return EngineResult(
                            status=status,
                            final_context=self.final_context,
                            tasks_processed=self.tasks_processed,
                        )

                    # Handle task switching
                    switched = self._handle_task_switch(
                        task=executable_node.task,
                        execution_context=execution_context,
                        previous_context=executable_node.previous_context,
                        queue=self.task_queue,
                    )
                    if switched:
                        continue

                    # Determine next task
                    next_task = self._resolve_next_task(
                        executable_node.task, execution_context
                    )

                    # Schedule the next task (prepend for LIFO depth-first)
                    if next_task:
                        self.task_queue.appendleft(
                            TaskNode(next_task, execution_context)
                        )

                except Exception as e:
                    logger.error(
                        f"[Engine] Error processing task "
                        f"{executable_node.task}: {e}",
                        exc_info=True,
                    )

                    if self.strict_mode:
                        return EngineResult(
                            status=EngineExecutionResult.FAILED,
                            final_context=self.final_context,
                            error=e,
                            tasks_processed=self.tasks_processed,
                        )
                    # In non-strict mode, continue to next task

            # Process deferred sink nodes
            await self._drain_sink_nodes(pipeline)

            if self.enable_debug_logging:
                logger.debug(
                    f"[Engine] Completed processing {self.tasks_processed} tasks"
                )

        except Exception as e:
            logger.exception("[Engine] Fatal error during workflow execution")
            return EngineResult(
                status=EngineExecutionResult.FAILED,
                final_context=self.final_context,
                error=e,
                tasks_processed=self.tasks_processed,
            )

        return EngineResult(
            status=EngineExecutionResult.COMPLETED,
            final_context=self.final_context,
            tasks_processed=self.tasks_processed,
        )

    async def _checkpoint_after_task(self, context: "ExecutionContext", success: bool):
        """
        Override base to persist after a task without double-counting.

        The execute() loop manages ``self.tasks_processed`` directly,
        so this override only handles persistence and state cleanup.
        """
        if not self._checkpointer:
            return

        self.current_task_node = None

        if self._checkpoint_frequency in [
            CheckPointFrequency.PER_TASK,
            CheckPointFrequency.ON_STATE_CHANGE,
        ]:
            await context.persist()
            logger.debug(
                f"[Engine] Checkpointed after task (success={success}): "
                f"{self.tasks_processed} tasks processed"
            )

    def _detect_parallel_tasks(
        self, task: TaskType
    ) -> typing.Optional[typing.Set[TaskType]]:
        """
        Detect parallel task chains by following PARALLELISM pipes.

        Walks the success path collecting tasks marked for parallel execution
        until a non-parallel pipe or end is reached.

        Args:
            task: Task to check for parallel execution

        Returns:
            Set of tasks to execute in parallel, or None if sequential
        """
        if not task.is_parallel_execution_node:
            return None

        parallel_tasks: typing.Set[TaskType] = set()
        current = task

        while (
            current and current.condition_node.on_success_pipe == PipeType.PARALLELISM
        ):
            parallel_tasks.add(current)
            current = current.condition_node.on_success_event

        # Include final task in parallel chain
        if parallel_tasks and current:
            parallel_tasks.add(current)

        if self.enable_debug_logging and parallel_tasks:
            logger.debug(f"[Engine] Detected {len(parallel_tasks)} parallel tasks")

        return parallel_tasks if parallel_tasks else None

    async def _build_context(
        self,
        task: TaskType,
        pipeline: Pipeline,
        previous_context: typing.Optional[ExecutionContext] = None,
        parallel_tasks: typing.Optional[typing.Set[TaskType]] = None,
    ) -> ExecutionContext:
        """
        Create and chain execution context for root or sub-engine workflows.

        - Root Engine (Entry Node): Creates root ExecutionContext & sets pipeline.execution_context.
        - Sub-Engine (Entry Node): Spawns VERTICALLY from parent_engine.final_context.
        - Subsequent Nodes (Peer Nodes): Chains HORIZONTALLY from previous_context.
        """
        task_profiles = list(parallel_tasks) if parallel_tasks else task

        if previous_context is None:
            # ENTRY NODE OF THIS ENGINE / SUB-ENGINE
            if self.is_root_engine():
                # ROOT ENGINE ENTRY: Create root context & set fractal tree root
                context = await ExecutionContext.create_context(
                    pipeline=pipeline,
                    task_profiles=task_profiles,  # type: ignore
                    workflow_id=getattr(pipeline, "id", self._engine_id),  # type: ignore
                    workflow_name=pipeline.__class__.__name__,
                )
                # Immutable fractal root — assigned ONLY by the root engine
                pipeline.execution_context = context

            else:
                # SUB-ENGINE ENTRY: Validate parent state and spawn VERTICALLY
                if self.parent_engine is None:
                    raise RuntimeError(
                        "Internal error: nested subgraph engine has no parent engine "
                        "reference. The parent engine must be set when a child engine "
                        "is created from a {} block."
                    )

                if self.parent_engine.final_context is None:
                    raise RuntimeError(
                        "Sub-engine cannot create context: parent has no final_context. "
                        "Parent must complete at least one task before {} block begins."
                    )

                parent_ctx = self.parent_engine.final_context

                # VERTICAL LINK: Spawns child subtree root off parent's final_context
                context = await parent_ctx.spawn_child(
                    task_profiles=task_profiles  # type: ignore
                )

        else:
            # SUBSEQUENT NODES WITHIN THE SAME ENGINE (HORIZONTAL PEERS)
            # HORIZONTAL LINK: Peer creation on the same execution depth layer
            context = await ExecutionContext.create_context(
                pipeline=pipeline,
                task_profiles=task_profiles,  # type: ignore
                previous_context=previous_context,
                parent_context=previous_context.parent_context,  # Retain sub-engine parent ref
                workflow_id=previous_context.workflow_id,
                workflow_name=previous_context.workflow_name,
            )

        # HORIZONTAL DOUBLY-LINKED CHAINING & REGISTRATION
        context.set_engine(self)

        if previous_context is not None:
            # Wire horizontal peer links (Node 0 <-> Node 1 <-> Node 2)
            context.previous_context = previous_context
            previous_context.next_context = context

        # Collect sink nodes for deferred execution
        if task.sink_node:
            self.sink_queue.append(task.sink_node)

        return context

    def _should_terminate(self, execution_context: ExecutionContext) -> bool:
        """
        Check if execution should stop due to cancellation/abortion.

        Args:
            execution_context: Current execution context

        Returns:
            True if execution should terminate early
        """
        should_stop = execution_context.status in {
            ExecutionStatus.CANCELLED,
            ExecutionStatus.ABORTED,
            ExecutionStatus.PAUSED,
            ExecutionStatus.FAILED,
        }

        if should_stop and self.enable_debug_logging:
            logger.debug(f"[Engine] Early termination: {execution_context.status}")

        return should_stop

    def _map_termination_status(
        self, execution_status: ExecutionStatus
    ) -> EngineExecutionResult:
        """Map execution status to engine result status."""
        if execution_status == ExecutionStatus.PAUSED:
            return EngineExecutionResult.SUSPENDED
        if execution_status == ExecutionStatus.FAILED:
            return EngineExecutionResult.FAILED
        return EngineExecutionResult.TERMINATED_EARLY

    def _handle_task_switch(
        self,
        task: TaskType,
        execution_context: ExecutionContext,
        queue: typing.Deque[TaskNode],
        previous_context: typing.Optional[ExecutionContext] = None,
    ) -> bool:
        """
        Handle dynamic task switching via descriptors.

        When a task requests a switch, validates the target descriptor
        and schedules the new task with the same previous context.

        Args:
            task: Current task
            execution_context: Context with potential switch request
            previous_context: Context to reuse for switched task
            queue: Work queue to prepend a switched task

        Returns:
            True if a switch occurred, False otherwise

        Raises:
            TaskSwitchingError: If the target descriptor doesn't exist
        """
        switch_request = execution_context.get_switch_request()

        if not switch_request or not switch_request.descriptor_configured:  # type: ignore
            return False

        next_task = task.get_descriptor(switch_request.next_task_descriptor)  # type: ignore

        if next_task is None:
            raise TaskSwitchingError(
                f"Cannot switch to descriptor "
                f"'{switch_request.next_task_descriptor}'",
                params=switch_request,
                code="task-switching-failed",
            )

        # Schedule the switched task
        queue.appendleft(TaskNode(next_task, previous_context))

        if self.enable_debug_logging:
            logger.debug(
                f"[Engine] Switched to descriptor: "
                f"{switch_request.next_task_descriptor}"  # type: ignore
            )

        return True

    def _resolve_next_task(
        self, task: TaskType, execution_context: ExecutionContext
    ) -> typing.Optional[TaskType]:
        """
        Determine the next task based on conditional or sequential flow.

        For conditional tasks, evaluate the condition and follow
         the success / failure branch. For normal tasks, follow a success path.

        Args:
            task: Current task
            execution_context: Context with execution results

        Returns:
            Next task to execute, or None if the workflow ends
        """
        if task.is_conditional:
            return self._evaluate_conditional_branch(task, execution_context)
        else:
            return self._follow_sequential_flow(task, execution_context)

    def _evaluate_conditional_branch(
        self, task: TaskType, execution_context: ExecutionContext
    ) -> typing.Optional[TaskType]:
        """
        Evaluate conditional and select branch.

        Args:
            task: Conditional task
            execution_context: Context with results

        Returns:
            Task on success or failure branch
        """
        result = evaluate_context_execution_results(execution_context)

        if result is None:
            logger.error(f"[Engine] Conditional task has no result: {task}")
            return None

        next_task = (
            task.condition_node.on_failure_event
            if not result.success
            else task.condition_node.on_success_event
        )

        if self.enable_debug_logging:
            branch = "success" if result.success else "failure"
            logger.debug(f"[Engine: {self._engine_id}] Conditional branch: {branch}")

        return next_task  # type: ignore

    def _follow_sequential_flow(
        self, task: TaskType, execution_context: ExecutionContext
    ) -> typing.Optional[TaskType]:
        """
        Follow the normal sequential flow.

        For multitask contexts, uses decision task's success path.
        For single tasks, follows standard success path.

        Args:
            task: Current task
            execution_context: Context

        Returns:
            Next task in sequence
        """
        if execution_context.is_multitask():
            decision_task = execution_context.get_decision_task_profile()
            return (
                decision_task.condition_node.on_success_event if decision_task else None
            )
        else:
            return task.condition_node.on_success_event  # type: ignore

    async def _drain_sink_nodes(self, pipeline: Pipeline) -> None:
        """
        Execute deferred sink nodes.

        Sink nodes are executed after the main workflow completes,
        typically for cleanup or finalization tasks.

        Args:
            pipeline: Workflow pipeline
        """
        if not self.sink_queue:
            return

        if self.enable_debug_logging:
            logger.debug(
                f"[Engine: {self._engine_id}] Processing {len(self.sink_queue)} sink nodes"
            )

        while self.sink_queue:
            sink_task = self.sink_queue.popleft()

            try:
                # Create standalone context for sink node
                context = await ExecutionContext.create_context(
                    workflow_id=self._engine_id,
                    workflow_name=pipeline.name,
                    pipeline=pipeline,
                    task_profiles=sink_task,
                )
                await context.dispatch()

            except Exception as e:
                logger.error(f"[Engine] Error in sink node {sink_task}: {e}")
                if self.strict_mode:
                    raise
