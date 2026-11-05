import asyncio
import logging
import typing
from collections import deque

from volnux.exceptions import (
    EngineIterationLimitExceeded,
    TaskCancelledError,
    TaskSwitchingError,
)
from volnux.execution.context import ExecutionContext
from volnux.execution.status import ExecutionStatus
from volnux.parser.operator import PipeType
from volnux.parser.protocols import TaskType
from volnux.execution.pipeline import Pipeline
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

#: Statuses of a context that stopped to wait for something outside the run.
_WAITING = frozenset({ExecutionStatus.SUSPENDED, ExecutionStatus.PAUSED})


class DefaultWorkflowEngine(WorkflowEngine):
    """
    Default iterative workflow execution engine.

    This engine uses a queue-based traversal strategy to execute
    workflows without recursion, preventing stack overflow issues.

    Orchestration Strategy:
    - Queue-based task scheduling (a successor is pushed in front of the queue, so a
      branch is followed before the tasks queued beside it)
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
        max_iterations: typing.Optional[int] = None,
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
            max_iterations: Most task executions this engine may make in one run
                before it fails as a runaway (a cycle); default the
                ``MAX_ENGINE_ITERATIONS`` setting, 0 for no limit
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
        self._max_iterations = max_iterations
        # sink nodes already queued (a sink runs once, however often its task does)
        self._sink_ids: typing.Set[str] = set()

    def _iteration_limit(self) -> int:
        if self._max_iterations is not None:
            return max(0, self._max_iterations)
        try:
            from volnux.config import VolnuxConfig

            return max(
                0, int(VolnuxConfig.get_instance().get("MAX_ENGINE_ITERATIONS", 10000))
            )
        except Exception:
            return 10000

    def _queue_sink(self, sink: typing.Optional[TaskType]) -> None:
        """Defer ``sink`` to the end of the run, once."""
        if not sink:
            return
        try:
            key = sink.get_id()
        except Exception:
            key = str(id(sink))
        if key in self._sink_ids:
            return
        self._sink_ids.add(key)
        self.sink_queue.append(sink)

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
            self._forget_child(sub_engine)

            if self.enable_debug_logging:
                logger.debug(
                    f"[Engine:{self._engine_id}] Sub-engine [{sub_engine._engine_id}] "
                    f"completed: {result.status.value}, "
                    f"processed {sub_engine.tasks_processed} tasks"
                )

            return result

        except Exception as e:
            self.tasks_processed += sub_engine.tasks_processed
            self._forget_child(sub_engine)
            return self._resolve_sub_engine_error(sub_engine, e, error_strategy)

    def _forget_child(self, child: "DefaultWorkflowEngine") -> None:
        """A finished child is dropped: it holds its context graph, and its counts
        are merged. (Left in ``child_engines`` it would pile up over a long run.)"""
        try:
            self.child_engines.remove(child)
        except ValueError:
            pass

    @staticmethod
    def _cooperative_limit() -> int:
        """Default cap on the chains of a ``{}`` group that run at once (0: none)."""
        try:
            from volnux.config import VolnuxConfig

            return max(
                0,
                int(VolnuxConfig.get_instance().get("COOPERATIVE_MAX_CONCURRENCY", 64)),
            )
        except Exception:
            return 64

    async def execute_subgraphs(
        self,
        root_tasks: typing.Sequence[TaskType],
        pipeline: Pipeline,
        error_strategy: SubgraphErrorStrategy = SubgraphErrorStrategy.TREAT_AS_FAILURE,
        max_concurrency: typing.Optional[int] = None,
    ) -> typing.List[EngineResult]:
        """
        Execute multiple subgraphs concurrently, one fresh sub-engine per root task.

        Same spawn → execute → handle errors → merge task counts pattern as
        execute_subgraph(), but all sub-engines run concurrently via
        asyncio.gather() instead of one at a time.

        At most ``max_concurrency`` of them run at once (default: the
        ``COOPERATIVE_MAX_CONCURRENCY`` configuration value, 64; 0 means no limit).
        The rest wait for a slot; none is dropped. The chains of one group are
        independent of each other, so waiting for a slot cannot deadlock.
        """
        children = [
            await self.spawn_sub_engine(
                root_task=task, pipeline=pipeline, error_strategy=error_strategy
            )
            for task in root_tasks
        ]

        limit = (
            max_concurrency
            if max_concurrency is not None
            else self._cooperative_limit()
        )
        slots = asyncio.Semaphore(limit) if limit else None

        async def run(child: "DefaultWorkflowEngine", task: TaskType) -> EngineResult:
            if slots is None:
                return await child.execute(task, pipeline)
            async with slots:
                return await child.execute(task, pipeline)

        raw_results = await asyncio.gather(
            *(run(child, task) for child, task in zip(children, root_tasks)),
            return_exceptions=True,
        )

        # Merge every child's task count first — gather() already ran all of
        # them to completion, so a BUBBLE_UP raise below must not cause any
        # child's contribution to silently go unmerged.
        results: typing.List[EngineResult] = []
        first_bubble_error: typing.Optional[Exception] = None

        for child, raw in zip(children, raw_results):
            self.tasks_processed += child.tasks_processed
            self._forget_child(child)

            if isinstance(raw, asyncio.CancelledError):
                # a chain cancelled from outside (a consumer that failed stops its
                # producers): that chain failed; the parent was not cancelled
                raw = TaskCancelledError("sub-engine cancelled")
                result = EngineResult(
                    status=EngineExecutionResult.FAILED,
                    final_context=child.final_context,
                    error=raw,
                    tasks_processed=child.tasks_processed,
                )
                results.append(result)
                continue
            if isinstance(raw, BaseException) and not isinstance(raw, Exception):
                raise raw  # KeyboardInterrupt, SystemExit ...
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

    async def _run_group_chains(
        self, task: "PipelineTaskGrouping", pipeline: Pipeline
    ) -> typing.List[EngineResult]:
        """Run every chain of ``task`` concurrently; retry the whole group as a unit
        (``{...}[retry_attempts = N]``) while any chain fails."""
        attempts = max(1, int(getattr(task.options, "retry_attempts", 0) or 0))
        limit = getattr(task.options, "max_concurrency", None)
        extra = {"max_concurrency": limit} if limit is not None else {}
        results = await self.execute_subgraphs(task.chains, pipeline, **extra)
        for attempt in range(2, attempts + 1):
            # a suspended chain waits for something outside; running it again would
            # repeat what it already asked for
            if any(r.status == EngineExecutionResult.SUSPENDED for r in results):
                break
            if not any(self._chain_failed(r) for r in results):
                break
            logger.info(
                f"[Engine:{self._engine_id}] Task grouping failed; retrying "
                f"the whole group (attempt {attempt}/{attempts})"
            )
            results = await self.execute_subgraphs(task.chains, pipeline, **extra)
        return results

    @staticmethod
    def _chain_failed(result: EngineResult) -> bool:
        """A chain failed if its engine failed, or if the last task it ran failed
        (a failure that a branch handled leaves a later task as the last one)."""
        if result.status == EngineExecutionResult.FAILED:
            return True
        if result.status == EngineExecutionResult.SUSPENDED:
            return False  # not failed: waiting
        if result.final_context is None:
            return False
        try:
            outcome = evaluate_context_execution_results(result.final_context)
        except Exception:
            # a context that holds a nested group (no event of its own) has nothing
            # to evaluate: that group's chains were judged on their own
            return False
        return outcome is not None and not outcome.success

    def _detect_group_stream_consumers(
        self, group: "PipelineTaskGrouping"
    ) -> typing.Optional[typing.List[TaskType]]:
        """The tasks that read the streams of a ``{}`` group: ``{ A, B -> C } |-> D``
        makes ``D`` (and whatever streams on from it) read from ``A`` and ``C``.

        None when the group is not followed by ``|->`` or no chain ends on a
        streaming event, in which case ``|->`` is just ``->``.
        """
        if group.condition_node.on_success_pipe != PipeType.PIPE_POINTER:
            return None
        first = group.condition_node.on_success_event
        if first is None:
            return None
        tails = [t for chain in group.chains for t in chain.get_chain_tails()]
        if not any(self._streams(t) for t in tails):
            return None
        return self._detect_stream_segment(first) or self._parallel_group(first)

    async def _execute_stream_group(
        self,
        group: "PipelineTaskGrouping",
        consumers: typing.List[TaskType],
        pipeline: Pipeline,
        previous_context: typing.Optional[ExecutionContext],
    ) -> typing.Tuple[
        typing.Optional[EngineResult],
        typing.Optional[TaskType],
        typing.Optional[ExecutionContext],
    ]:
        """Run a ``{}`` group together with the tasks that stream from it.

        The chains run in sub-engines; the consumers run as a context of this engine,
        at the same time. Returns ``(early_result, next_task, context)``: the next
        task follows the last consumer, as for any stream segment.
        """
        tails = [t for chain in group.chains for t in chain.get_chain_tails()]
        # a plain tail publishes its one result as a stream (the consumer reads one
        # merged stream whatever kind each chain ends on)
        self.mark_plain_stream_producers(
            t.get_id() for t in tails if not self._streams(t)
        )

        context = await self._build_context(
            task=consumers[0],
            pipeline=pipeline,
            previous_context=previous_context or self.final_context,
            parallel_tasks=consumers,
        )

        chains_job = asyncio.ensure_future(self._run_group_chains(group, pipeline))
        try:
            await context.dispatch()
            results = await chains_job
        finally:
            if not chains_job.done():
                chains_job.cancel()
                await asyncio.gather(chains_job, return_exceptions=True)
        self.final_context = context

        if context.status in _WAITING:
            # the consumers are what waits: save the position like any suspended task
            await self._persist_suspension(
                context, TaskNode(consumers[0], previous_context)
            )

        for result in results:
            if result.status == EngineExecutionResult.SUSPENDED:
                return (
                    EngineResult(
                        status=EngineExecutionResult.SUSPENDED,
                        final_context=result.final_context or self.final_context,
                        tasks_processed=self.tasks_processed,
                    ),
                    None,
                    context,
                )

        if self._should_terminate(context):
            return (
                EngineResult(
                    status=self._map_termination_status(context.status),
                    final_context=context,
                    tasks_processed=self.tasks_processed,
                ),
                None,
                context,
            )
        return None, self._resolve_next_task(consumers[0], context), context

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
        results = await self._run_group_chains(task, pipeline)

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

        any_failed = any(self._chain_failed(result) for result in results)

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

        self.tasks_processed = 0
        return await self._with_checkpointing(
            self._run_loop(pipeline, [TaskNode(root_task, None)])
        )

    async def resume(self, pipeline: Pipeline) -> EngineResult:
        """Continue a workflow from where a rehydrated engine stopped.

        The engine comes from ``LazyRehydrator.resume_from``: its work queue holds
        the interrupted task first, then whatever was still to do, and its sink
        queue the deferred sinks. The interrupted task runs in a new context that
        follows the one before it; the suspended context stays as history. The
        workflow then proceeds exactly as an uninterrupted run would.
        """
        seed = list(self.task_queue)
        if not seed:
            return EngineResult(
                status=EngineExecutionResult.COMPLETED,
                final_context=self.final_context,
                tasks_processed=self.tasks_processed,
            )
        return await self._with_checkpointing(
            self._run_loop(pipeline, seed, list(self.sink_queue))
        )

    async def _with_checkpointing(
        self, run: typing.Awaitable[EngineResult]
    ) -> EngineResult:
        """Run ``run`` with the checkpoint manager started, and stopped after.

        Only the root engine starts and stops the manager (sub-engines share
        it). The ``finally`` covers every way out, including a suspension, so
        queued snapshots are persisted before the call returns.
        """
        await self._start_checkpointing()
        try:
            return await run
        finally:
            await self._stop_checkpointing()

    async def _run_loop(
        self,
        pipeline: Pipeline,
        seed: typing.Sequence[TaskNode],
        sinks: typing.Sequence[TaskType] = (),
    ) -> EngineResult:
        """The queue-driven traversal shared by ``execute`` and ``resume``.

        ``seed`` is the initial work queue (the root task for a fresh run, the
        restored queue for a resume) and ``sinks`` the deferred sink nodes.

        Sink nodes are cleanup, so they run however the traversal ended, except
        when it suspended: a suspended run is resumed later and its sinks are
        persisted with it. A failing sink fails a run that otherwise completed
        (in strict mode); after a failure it is only logged.
        """
        # Reset state, then seed the work queue
        self.task_queue.clear()
        self.sink_queue.clear()
        self._sink_ids.clear()
        self.final_context = None
        self.current_task_node = None
        self.task_queue.extend(seed)
        for sink in sinks:
            self._queue_sink(sink)

        result = await self._traverse(pipeline)
        if result.status == EngineExecutionResult.SUSPENDED:
            return result

        completed = result.status == EngineExecutionResult.COMPLETED
        try:
            if completed:
                await self._drain_sink_nodes(pipeline)
            else:
                await self._drain_sink_nodes(pipeline, raise_errors=False)
        except Exception as e:
            logger.exception("[Engine] Error in a sink node")
            return EngineResult(
                status=EngineExecutionResult.FAILED,
                final_context=self.final_context,
                error=e,
                tasks_processed=self.tasks_processed,
            )

        if self.enable_debug_logging:
            logger.debug(f"[Engine] Completed processing {self.tasks_processed} tasks")
        return result

    async def _traverse(self, pipeline: Pipeline) -> EngineResult:
        """Run the work queue to its end (or to the first reason to stop)."""
        limit = self._iteration_limit()
        try:
            while self.task_queue:
                executable_node = self.task_queue.popleft()
                self.tasks_processed += 1

                if limit and self.tasks_processed > limit:
                    error = EngineIterationLimitExceeded(
                        f"The workflow made more than {limit} task executions "
                        f"(MAX_ENGINE_ITERATIONS): it is probably cyclic. Last task: "
                        f"{executable_node.task}"
                    )
                    logger.error(f"[Engine] {error}")
                    return EngineResult(
                        status=EngineExecutionResult.FAILED,
                        final_context=self.final_context,
                        error=error,
                        tasks_processed=self.tasks_processed,
                    )

                # Pipeline.pause() holds the run between tasks; stop() wakes it
                await self._wait_while_paused(pipeline)
                if getattr(pipeline, "_stop_requested", False) is True:
                    return EngineResult(
                        status=EngineExecutionResult.TERMINATED_EARLY,
                        final_context=self.final_context,
                        tasks_processed=self.tasks_processed,
                    )

                if self.enable_debug_logging:
                    logger.debug(f"[Engine] Processing task: {executable_node.task}")

                try:
                    task = executable_node.task

                    # ``{}`` is the cooperative paradigm: every chain of the
                    # group runs concurrently in a sub-engine of its own (a
                    # one-chain group is just a unit that can be retried). This
                    # path bypasses _build_context(), so replicate its
                    # sink-node collection.
                    if isinstance(task, PipelineTaskGrouping):
                        self._queue_sink(task.sink_node)

                        if self.final_context is None:
                            # Nothing has run in this engine yet, but the chains'
                            # contexts need a parent to hang from: the group gets a
                            # context of its own.
                            self.final_context = await self._build_context(
                                task=task, pipeline=pipeline, previous_context=None
                            )

                        consumers = self._detect_group_stream_consumers(task)
                        if consumers:
                            (
                                early_result,
                                next_task,
                                stream_context,
                            ) = await self._execute_stream_group(
                                task,
                                consumers,
                                pipeline,
                                executable_node.previous_context,
                            )
                            if early_result is not None:
                                return early_result
                            if next_task:
                                self.task_queue.appendleft(
                                    TaskNode(next_task, stream_context)
                                )
                            continue

                        early_result, next_task = await self._execute_task_grouping(
                            task, pipeline
                        )
                        if early_result is not None:
                            return early_result
                        if next_task:
                            self.task_queue.appendleft(
                                TaskNode(
                                    next_task,
                                    executable_node.previous_context
                                    or self.final_context,
                                )
                            )
                        continue

                    # Detect parallelism, or a ``|->`` stream segment: both run
                    # their members together as one context
                    parallel_tasks = self._detect_stream_segment(
                        task
                    ) or self._detect_parallel_tasks(task)

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
                    try:
                        await execution_context.dispatch()
                    except BaseException:
                        self._unmonitor(execution_context)
                        raise

                    # A suspended task is resumed later, maybe by another process:
                    # persist where this engine stood before unwinding.
                    if execution_context.status in _WAITING:
                        await self._persist_suspension(
                            execution_context, executable_node
                        )

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

    async def _persist_suspension(
        self, context: "ExecutionContext", node: TaskNode
    ) -> None:
        """Save where a suspended run stands. A storage failure is logged, not raised:
        the run is suspended either way, and turning that into a failure would hide
        the reason it stopped."""
        try:
            await self._save_suspension_state(context, node)
        except Exception:
            logger.exception(
                "[Engine] Could not save the state of the suspended run; "
                "it cannot be resumed"
            )

    async def _save_suspension_state(
        self, context: "ExecutionContext", node: TaskNode
    ) -> None:
        """Persist the engine's position so a suspended run can be resumed.

        Saves the ``EngineStateSnapshot`` (keyed by the suspended context) and a
        ``TaskSnapshot`` for the suspended task, the queued tasks and the deferred
        sinks; the whole graph hangs off those tasks. Sub-engines are not covered:
        resuming inside a ``{}`` group is not supported, so a suspension there is
        logged and left unresumable rather than half-saved.
        """
        if not self.is_root_engine():
            logger.warning(
                "[Engine] Task suspended inside a {} group; it cannot be resumed"
            )
            return

        # imported here: the snapshot module imports this package's base
        from volnux.execution.rehydrator.engine.builder import (
            SnapshotBuilder as EngineSnapshotBuilder,
        )

        builder = EngineSnapshotBuilder()
        # The contexts behind this one hold the results the resumed run builds on
        # and the links it walks; they are only persisted on demand, so persist
        # the chain now (each save also stores that context's results).
        link: typing.Optional[ExecutionContext] = context
        while isinstance(link, ExecutionContext):
            await link.save()
            link = link.previous_context
        tasks = [node.task, *(n.task for n in self.task_queue), *self.sink_queue]
        for task in tasks:
            await builder.build_pipeline_task(task, context.state_id).save()
        await builder.build_engine_state(context, current_task_node=node).save()

    def _unmonitor(self, context: "ExecutionContext") -> None:
        """Stop the periodic checkpointer watching ``context`` (it will not change)."""
        if self._checkpointer is not None:
            try:
                self._checkpointer.unmonitor(context)
            except Exception:
                logger.debug("[Engine] unmonitor failed", exc_info=True)

    async def _checkpoint_after_task(self, context: "ExecutionContext", success: bool):
        """
        Override base to persist after a task without double-counting.

        The execute() loop manages ``self.tasks_processed`` directly,
        so this override only handles persistence and state cleanup.
        """
        if not self._checkpointer:
            return

        self.current_task_node = None
        self._checkpointer.unmonitor(context)

        if self._checkpoint_frequency in [
            CheckPointFrequency.PER_TASK,
            CheckPointFrequency.ON_STATE_CHANGE,
        ]:
            await context.save()
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
            current
            and current.condition_node.on_success_pipe == PipeType.PARALLELISM
            and current not in parallel_tasks  # a cycle on the pointers
        ):
            parallel_tasks.add(current)
            current = current.condition_node.on_success_event

        # Include final task in parallel chain
        if parallel_tasks and current:
            parallel_tasks.add(current)

        if self.enable_debug_logging and parallel_tasks:
            logger.debug(f"[Engine] Detected {len(parallel_tasks)} parallel tasks")

        return parallel_tasks if parallel_tasks else None

    @staticmethod
    def _streams(task: TaskType) -> bool:
        """True when ``task`` is an event whose ``process()`` is a generator."""
        try:
            event_class = task.get_event_class()
        except Exception:
            return False  # unresolved here; the run reports it
        return bool(getattr(event_class, "_volnux_streaming", False))

    @staticmethod
    def _parallel_group(task: TaskType) -> typing.List[TaskType]:
        """``task`` and the tasks joined to it by ``||``, in order (``[task]`` if none)."""
        group = [task]
        current = task
        while current.condition_node.on_success_pipe == PipeType.PARALLELISM:
            current = current.condition_node.on_success_event
            if current is None or current in group:  # the end, or a cycle
                break
            group.append(current)
        return group

    def _detect_stream_segment(
        self, task: TaskType
    ) -> typing.Optional[typing.List[TaskType]]:
        """Collect the tasks linked to ``task`` by ``|->`` into one segment.

        * ``A |-> B |-> C`` is ``[A, B, C]``.
        * ``A |-> (B || C)`` is a fan-out: ``[A, B, C]``.
        * ``(A || B) |-> C`` is a fan-in: ``[A, B, C]`` (``task`` is ``A``, the head of
          the parallel group that feeds ``C``).
        * They combine: ``(A || B) |-> C |-> (D || E)`` is one segment of five.

        The members run together, connected by streams, so an edge only counts when
        at least one of the tasks it leaves from is a streaming event; otherwise
        ``|->`` is just ``->`` and the segment ends there.

        Returns:
            The members in order, or None when ``task`` starts no segment.
        """
        group = self._parallel_group(task)
        members: typing.List[TaskType] = list(group)

        while True:
            tail = group[-1]
            if tail.condition_node.on_success_pipe != PipeType.PIPE_POINTER:
                break
            if not any(self._streams(producer) for producer in group):
                break
            first = tail.condition_node.on_success_event
            if first is None:
                break
            group = self._parallel_group(first)
            if any(member in members for member in group):  # a cycle: stop here
                break
            members.extend(group)

        if len(members) == len(self._parallel_group(task)):
            return None
        if self.enable_debug_logging:
            logger.debug(f"[Engine] Detected a stream segment of {len(members)} tasks")
        return members

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

        if self._resumed_events:
            profiles = list(parallel_tasks) if parallel_tasks else [task]
            staged = {
                profile.get_id(): self._resumed_events.pop(profile.get_id())
                for profile in profiles
                if profile.get_id() in self._resumed_events
            }
            if staged:
                context.set_resumed_events(staged)

        if previous_context is not None:
            # Wire horizontal peer links (Node 0 <-> Node 1 <-> Node 2)
            context.previous_context = previous_context
            previous_context.next_context = context

        # Collect sink nodes for deferred execution
        self._queue_sink(task.sink_node)

        if (
            self._checkpointer is not None
            and self._checkpoint_frequency == CheckPointFrequency.PERIODIC
        ):
            self._checkpointer.monitor(context)

        return context

    @staticmethod
    async def _wait_while_paused(pipeline: Pipeline) -> None:
        """Block while the pipeline is paused (its run gate exists and is clear)."""
        gate = getattr(pipeline, "_run_gate", None)
        if isinstance(gate, asyncio.Event):
            await gate.wait()

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
            ExecutionStatus.SUSPENDED,
            ExecutionStatus.FAILED,
        }

        if should_stop and self.enable_debug_logging:
            logger.debug(f"[Engine] Early termination: {execution_context.status}")

        return should_stop

    def _map_termination_status(
        self, execution_status: ExecutionStatus
    ) -> EngineExecutionResult:
        """Map execution status to engine result status."""
        if execution_status in (ExecutionStatus.PAUSED, ExecutionStatus.SUSPENDED):
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
        if execution_context.is_multitask():
            # a parallel group or stream segment: what follows hangs off its last task
            decision = execution_context.get_decision_task_profile()
            if decision is not None and decision.is_conditional:
                return self._evaluate_conditional_branch(decision, execution_context)
            return self._follow_sequential_flow(task, execution_context)
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

    async def _drain_sink_nodes(
        self, pipeline: Pipeline, raise_errors: typing.Optional[bool] = None
    ) -> None:
        """
        Execute deferred sink nodes.

        Sink nodes are executed after the main workflow ends, typically for cleanup
        or finalization. Each runs in a context of its own (not part of the chain
        the pipeline reports its outcome from). A sink that fails stops the others
        and re-raises in strict mode; with ``raise_errors=False`` (the run already
        failed) it is only logged and the remaining sinks still run.

        Args:
            pipeline: Workflow pipeline
            raise_errors: Whether a failing sink raises; default: ``strict_mode``
        """
        if not self.sink_queue:
            return

        should_raise = self.strict_mode and (
            True if raise_errors is None else raise_errors
        )

        if self.enable_debug_logging:
            logger.debug(
                f"[Engine: {self._engine_id}] Processing {len(self.sink_queue)} sink nodes"
            )

        while self.sink_queue:
            sink_task = self.sink_queue.popleft()

            try:
                # Create standalone context for sink node
                context = await ExecutionContext.create_context(
                    workflow_id=getattr(pipeline, "id", self._engine_id),
                    workflow_name=pipeline.__class__.__name__,
                    pipeline=pipeline,
                    task_profiles=sink_task,
                )
                context.set_engine(self)
                await context.dispatch()

            except Exception as e:
                logger.error(
                    f"[Engine] Error in sink node {sink_task}: {e}", exc_info=True
                )
                if should_raise:
                    raise
