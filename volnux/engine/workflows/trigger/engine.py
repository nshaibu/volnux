"""
CLI for stop usage
import signal

engine = TriggerEngine(executor, drain_timeout=30.0)

async def shutdown(sig: signal.Signals):
    logger.warning(f"Received {sig.name}, initiating graceful shutdown...")
    try:
        await asyncio.wait_for(engine.stop(), timeout=35.0)
    except asyncio.TimeoutError:
        logger.error("Graceful shutdown timed out — forcing stop.")
        dropped = await engine.force_stop()
        if dropped:
            logger.error(f"{dropped} workflow(s) were lost.")

loop = asyncio.get_event_loop()
for sig in (signal.SIGTERM, signal.SIGINT):
    loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
"""

import logging
import asyncio
from typing import (
    Dict,
    List,
    Tuple,
    Any,
    TYPE_CHECKING,
    Optional,
)

from volnux.config import setting
from .executors.base import WorkflowExecutor
from .state import TriggerStateRecord
from volnux.engine.workflows.workflow import WorkflowExecutionError, WorkflowNotFound

from .triggers import TriggerBase, TriggerLifecycle, TriggerActivation, TriggerType

if TYPE_CHECKING:
    from volnux.engine.workflows import WorkflowRegistry

logger = logging.getLogger(__name__)


class TriggerEngine:
    """
    Manages the execution of triggers and workflows.

    The TriggerEngine is responsible for managing the lifecycle of triggers, queuing
    workflow execution requests, and running a pool of consumer workers to process
    workflow tasks. It integrates with a workflow executor to execute registered workflows
    based on the triggers' events and configurations.

    :ivar workflow_executor: The workflow executor is used to handle workflow processing.
    :type workflow_executor: Optional[WorkflowExecutor]
    :ivar consumer_concurrency: The number of consumer workers to process workflow tasks.
    :type consumer_concurrency: int
    :ivar triggers: A mapping between trigger IDs and the respective TriggerBase instances.
    :type triggers: Dict[str, TriggerBase]
    :ivar drain_timeout: The timeout in seconds to wait for the task queue to drain during shutdown.
    :type drain_timeout: float
    :ivar _state_sync_interval: The interval in seconds for synchronizing trigger states.
    :type _state_sync_interval: float
    :ivar task_queue: An asynchronous queue holding tuples of workflow names and their parameters.
    :type task_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]]
    """

    def __init__(
        self,
        workflow_executor: Optional[WorkflowExecutor] = None,
        consumer_concurrency: Optional[int] = None,
        max_queue_size: Optional[int] = None,
        drain_timeout: Optional[float] = None,
        state_sync_interval: Optional[float] = None,
    ):
        """
        Initialize the class with configurations related to the consumer concurrency,
        maximum queue size, drain timeout, and workflow executor. This constructor
        sets up the necessary attributes and validates input parameters to ensure correct
        functionality.

        :param workflow_executor: An instance of WorkflowExecutor to handle
            workflow execution. Defaults to None.
        :param consumer_concurrency: The number of concurrent consumers allowed for
            processing the task queue. Must be at least 1. Defaults to the
            ``TRIGGER_ENGINE_CONSUMER_CONCURRENCY`` setting (5).
        :param max_queue_size: The maximum size of the task queue. Once the queue
            reaches this size, further activations are dropped (and counted as errors
            of their trigger). 0 means unbounded. Defaults to the
            ``TRIGGER_ENGINE_MAX_QUEUE_SIZE`` setting (1000).
        :param drain_timeout: Specifies the timeout (in seconds) for draining the task
            queue when shutting down. Must be a positive number. Defaults to the
            ``TRIGGER_ENGINE_DRAIN_TIMEOUT`` setting (30).
        :param state_sync_interval: The interval (in seconds) for synchronizing trigger
            states. Must be a positive number. Defaults to the
            ``TRIGGER_ENGINE_STATE_SYNC_INTERVAL`` setting (10).
        """
        if consumer_concurrency is None:
            consumer_concurrency = int(
                setting("TRIGGER_ENGINE_CONSUMER_CONCURRENCY", 5)
            )
        if max_queue_size is None:
            max_queue_size = int(setting("TRIGGER_ENGINE_MAX_QUEUE_SIZE", 1000))
        if drain_timeout is None:
            drain_timeout = float(setting("TRIGGER_ENGINE_DRAIN_TIMEOUT", 30))
        if state_sync_interval is None:
            state_sync_interval = float(
                setting("TRIGGER_ENGINE_STATE_SYNC_INTERVAL", 10)
            )

        if consumer_concurrency < 1:
            raise ValueError("consumer_concurrency must be at least 1")

        if drain_timeout <= 0:
            raise ValueError("drain_timeout must be a positive number")

        if state_sync_interval <= 0:
            # (a zero interval would poll the state store in a busy loop)
            raise ValueError("state_sync_interval must be a positive number")

        if max_queue_size < 0:
            raise ValueError("max_queue_size must not be negative (0 means unbounded)")

        self.workflow_executor: WorkflowExecutor = workflow_executor  # type: ignore[assignment]
        self.consumer_concurrency = consumer_concurrency
        self.triggers: Dict[str, TriggerBase] = {}
        self._running = False
        self.drain_timeout = drain_timeout
        self._max_queue_size = max_queue_size
        self._state_sync_interval = state_sync_interval

        # The asynchronous queue to hold (workflow_name, params) tuples
        self.task_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]] = asyncio.Queue(
            maxsize=max_queue_size
        )

        # The background tasks for processing the queue
        self._consumer_tasks: List[asyncio.Task] = []

        self._state_sync_task: Optional[asyncio.Task] = None

    def is_running(self) -> bool:
        return self._running

    def has_workflow_executor(self) -> bool:
        return self.workflow_executor is not None

    def set_workflow_executor(self, workflow_executor: WorkflowExecutor) -> None:
        """
        Sets the workflow executor for the current object. This method allows replacing
        or updating the existing workflow executor with a new instance that adheres to
        the required interface.

        :param workflow_executor: The workflow executor instance is responsible for
            managing workflow configurations.
        :type workflow_executor: WorkflowExecutor
        :return: None
        """
        self.workflow_executor = workflow_executor

    def get_workflow_registry(self) -> "WorkflowRegistry":
        """
        Retrieves the workflow registry associated with the workflow executor. This method ensures
        that the framework has been properly initialized and a workflow executor is available before
        attempting to access the workflow registry.

        :raises WorkflowExecutionError: If the workflow executor is not available, or the framework
            has not been initialized correctly.

        :return: The workflow registry instance associated with the workflow executor.
        :rtype: WorkflowRegistry
        """
        if not self.has_workflow_executor():
            raise WorkflowExecutionError(
                "Workflow triggers executor was not provided. The framework has not been initialized yet."
            )
        return self.workflow_executor.get_workflow_registry()

    def register(self, trigger: TriggerBase) -> None:
        """
        Register a trigger and set its activation callback.
        Args:
            trigger: instance of the trigger
        Raises:
            WorkflowExecutionError: if no trigger executor was provided
            WorkflowNotFound: Exception when workflow is not found
            ValueError: Trigger already registered
        """
        if trigger.trigger_id in self.triggers:
            raise ValueError(
                f"Trigger with id '{trigger.trigger_id}' is already registered."
            )

        workflow = self.get_workflow_registry().get_workflow_config(
            trigger.workflow_name
        )
        if workflow is None:
            raise WorkflowNotFound(
                f"Workflow with name '{trigger.workflow_name}' was not found"
            )

        workflow.triggers.register(trigger, self._handle_activation)
        self.triggers[trigger.trigger_id] = trigger

        logger.info(f"Registered trigger: {trigger.trigger_id}")

    def register_manager(self, manager: TriggerBase) -> None:
        """Register a trigger that serves the whole node, not one workflow.

        A manager (``TriggerType.MANAGER``, such as the rehydration manager)
        listens on a queue shared by every workflow, so it has no workflow to be
        attached to and is not subject to the workflow registry. It is started and
        stopped with the engine like any other trigger.

        Raises:
            ValueError: ``manager`` is not a manager trigger, or is already registered.
        """
        if manager.trigger_type is not TriggerType.MANAGER:
            raise ValueError(
                f"register_manager() takes a MANAGER trigger, got "
                f"{manager.trigger_type.value!r}."
            )
        if manager.trigger_id in self.triggers:
            raise ValueError(
                f"Trigger with id '{manager.trigger_id}' is already registered."
            )
        self.triggers[manager.trigger_id] = manager
        logger.info(f"Registered manager: {manager.trigger_id}")

    async def fire_trigger(
        self,
        workflow_name: str,
        workflow_params: Dict[str, Any],
        trigger_type: TriggerType,
        execution_id: Optional[str] = None,
    ) -> None:
        """
        Fire a trigger to execute a specified workflow with parameters. This method ensures
        that the trigger type matches the allowed triggers for the workflow, validates the
        workflow configuration using its registry, and queues the workflow for execution
        if the task queue permits.

        :param workflow_name: The unique name of the workflow to trigger.
        :type workflow_name: str
        :param workflow_params: A dictionary of parameters needed for the workflow execution.
        :type workflow_params: Dict[str, Any]
        :param trigger_type: The type of trigger that initiates the workflow.
        :type trigger_type: TriggerType
        :param execution_id: The caller's id for this run (the platform's ``Execution``
            record). It travels with the queued run to the executor, which uses it to
            pause, resume or stop the run and to report its end.
        :type execution_id: Optional[str]
        :return: This function does not return a value.
        :rtype: None

        :raises RuntimeError: If the workflow executor or workflow registry is unavailable,
                              or if the workflow configuration cannot be located.
        :raises ValueError: If the specified trigger type is not enabled for the workflow.
        """
        if not self.has_workflow_executor():
            raise RuntimeError("Workflow executor is not ready.")

        workflow_registry: Optional["WorkflowRegistry"] = (
            self.workflow_executor.get_workflow_registry()
        )
        if workflow_registry is None:
            raise RuntimeError(
                "Workflow registry was not provided by workflow executor."
            )

        workflow_config = workflow_registry.get_workflow_config(workflow_name)
        if workflow_config is None:
            raise RuntimeError("Workflow config not found.")

        if not workflow_config.triggers.has_type(trigger_type):
            raise ValueError(
                f"Trigger type {trigger_type.value} not enabled for workflow {workflow_name}"
            )

        queued = (
            self._enqueue(workflow_name, workflow_params)
            if execution_id is None
            else self._enqueue(workflow_name, workflow_params, execution_id)
        )
        if queued:
            logger.debug(f"fire_trigger: workflow '{workflow_name}' enqueued.")
        else:
            logger.warning(
                f"fire_trigger: task queue is full, dropping workflow '{workflow_name}'."
            )

    def _enqueue(
        self,
        workflow_name: str,
        params: Dict[str, Any],
        execution_id: Optional[str] = None,
    ) -> bool:
        """Put a workflow run on the queue without waiting.

        ``put_nowait``, not ``put``: on a full bounded queue ``put()`` waits for room
        and never raises ``QueueFull``, which would stall the caller (a trigger, and
        everything awaiting its ``activate()``) behind a backlog.

        :return: False if the queue is full (the run is dropped), True otherwise.
        """
        try:
            item = (
                (workflow_name, params)
                if execution_id is None
                else (workflow_name, params, execution_id)
            )
            self.task_queue.put_nowait(item)
        except asyncio.QueueFull:
            return False
        return True

    async def start_trigger(self, trigger_id: str) -> bool:
        """
        Start a trigger registered after the engine started (``start()`` only
        starts the triggers known when it runs). Does nothing, returning False,
        while the engine is not running or the trigger is already started.

        :raises ValueError: no trigger with this id is registered.
        """
        trigger = self.triggers.get(trigger_id)
        if trigger is None:
            raise ValueError(f"Trigger '{trigger_id}' is not registered.")
        if not self._running or trigger.lifecycle in (
            TriggerLifecycle.ACTIVE,
            TriggerLifecycle.PAUSED,
        ):
            return False
        await trigger.run()
        return True

    async def unregister(self, trigger_id: str) -> None:
        """
        Remove a trigger: it is stopped, and no longer known to the engine or to
        its workflow.

        :raises ValueError: no trigger with this id is registered.
        """
        trigger = self.triggers.get(trigger_id)
        if trigger is None:
            raise ValueError(f"Trigger '{trigger_id}' is not registered.")

        try:
            await self._end_trigger(trigger)
        finally:
            del self.triggers[trigger_id]
            workflow = self.get_workflow_registry().get_workflow_config(
                trigger.workflow_name
            )
            if workflow is not None:
                try:
                    workflow.triggers.unregister(trigger_id)
                except ValueError:
                    pass  # (the workflow no longer holds it)
        logger.info(f"Unregistered trigger: {trigger_id}")

    async def start(self):
        """
        Starts the TriggerEngine and all its associated workers and triggers.

        This method initializes and launches the workflow consumer workers based on the
        configured concurrency level. It also starts all assigned triggers stored in the
        `triggers` collection. Each trigger is executed asynchronously, and its lifecycle
        state is updated accordingly. If any trigger fails to start, an error is logged,
        and its lifecycle is marked as `ERROR`.

        Errors such as missing workflow executors are checked at the beginning, and
        an appropriate RuntimeError is raised to prevent attempting to start the engine
        in an invalid state.

        :raises RuntimeError: If the workflow_executor attribute is not set before
            starting the engine.
        """
        if not self.has_workflow_executor():
            raise RuntimeError(
                "Cannot start TriggerEngine: workflow_executor is not set."
            )

        if self._running:
            # (a second start would add a second pool of consumers and a second
            # state-sync loop)
            logger.warning("TriggerEngine is already running; start() ignored.")
            return

        self._running = True
        logger.info(f"Starting trigger engine with {len(self.triggers)} triggers")

        for i in range(self.consumer_concurrency):
            worker = asyncio.create_task(
                self._workflow_consumer_worker(i), name=f"Trigger-Worker-{i}"
            )
            self._consumer_tasks.append(worker)

        self._state_sync_task = asyncio.create_task(
            self._state_sync_loop(), name="State-Sync-Worker"
        )

        logger.info(f"Started {self.consumer_concurrency} workflow consumer worker(s).")

        for trigger in self.triggers.values():
            try:
                await trigger.run()
                logger.info(f"Started trigger: {trigger.trigger_id}")
            except Exception as e:
                logger.error(f"Failed to start trigger {trigger.trigger_id}: {e}")
                trigger.state.lifecycle = TriggerLifecycle.ERROR
                await trigger.state.save_async()

    async def stop(self):
        """
        Stops the trigger engine and performs a clean shutdown process.

        The method ensures that all active triggers are gracefully stopped, the task
        queue is drained within a timeout (if applicable), and all consumer tasks are
        cancelled and awaited cleanly. If the queue draining timeout occurs, any
        remaining items in the queue will be dropped.

        A drain that exceeds ``drain_timeout`` is not an error: what is still queued
        is dropped (and logged), so a later ``start()`` does not run stale items.
        """
        self._running = False
        logger.info("Stopping trigger engine — draining queue before shutdown.")

        for trigger in self.triggers.values():
            try:
                if await self._end_trigger(trigger):
                    logger.info(f"Stopped trigger: {trigger.trigger_id}")
            except Exception as e:
                logger.error(f"Failed to stop trigger {trigger.trigger_id}: {e}")

        await self._stop_state_sync()

        remaining = self.task_queue.qsize()
        if remaining > 0:
            logger.info(
                f"Draining {remaining} queued item(s) "
                f"(timeout: {self.drain_timeout}s) ..."
            )
            try:
                await asyncio.wait_for(
                    self.task_queue.join(),
                    timeout=self.drain_timeout,
                )
                logger.info("Queue drained successfully.")
            except asyncio.TimeoutError:
                leftover = self.task_queue.qsize()
                logger.warning(
                    f"Drain timed out after {self.drain_timeout}s — "
                    f"{leftover} item(s) will be dropped."
                )
                self._clear_queue()
        else:
            logger.info("Queue already empty, skipping drain.")

        await self._cancel_consumers()

        logger.info("Trigger engine and consumer pool stopped.")

    async def force_stop(self) -> int:
        """
        Forcefully stops the trigger engine, halting all tasks and clearing the queue.
        This method overrides the standard shutdown process, skipping any pending
        queue operations and force-cancelling all consumer tasks. Any errors
        encountered during the stopping process will be logged. Additionally, the
        method logs the number of unprocessed queue items that were dropped during the
        operation.

        :raises Exception: Captures and logs exceptions raised during trigger stoppage.
        :raises asyncio.CancelledError: Any tasks forcefully cancelled may surface
            this exception during cancellation handling.
        :return: The number of unprocessed queue items dropped during the shutdown.
        :rtype: int
        """
        self._running = False
        logger.warning("force_stop() called — skipping queue drain.")

        stop_errors: List[Tuple[str, Exception]] = []
        for trigger in self.triggers.values():
            try:
                await self._end_trigger(trigger)
            except Exception as e:
                stop_errors.append((trigger.trigger_id, e))

        if stop_errors:
            for trigger_id, err in stop_errors:
                logger.error(
                    f"force_stop: failed to stop trigger '{trigger_id}': {err}"
                )

        # (the state-sync loop used to be left running after a force stop)
        await self._stop_state_sync()
        await self._cancel_consumers()

        dropped = self._clear_queue()

        if dropped:
            logger.warning(f"force_stop: dropped {dropped} unprocessed queue item(s).")
        else:
            logger.info("force_stop: queue was already empty.")

        logger.warning("Trigger engine force-stopped.")
        return dropped

    async def _stop_state_sync(self) -> None:
        """Cancel the state-sync loop and wait until it has ended."""
        task, self._state_sync_task = self._state_sync_task, None
        if task is None:
            return
        if task.done():
            logger.info("State-sync loop already stopped.")
            return
        logger.info("Stopping state-sync loop.")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _cancel_consumers(self) -> None:
        """Cancel the consumer pool and wait until every worker has ended."""
        tasks, self._consumer_tasks = self._consumer_tasks, []
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _clear_queue(self) -> int:
        """Drop everything still queued; returns how many items were dropped."""
        dropped = 0
        while True:
            try:
                self.task_queue.get_nowait()
            except asyncio.QueueEmpty:
                return dropped
            self.task_queue.task_done()
            dropped += 1

    @staticmethod
    async def _end_trigger(trigger: TriggerBase) -> bool:
        """Stop a trigger and record it as STOPPED; returns whether it was stopped.

        ``end()`` is the whole shutdown: the trigger's own ``stop()`` hook (timers,
        subscriptions) and the persisted lifecycle. Calling only ``stop()`` left the
        record saying ACTIVE after the engine had shut down. A trigger that never
        ran, or is already stopped, has nothing to stop.
        """
        if trigger.lifecycle in (
            TriggerLifecycle.CREATED,
            TriggerLifecycle.INITIALIZED,
            TriggerLifecycle.STOPPED,
        ):
            return False
        await trigger.end()
        return True

    async def _state_sync_loop(self):
        """
        Background task: poll for dirty TriggerStateRecord rows every
        ``self._state_sync_interval`` seconds and apply the requested changes.
        """
        logger.debug(
            "State-sync loop started (interval=%ss).", self._state_sync_interval
        )
        try:
            while True:
                await asyncio.sleep(self._state_sync_interval)
                try:
                    dirty_records = await (
                        await TriggerStateRecord.get_dirty()
                    ).to_list()
                    for record in dirty_records:
                        try:
                            await self._apply_state_change(record)
                        except Exception as exc:
                            logger.exception(
                                "State-sync: could not apply change to trigger '%s': %s",
                                record.id,
                                exc,
                            )
                except Exception as exc:
                    logger.exception("Error during state-sync poll: %s", exc)
        except asyncio.CancelledError:
            logger.info("State-sync loop cancelled — shutting down cleanly.")

    async def _apply_state_change(self, record: TriggerStateRecord):
        """
        Apply a state change described by a dirty TriggerStateRecord.

        Looks up the live trigger object, applies pause / resume / stop as
        indicated by ``record.lifecycle``, then clears the dirty flag and
        persists the updated record.
        """
        trigger = self.triggers.get(record.id)
        if trigger is None:
            logger.warning(
                "State-sync: trigger '%s' not found in engine registry; skipping.",
                record.id,
            )
            record.dirty = False
            await record.save()
            return

        lifecycle = record.lifecycle

        try:
            if lifecycle == trigger.lifecycle:
                logger.debug(
                    "State-sync: trigger '%s' is already '%s'.",
                    record.id,
                    lifecycle.value,
                )

            elif lifecycle == TriggerLifecycle.PAUSED:
                logger.info("State-sync: pausing trigger '%s'.", record.id)
                await trigger.pause()

            elif lifecycle == TriggerLifecycle.ACTIVE:
                logger.info("State-sync: resuming trigger '%s'.", record.id)
                await trigger.resume()

            elif lifecycle == TriggerLifecycle.STOPPED:
                logger.info("State-sync: stopping trigger '%s'.", record.id)
                await self._end_trigger(trigger)

            else:
                logger.debug(
                    "State-sync: no action required for lifecycle '%s' on trigger '%s'.",
                    lifecycle.value,
                    record.id,
                )
        except RuntimeError as exc:
            # A request that cannot apply (resume of a stopped trigger, ...) is
            # reported and dropped: left dirty it would be retried forever.
            logger.warning(
                "State-sync: cannot apply '%s' to trigger '%s': %s",
                lifecycle.value,
                record.id,
                exc,
            )

        # Clear the dirty flag and persist.
        record.dirty = False
        await record.save()  # stamps updated_at

    async def _handle_activation(self, activation: TriggerActivation):
        """
        Callback provided to triggers. Enqueues the task for background execution.
        """
        trigger = self.triggers.get(activation.trigger_id)
        if not trigger:
            logger.error(f"Unknown trigger: {activation.trigger_id}")
            return

        try:
            queued = self._enqueue(trigger.workflow_name, activation.workflow_params)
        except Exception as e:
            # (rare for an asyncio.Queue)
            logger.error(f"Failed to enqueue workflow for {trigger.trigger_id}: {e}")
            queued = False
        else:
            if queued:
                logger.debug(f"Workflow '{trigger.workflow_name}' enqueued.")
            else:
                logger.warning(
                    f"Task queue is full; dropping activation for trigger "
                    f"'{trigger.trigger_id}' (workflow: '{trigger.workflow_name}')."
                )

        if not queued:
            trigger.state.error_count += 1
            await trigger.state.save_async()

    async def _workflow_consumer_worker(self, worker_id: int):
        """
        Start and manage an asynchronous consumer worker responsible for processing
        tasks from a queue using a workflow_executor. The worker continuously fetches
        tasks from the task queue until cancelled and logs its progress and any
        execution errors.

        :param worker_id: The unique identifier for the consumer worker.
        :type worker_id: int
        :return: None.
        :rtype: None
        """
        logger.debug(f"Consumer Worker {worker_id} started.")
        workflow_name = f"<unknown:{worker_id}>"
        while True:
            try:
                item = await self.task_queue.get()
                try:
                    workflow_name, params, *rest = item

                    logger.info(f"Worker {worker_id} executing '{workflow_name}'")
                    if rest:  # (the caller's execution id, when there is one)
                        await self.workflow_executor(
                            workflow_name, params, execution_id=rest[0]
                        )
                    else:
                        await self.workflow_executor(workflow_name, params)
                    logger.info(f"Worker {worker_id} finished '{workflow_name}'")
                finally:
                    self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Worker {worker_id} execution failure for {workflow_name}: {e}"
                )
