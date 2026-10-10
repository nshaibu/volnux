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
import abc
from typing import (
    Dict,
    List,
    Tuple,
    Any,
    cast,
    TYPE_CHECKING,
    Literal,
    Optional,
)
from datetime import datetime, timezone

from .state import TriggerStateRecord
from volnux.engine.workflows.workflow import WorkflowExecutionError, WorkflowNotFound

from .triggers import TriggerBase, TriggerLifecycle, TriggerActivation, TriggerType

if TYPE_CHECKING:
    from volnux.engine.workflows import WorkflowRegistry

logger = logging.getLogger(__name__)


class BaseWorkflowConfigExecutor(abc.ABC):

    def __init__(self, workflow_registry: "WorkflowRegistry"):
        self._workflow_registry = workflow_registry

    def get_workflow_registry(self) -> "WorkflowRegistry":
        return self._workflow_registry

    @abc.abstractmethod
    async def execute(self, workflow_name: str, params: Dict[str, Any]) -> Any:
        """
        Executes a workflow by its name with the specified parameters. This method must
        be implemented by subclasses and is designed to handle asynchronous execution
        logic for workflows.

        :param workflow_name: The name of the workflow to be executed.
        :type workflow_name: str
        :param params: A dictionary containing the parameters for the workflow execution.
        :type params: Dict[str, Any]
        :return: The result of the workflow execution.
        :rtype: Any
        """
        raise NotImplementedError("execute() must be implemented by subclasses.")

    async def __call__(self, workflow_name: str, params: Dict[str, Any]) -> Any:
        return await self.execute(workflow_name, params)


class WorkflowConfigExecutor(BaseWorkflowConfigExecutor):

    async def execute(self, workflow_name: str, params: Dict[str, Any]) -> Any:
        """
        Execute the workflow through WorkflowConfig.
        :rtype: Any
        """
        if not self._workflow_registry.is_ready():
            logger.error("Workflow registry is not ready.")
            raise WorkflowExecutionError("Workflow registry is not ready.")

        config = self._workflow_registry.get_workflow_config(workflow_name)
        if config is None:
            logger.error("Workflow config not found.")
            raise WorkflowExecutionError("Workflow config not found.")

        run_type = cast(Literal["batch", "single"], params.get("run_type", "single"))
        params = {k: v for k, v in params.items() if k != "run_type"}

        try:
            result = await config.run_workflow(params=params, run_type=run_type)

            logger.info(f"Workflow {workflow_name} completed successfully")
            return result

        except WorkflowExecutionError as e:
            logger.error(f"Workflow execution error: {e}")
            raise


class TriggerEngine:
    """
    Manages the execution of triggers and workflows.

    The TriggerEngine is responsible for managing the lifecycle of triggers, queuing
    workflow execution requests, and running a pool of consumer workers to process
    workflow tasks. It integrates with a workflow executor to execute registered workflows
    based on the triggers' events and configurations.

    :ivar workflow_executor: The workflow executor is used to handle workflow processing.
    :type workflow_executor: Optional[BaseWorkflowConfigExecutor]
    :ivar consumer_concurrency: The number of consumer workers to process workflow tasks.
    :type consumer_concurrency: int
    :ivar triggers: A mapping between trigger IDs and the respective TriggerBase instances.
    :type triggers: Dict[str, TriggerBase]
    :ivar drain_timeout: The timeout in seconds to wait for the task queue to drain during shutdown.
    :type drain_timeout: int
    :ivar _state_sync_interval: The interval in seconds for synchronizing trigger states.
    :type _state_sync_interval: int
    :ivar task_queue: An asynchronous queue holding tuples of workflow names and their parameters.
    :type task_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]]
    """

    def __init__(
        self,
        workflow_executor: Optional[BaseWorkflowConfigExecutor] = None,
        consumer_concurrency: int = 5,
        max_queue_size: int = 1000,
        drain_timeout: int = 30,
        state_sync_interval: int = 10,
    ):
        """
        Initialize the class with configurations related to the consumer concurrency,
        maximum queue size, drain timeout, and workflow executor. This constructor
        sets up the necessary attributes and validates input parameters to ensure correct
        functionality.

        :param workflow_executor: An instance of BaseWorkflowConfigExecutor to handle
            workflow execution. Defaults to None.
        :param consumer_concurrency: The number of concurrent consumers allowed for
            processing the task queue. Must be at least 1. Defaults to 5.
        :param max_queue_size: The maximum size of the task queue. Once the queue
            reaches this size, additional tasks will be blocked from being added until
            space is available. Defaults to 1000.
        :param drain_timeout: Specifies the timeout (in seconds) for draining the task
            queue when shutting down. Must be a positive number. Defaults to 30.
        :param state_sync_interval: The interval (in seconds) for synchronizing trigger
            states. Must be a positive number. Defaults to 10.
        """
        if consumer_concurrency < 1:
            raise ValueError("consumer_concurrency must be at least 1")

        if drain_timeout <= 0:
            raise ValueError("drain_timeout must be a positive number")

        self.workflow_executor: BaseWorkflowConfigExecutor = workflow_executor  # type: ignore[assignment]
        self.consumer_concurrency = consumer_concurrency
        self.triggers: Dict[str, TriggerBase] = {}
        self._running = False
        self.drain_timeout = drain_timeout
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

    def set_workflow_executor(
        self, workflow_executor: BaseWorkflowConfigExecutor
    ) -> None:
        """
        Sets the workflow executor for the current object. This method allows replacing
        or updating the existing workflow executor with a new instance that adheres to
        the required interface.

        :param workflow_executor: The workflow executor instance is responsible for
            managing workflow configurations.
        :type workflow_executor: BaseWorkflowConfigExecutor
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

    async def fire_trigger(
        self,
        workflow_name: str,
        workflow_params: Dict[str, Any],
        trigger_type: TriggerType,
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
        :return: This function does not return a value.
        :rtype: None

        :raises RuntimeError: If the workflow executor or workflow registry is unavailable,
                              or if the workflow configuration cannot be located.
        :raises ValueError: If the specified trigger type is not enabled for the workflow.
        """
        if not self.has_workflow_executor():
            raise RuntimeError("Workflow executor is not ready.")

        workflow_registry: Optional["WorkflowRegistry"] = getattr(
            self.workflow_executor, "_workflow_registry", None
        )
        if workflow_registry is None:
            raise RuntimeError(
                "Workflow registry was not provided by workflow executor."
            )

        workflow_config = workflow_registry.get_workflow_config(workflow_name)
        if workflow_config is None:
            raise RuntimeError("Workflow config not found.")

        qs = workflow_config._registry._triggers.filter(trigger_type=trigger_type)
        if not qs.exists():
            raise ValueError(
                f"Trigger type {trigger_type.value} not enabled for workflow {workflow_name}"
            )

        try:
            self.task_queue.put_nowait((workflow_name, workflow_params))
            logger.debug(f"fire_trigger: workflow '{workflow_name}' enqueued.")
        except asyncio.QueueFull:
            logger.warning(
                f"fire_trigger: task queue is full, dropping workflow '{workflow_name}'."
            )

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
        if self.workflow_executor is None:
            raise RuntimeError(
                "Cannot start TriggerEngine: workflow_executor is not set."
            )

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

        :raises asyncio.TimeoutError: If the queue draining operation exceeds the
            defined drain timeout.
        """
        self._running = False
        logger.info("Stopping trigger engine — draining queue before shutdown.")

        for trigger in self.triggers.values():
            try:
                await trigger.stop()
                logger.info(f"Stopped trigger: {trigger.trigger_id}")
            except Exception as e:
                logger.error(f"Failed to stop trigger {trigger.trigger_id}: {e}")

        if self._state_sync_task:
            if not self._state_sync_task.done():
                logger.info("Stopping state-sync loop.")
                self._state_sync_task.cancel()
                try:
                    await self._state_sync_task
                except asyncio.CancelledError:
                    pass
                finally:
                    self._state_sync_task = None
            else:
                logger.info("State-sync loop already stopped.")

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
        else:
            logger.info("Queue already empty, skipping drain.")

        for task in self._consumer_tasks:
            task.cancel()

        await asyncio.gather(*self._consumer_tasks, return_exceptions=True)
        self._consumer_tasks.clear()

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
                await trigger.end()
            except Exception as e:
                stop_errors.append((trigger.trigger_id, e))

        if stop_errors:
            for trigger_id, err in stop_errors:
                logger.error(
                    f"force_stop: failed to stop trigger '{trigger_id}': {err}"
                )

        for task in self._consumer_tasks:
            task.cancel()

        await asyncio.gather(*self._consumer_tasks, return_exceptions=True)
        self._consumer_tasks.clear()

        dropped = 0
        while not self.task_queue.empty():
            try:
                self.task_queue.get_nowait()
                self.task_queue.task_done()
                dropped += 1
            except asyncio.QueueEmpty:
                break

        if dropped:
            logger.warning(f"force_stop: dropped {dropped} unprocessed queue item(s).")
        else:
            logger.info("force_stop: queue was already empty.")

        logger.warning("Trigger engine force-stopped.")
        return dropped

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
                    dirty_records = await TriggerStateRecord.get_dirty()
                    for record in dirty_records:
                        await self._apply_state_change(record)
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

        if lifecycle == TriggerLifecycle.PAUSED:
            logger.info("State-sync: pausing trigger '%s'.", record.id)
            await trigger.pause()

        elif lifecycle == TriggerLifecycle.ACTIVE:
            logger.info("State-sync: resuming trigger '%s'.", record.id)
            await trigger.resume()

        elif lifecycle == TriggerLifecycle.STOPPED:
            logger.info("State-sync: stopping trigger '%s'.", record.id)
            await trigger.end()

        else:
            logger.debug(
                "State-sync: no action required for lifecycle '%s' on trigger '%s'.",
                lifecycle,
                record.id,
            )

        # Clear the dirty flag and persist.
        record.dirty = False
        record.updated_at = datetime.now(timezone.utc)
        await record.save()

    async def _handle_activation(self, activation: TriggerActivation):
        """
        Callback provided to triggers. Enqueues the task for background execution.
        """
        trigger = self.triggers.get(activation.trigger_id)
        if not trigger:
            logger.error(f"Unknown trigger: {activation.trigger_id}")
            return

        try:
            # Enqueue the workflow details
            await self.task_queue.put(
                (trigger.workflow_name, activation.workflow_params)
            )
            logger.debug(f"Workflow '{trigger.workflow_name}' enqueued.")
        except asyncio.QueueFull:
            logger.warning(
                f"Task queue is full; dropping activation for trigger '{trigger.trigger_id}' "
                f"(workflow: '{trigger.workflow_name}')."
            )
            trigger.state.error_count += 1
            await trigger.state.save_async()
        except Exception as e:
            # If putting on the queue fails (rare for asyncio.Queue)
            logger.error(f"Failed to enqueue workflow for {trigger.trigger_id}: {e}")
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
                    workflow_name, params = item

                    logger.info(f"Worker {worker_id} executing '{workflow_name}'")
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
