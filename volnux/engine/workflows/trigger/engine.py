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

from volnux.engine.workflows.workflow import WorkflowExecutionError, WorkflowNotfound
from .triggers import TriggerBase, TriggerLifecycle, TriggerActivation

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
        Execute workflow through WorkflowConfig.
        Args:
            workflow_name: workflow name
            params: workflow parameters
        """
        pass

    async def __call__(self, workflow_name: str, params: Dict[str, Any]) -> Any:
        return await self.execute(workflow_name, params)


class WorkflowConfigExecutor(BaseWorkflowConfigExecutor):

    async def execute(self, workflow_name: str, params: Dict[str, Any]) -> Any:
        """
        Execute workflow through WorkflowConfig.
        """
        if not self._workflow_registry.is_ready():
            logger.error("Workflow registry is not ready.")
            raise WorkflowExecutionError("Workflow registry is not ready.")

        config = self._workflow_registry.get_workflow_config(workflow_name)
        if config is None:
            logger.error("Workflow config not found.")
            raise WorkflowExecutionError("Workflow config not found.")

        run_type = cast(Literal["batch", "single"], params.pop("run_type", "single"))

        try:
            result = config.run_workflow(params=params, run_type=run_type)

            logger.info(f"Workflow {workflow_name} completed successfully")
            return result

        except WorkflowExecutionError as e:
            logger.error(f"Workflow execution error: {e}")
            raise


class TriggerEngine:
    """
    Engine that orchestrates self-activating triggers and offloads workflow
    execution to an internal asynchronous task queue.
    """

    def __init__(
        self,
        workflow_executor: Optional[BaseWorkflowConfigExecutor] = None,
        consumer_concurrency: int = 5,
    ):
        self.workflow_executor = workflow_executor
        self.consumer_concurrency = consumer_concurrency
        self.triggers: Dict[str, TriggerBase] = {}
        self._running = False

        # The asynchronous queue to hold (workflow_name, params) tuples
        self.task_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]] = asyncio.Queue()

        # The background tasks for processing the queue
        self._consumer_tasks: List[asyncio.Task] = []

    def is_running(self) -> bool:
        return self._running

    def has_workflow_executor(self) -> bool:
        return self.workflow_executor is not None

    def set_workflow_executor(
        self, workflow_executor: BaseWorkflowConfigExecutor
    ) -> None:
        """
        Set workflow executor.
        :param workflow_executor:
        :return:
        """
        self.workflow_executor = workflow_executor

    def get_workflows_registry(self) -> "WorkflowRegistry":
        """
        Get the system workflows registry
        Returns:
            workflow registry
        Raises:
            WorkflowExecutionError: trigger execution was not provided.
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
            WorkflowNotfound: Exception when workflow is not found
            ValueError: Trigger already registered
        """

        workflow = self.get_workflows_registry().get_workflow_config(
            trigger.workflow_name
        )
        if workflow is None:
            raise WorkflowNotfound(
                f"Workflow with name '{trigger.workflow_name}' was not found"
            )

        workflow.triggers.register(trigger, self._handle_activation)

        logger.info(f"Registered trigger: {trigger.trigger_id}")

    def fire_trigger(
        self,
        workflow_name: str,
        workflow_params: Dict[str, Any],
        trigger_type: "TriggerType",
    ) -> None:
        if not self.has_workflow_executor():
            raise RuntimeError("Workflow executor is not ready.")

        workflow_registry: Optional[WorkflowRegistry] = getattr(
            self.workflow_executor, "_workflow_registry", None
        )
        if workflow_registry is None:
            raise RuntimeError(
                "Workflow registry was not provided by workflow executor."
            )

        workflow_config = workflow_registry.get_workflow_config(workflow_name)
        if workflow_config is None:
            raise RuntimeError("Workflow config not found.")

    async def start(self):
        """Start all registered triggers and the consumer worker pool."""
        self._running = True
        logger.info(f"Starting trigger engine with {len(self.triggers)} triggers")

        for i in range(self.consumer_concurrency):
            worker = asyncio.create_task(self._workflow_consumer_worker(i))
            self._consumer_tasks.append(worker)

        logger.info(f"Started {self.consumer_concurrency} workflow consumer worker(s).")

        for trigger in self.triggers.values():
            try:
                await trigger.start()
                logger.info(f"Started trigger: {trigger.trigger_id}")
            except Exception as e:
                logger.error(f"Failed to start trigger {trigger.trigger_id}: {e}")
                trigger.lifecycle = TriggerLifecycle.ERROR

    async def stop(self):
        """Stop all triggers and the consumer worker pool."""
        self._running = False
        logger.info("Stopping trigger engine")

        for trigger in self.triggers.values():
            try:
                await trigger.stop()
            except Exception as e:
                logger.error(f"Failed to stop trigger {trigger.trigger_id}: {e}")

        for task in self._consumer_tasks:
            task.cancel()

        # Wait for all workers to finish execution
        await asyncio.gather(*self._consumer_tasks, return_exceptions=True)

        logger.info("Trigger engine and consumer pool stopped.")

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
        except Exception as e:
            # If putting on the queue fails (rare for asyncio.Queue)
            logger.error(f"Failed to enqueue workflow for {trigger.trigger_id}: {e}")
            trigger.error_count += 1

    async def _workflow_consumer_worker(self, worker_id: int):
        """
        Individual worker that pulls tasks from the queue and executes them.
        """
        if self.workflow_executor is None:
            logger.error("Workflow executor is not set for consumer worker.")
            return

        logger.debug(f"Consumer Worker {worker_id} started.")
        workflow_name = f"<unknown:{worker_id}>"
        while True:
            try:
                workflow_name, params = cast(
                    Tuple[str, Dict[str, Any]], await self.task_queue.get()
                )

                logger.info(f"Worker {worker_id} executing '{workflow_name}'")

                await self.workflow_executor(workflow_name, params)
                self.task_queue.task_done()
                logger.info(f"Worker {worker_id} finished '{workflow_name}'")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Worker {worker_id} execution failure for {workflow_name}: {e}"
                )

                # Ensure the queue item is still marked done
                self.task_queue.task_done()
