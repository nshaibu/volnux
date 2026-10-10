import logging
import asyncio
import abc
from typing import Dict, Any, TYPE_CHECKING


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
