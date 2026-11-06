import abc
import inspect
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional, TYPE_CHECKING, Union


if TYPE_CHECKING:
    from volnux.engine.workflows import WorkflowRegistry

logger = logging.getLogger(__name__)


class WorkflowExecutor(abc.ABC):
    """What a trigger uses to run a workflow.

    Which one is used is the ``DEFAULT_WORKFLOW_EXECUTOR`` setting. An executor runs
    the workflow in this process (``AsyncTaskWorkflowExecutor``) or ships it to a
    Celery worker or a Kubernetes Job; wherever it runs, it runs under the
    ``volnux.context`` that was active when it was called, so the workflow sees the
    same configuration.
    """

    #: Whether ``pause_execution`` / ``resume_execution`` / ``stop_execution`` act on
    #: runs started by this executor. Executors that hand the workflow to another
    #: process (Celery, Kubernetes) cannot reach the running pipeline.
    supports_control: bool = False

    def __init__(self, workflow_registry: "WorkflowRegistry"):
        self._workflow_registry = workflow_registry
        self._listeners: List[
            Callable[[str, str, Optional[str]], Union[None, Awaitable[None]]]
        ] = []

    def get_workflow_registry(self) -> "WorkflowRegistry":
        return self._workflow_registry

    @abc.abstractmethod
    async def execute(
        self,
        workflow_name: str,
        params: Dict[str, Any],
        *,
        execution_id: Optional[str] = None,
    ) -> Any:
        """
        Executes a workflow by its name with the specified parameters. This method must
        be implemented by subclasses and is designed to handle asynchronous execution
        logic for workflows.

        :param workflow_name: The name of the workflow to be executed.
        :type workflow_name: str
        :param params: A dictionary containing the parameters for the workflow execution.
        :type params: Dict[str, Any]
        :param execution_id: The id the caller knows this run by (the platform's
            ``Execution`` record). Executors that can control runs use it as the run's
            id and report its end to the listeners; the others ignore it.
        :return: The result of the workflow execution.
        :rtype: Any
        """
        raise NotImplementedError("execute() must be implemented by subclasses.")

    async def __call__(
        self, workflow_name: str, params: Dict[str, Any], **kwargs: Any
    ) -> Any:
        return await self.execute(workflow_name, params, **kwargs)

    # --- control of running workflows ---------------------------------------------

    async def pause_execution(self, execution_id: str) -> bool:
        """Hold a running workflow before its next step. ``True`` if it was told to
        hold, ``False`` if no such run is live in this process.

        :raises NotImplementedError: this executor cannot control runs
            (``supports_control`` is false).
        """
        raise NotImplementedError(
            f"{type(self).__name__} cannot pause a running workflow"
        )

    async def resume_execution(self, execution_id: str) -> bool:
        """Release a workflow held by ``pause_execution`` (see there)."""
        raise NotImplementedError(
            f"{type(self).__name__} cannot resume a running workflow"
        )

    async def stop_execution(self, execution_id: str) -> bool:
        """Stop a running workflow (see ``pause_execution`` for the result)."""
        raise NotImplementedError(
            f"{type(self).__name__} cannot stop a running workflow"
        )

    # --- end-of-run notification ----------------------------------------------------

    def add_listener(
        self,
        callback: Callable[[str, str, Optional[str]], Union[None, Awaitable[None]]],
    ) -> None:
        """Call ``callback(execution_id, state, error)`` when a run started with an
        ``execution_id`` ends; ``state`` is ``completed``, ``failed`` or ``stopped``.
        The callback may be a coroutine function. Adding the same one twice is a no-op.
        """
        if callback not in self._listeners:
            self._listeners.append(callback)

    def remove_listener(self, callback: Callable[..., Any]) -> None:
        if callback in self._listeners:
            self._listeners.remove(callback)

    async def _notify(
        self, execution_id: str, state: str, error: Optional[str] = None
    ) -> None:
        for callback in list(self._listeners):
            try:
                result = callback(execution_id, state, error)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception(
                    "Execution listener failed for %s (%s)", execution_id, state
                )
