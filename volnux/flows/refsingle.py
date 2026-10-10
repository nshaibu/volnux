import asyncio
import logging
from typing import Optional, Type, TYPE_CHECKING

from volnux.executors import BaseExecutor
from .base import BaseFlow

if TYPE_CHECKING:
    from volnux.parser.protocols import TaskProtocol

logger = logging.getLogger(__name__)


class SingleFlow(BaseFlow):
    """Execution flow for a single event/task.

    Returns a raw future to the ExecutionCoordinator. Result resolution,
    error routing, and SubPhaseSuspension handling are managed downstream
    by ResultProcessor and the Coordinator.
    """

    task_profile: Optional["TaskProtocol"] = None

    def __post_init__(self, *args, **kwargs) -> None:
        super().__post_init__(*args, **kwargs)
        if not self.task_profiles:
            raise ValueError("SingleFlow requires at least one task profile")
        self.task_profile = self.task_profiles[0]

    async def get_flow_executor(self, *args, **kwargs) -> Type[BaseExecutor]:
        """Resolve executor from task options or event class default."""
        executor_class = self.get_task_executor_from_options(self.task_profile)
        if executor_class is not None:
            return executor_class
        event_class = self.task_profile.get_event_class()
        return event_class.get_task_executor()

    async def run(self) -> asyncio.Future:
        """Execute the single event and return a raw future.

        The coordinator passes this future to ResultProcessor.process_futures().
        Executor cleanup is handled by BaseFlow.close() — NOT here.

        Returns:
            Future representing the event execution result.

        Raises:
            ValueError: If executor or config is invalid.
            RuntimeError: If submission or execution fails critically.
        """
        logger.debug(
            "Starting sequential execution flow for task: %s", self.task_profile
        )

        executor_class, executor_config = await asyncio.gather(
            self.get_flow_executor(self.task_profile),
            self.get_flow_executor_config(self.task_profile),
            return_exceptions=True,
        )
        self.validate_executor_class_and_config(executor_class, executor_config)

        event, event_call_kwargs = self.get_initialized_event(self.task_profile)

        # Executor tracked by BaseFlow._flow_executors; cleaned up in BaseFlow.close()
        executor = self._initialize_executor(executor_class, executor_config)

        try:
            return await self._submit_event_to_executor(
                executor, event, event_call_kwargs
            )
        except (ValueError, RuntimeError):
            raise
        except Exception as e:
            logger.error(
                "Unexpected error in SingleFlow.run(): %s\nContext: %s\nTask: %s",
                e,
                self.context.state_id,
                self.task_profile,
                exc_info=True,
            )
            raise RuntimeError(
                f"Failed to execute event: {e}\nTask: {self.task_profile}"
            ) from e
        # NOTE: No finally shutdown — BaseFlow.close() is the single lifecycle owner
