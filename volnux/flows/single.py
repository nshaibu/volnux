import asyncio
import inspect
import logging
from typing import Optional, Type, TYPE_CHECKING
from volnux.executors import BaseExecutor

from .base import BaseFlow

if TYPE_CHECKING:
    from volnux.parser.protocols import TaskProtocol


logger = logging.getLogger(__name__)


class SingleFlow(BaseFlow):
    """Setup for the execution flow of a single event"""

    task_profile: Optional["TaskProtocol"] = None

    def __post_init__(self, *args, **kwargs) -> None:
        super().__post_init__(*args, **kwargs)
        self.task_profile = self.task_profiles[0]

    async def get_flow_executor(self, *args, **kwargs) -> Type[BaseExecutor]:
        """
        Get the executor class for this flow.
        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        Returns:
            The executor class.
        """
        executor_class = self.get_task_executor_from_options(self.task_profile)
        if executor_class is not None:
            return executor_class
        event_class = self.task_profile.get_event_class()
        return event_class.get_task_executor()

    async def run(self) -> asyncio.Future:
        """
        Execute the flow until it completes.
        Returns:
            Future object representing the result of the flow.
        Exceptions:
            ValueError: if the executor or the execution config is invalid.
            RuntimeError: if the event submission or execution fails
            BrokenPipeError: if the internal queue of the executor is broken.
        """
        executor = None
        try:
            logger.debug(
                f"Starting sequential execution flow for task: {self.task_profile}"
            )

            executor_class, executor_config = await asyncio.gather(
                self.get_flow_executor(self.task_profile),
                self.get_flow_executor_config(self.task_profile),
                return_exceptions=True,
            )

            self.validate_executor_class_and_config(executor_class, executor_config)

            event, event_call_kwargs = self.get_initialized_event(self.task_profile)

            executor = self._initialize_executor(executor_class, executor_config)

            future = await self._submit_event_to_executor(
                executor, event, event_call_kwargs
            )

            return future
        except ValueError as e:
            logger.error(f"Configuration error in SingleFlow.run(): {e}")
            raise
        except RuntimeError as e:
            logger.error(f"Executor runtime error in SingleFlow.run(): {e}")
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error in SingleFlow.run(): {e}\n"
                f"Context: {self.context.state_id}\n"
                f"Events: {self.context.task_profiles}",
                exc_info=True,
            )
            raise RuntimeError(
                f"Failed to execute events: {e}\n"
                f"Events: {self.context.task_profiles}"
            ) from e
        finally:
            if executor:
                await self.shutdown_executor(executor)
