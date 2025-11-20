import asyncio
import logging
import typing

from volnux.base import ExecutorInitializerConfig
from volnux.executors import BaseExecutor

from .base import BaseFlow

if typing.TYPE_CHECKING:
    from volnux.parser.protocols import TaskProtocol


logger = logging.getLogger(__name__)


class SingleFlow(BaseFlow):
    """Setup for execution flow of a single event"""

    task_profile: typing.Optional["TaskProtocol"] = None

    def __model_init__(self, *args, **kwargs) -> None:
        super().__model_init__(*args, **kwargs)
        self.task_profile = self.task_profiles[0]

    async def get_flow_executor(self, *args, **kwargs) -> typing.Type[BaseExecutor]:
        """
        Get the executor class for this flow.
        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        Returns:
            The executor class.
        """
        executor_class = await self.get_task_executor_from_options(self.task_profile)
        if executor_class is not None:
            return executor_class
        event_class = self.task_profile.get_event_class()
        return event_class.get_executor_class()

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
        try:
            executor_class, executor_config = await asyncio.gather(
                self.get_flow_executor(self.task_profile),
                self.get_flow_executor_config(self.task_profile),
                return_exceptions=True,
            )

            self.validate_executor_class_and_config(executor_class, executor_config)

            event, event_call_kwargs = self.get_initialized_event(self.task_profile)

            if typing.TYPE_CHECKING:
                executor_class = typing.cast(typing.Type[BaseExecutor], executor_class)
                executor_config = typing.cast(
                    ExecutorInitializerConfig, executor_config
                )

            config = self.parse_executor_initialisation_configuration(
                executor_class, executor_config
            )

            with executor_class(**config) as executor:
                future = await self._submit_event_to_executor(
                    executor, event, event_call_kwargs
                )

            return future
        except ValueError as e:
            logger.error(f"Configuration error in run(): {e}")
            raise
        except RuntimeError as e:
            logger.error(f"Executor runtime error in run(): {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in run(): {e}")
            raise RuntimeError(f"Failed to execute event: {e}")
