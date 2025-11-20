import asyncio
import logging
import typing

from volnux import Event
from volnux.base import ExecutorInitializerConfig
from volnux.executors import BaseExecutor, ProcessPoolExecutor
from volnux.utils import is_multiprocessing_executor

from .base import BaseFlow

logger = logging.getLogger(__name__)


class ParallelFlow(BaseFlow):
    """Class for parallel execution flows"""

    @staticmethod
    def get_multiprocessing_executor(
        executors: typing.List[typing.Type[BaseExecutor]],
    ) -> typing.Optional[typing.Type[BaseExecutor]]:
        """Get executor that support parallel execution."""
        for executor in executors:
            if is_multiprocessing_executor(executor):
                return executor
        return None

    async def _get_executors_from_task_profiles_options(
        self,
    ) -> typing.List[typing.Type[BaseExecutor]]:
        """Get the executor for this task from the configured options."""
        executors = await asyncio.gather(
            *[
                self.get_task_executor_from_options(task_profile)
                for task_profile in self.task_profiles
            ],
            return_exceptions=True,
        )
        if typing.TYPE_CHECKING:
            executors = typing.cast(
                typing.List[typing.Optional[typing.Type[BaseExecutor]]], executors
            )
        return [
            executor
            for executor in executors
            if executor is not None and issubclass(executor, BaseExecutor)
        ]

    async def get_flow_executor(self, *args, **kwargs) -> typing.Type[BaseExecutor]:
        """Get the executor for this task"""
        executors = await self._get_executors_from_task_profiles_options()
        if executors:
            executor = self.get_multiprocessing_executor(executors)
            if executor:
                return executor

        for task_profile in self.task_profiles:
            event_class = task_profile.get_event_class()
            executor = event_class.get_executor_class()
            if is_multiprocessing_executor(executor):
                return executor

        logger.debug("No valid parallel executor found, using default executor")
        return ProcessPoolExecutor

    def gather_parallel_events(
        self,
    ) -> typing.Dict[Event, typing.Dict[str, typing.Any]]:
        event_config = {}
        for task_profile in self.task_profiles:
            event, event_call_kwargs = self.get_initialized_event(task_profile)
            if isinstance(event, Event):
                event_config[event] = event_call_kwargs or {}

        return event_config

    async def run(self) -> asyncio.Future:
        try:
            task_profile = self.context.get_decision_task_profile()

            executor_class, executor_config = await asyncio.gather(
                self.get_flow_executor(),
                self.get_flow_executor_config(task_profile),
                return_exceptions=True,
            )

            if typing.TYPE_CHECKING:
                executor_class = typing.cast(typing.Type[BaseExecutor], executor_class)
                executor_config = typing.cast(
                    ExecutorInitializerConfig, executor_config
                )

            self.validate_executor_class_and_config(executor_class, executor_config)

            event_config = self.gather_parallel_events()

            config = self.parse_executor_initialisation_configuration(
                executor_class, executor_config
            )

            with executor_class(**config) as executor:
                future = await self._map_events_to_executor(
                    executor, event_execution_config=event_config
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
            raise RuntimeError(f"Failed to execute events: {e}")
