import asyncio
import typing
import logging

from .base import BaseFlow
from pydantic_mini import MiniAnnotated, Attrib

from volnux import Event
from volnux.executors import (
    BaseExecutor,
    DefaultExecutor,
    ThreadPoolExecutor,
    ProcessPoolExecutor,
)
from volnux.task import PipelineTask
from volnux.result import EventResult

logger = logging.getLogger(__name__)


class MetaFlow(BaseFlow):
    attributes: MiniAnnotated[
        typing.Dict[str, typing.Any], Attrib(default_factory=dict)
    ]

    async def get_flow_executor(
        self, *args: typing.Any, **kwargs: typing.Dict[str, typing.Any]
    ) -> typing.Type[BaseExecutor]:
        concurrent = self.attributes.get("concurrent", False)
        if not concurrent:
            return DefaultExecutor
        concurrency_mode = self.attributes.get("concurrency_mode", "thread").lower()
        if concurrency_mode not in ("thread", "process"):
            return ThreadPoolExecutor  # Making thread work-pools for now

        if concurrency_mode == "thread":
            return ThreadPoolExecutor
        return ProcessPoolExecutor

    def gather_multiple_events(
        self,
    ) -> typing.Dict[Event, typing.Dict[str, typing.Any]]:
        event_config = {}
        for task_profile in self.task_profiles:
            event, event_call_kwargs = self.get_initialized_event(task_profile)
            if isinstance(event, Event):
                event_config[event] = event_call_kwargs or {}

        return event_config

    async def run(self) -> asyncio.Future:
        """
        Execute the flow until it completes.
        Returns:
            Future object representing the result of the flow.
        Exceptions:
            ValueError: if the executor or the execution config is invalid.
            RuntimeError: if the event submission or execution fails,
            BrokenPipeError: if the internal queue of the executor is broken.
        """
        try:
            executor_class = typing.cast(
                typing.Type[BaseExecutor], await self.get_flow_executor()
            )

            if isinstance(executor_class, DefaultExecutor):
                executor_init = executor_class()  # for sequential execution
            else:
                executor_init = executor_class(max_workers=self.attributes.get("max_workers", min(4, len(self.task_profiles))))  # type: ignore

            event_config = self.gather_multiple_events()

            with executor_init as executor:
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
            raise RuntimeError(f"Failed to execute event: {e}")

    def _should_execute_concurrent(self) -> bool:
        """Check if tasks should execute concurrently"""
        if self.attributes:
            return self.attributes.get("concurrent", False)
        return False
