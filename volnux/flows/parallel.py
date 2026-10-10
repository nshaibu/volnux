import asyncio
import logging
from typing import List, Dict, Any, Type, TYPE_CHECKING
from volnux.executors import BaseExecutor
from volnux.flows.base import BaseFlow
from volnux.executors.utils.registry import get_global_executor_registry

logger = logging.getLogger(__name__)

_GLOBAL_EXECUTOR_REGISTRY = get_global_executor_registry()

if TYPE_CHECKING:
    from volnux import Event


class ParallelFlow(BaseFlow):
    """
    Represents a parallel execution flow where tasks and events can be processed
    concurrently.

    This class is responsible for managing event processing in a parallel flow,
    including the retrieval of executors, gathering event configurations, and
    executing events using parallel execution mechanisms. It is designed to support
    custom executor implementations and provide robust error handling.

    :ivar context: The context object containing configuration and state for the
        execution flow.
    :type context: Any
    :ivar task_profiles: A list of task profiles defining the events to be executed
        in the parallel flow.
    :type task_profiles: List[Any]
    """

    async def get_flow_executor(self, *args, **kwargs) -> Type[BaseExecutor]:
        return _GLOBAL_EXECUTOR_REGISTRY.get("process")

    def gather_parallel_events(
        self,
    ) -> Dict["Event", Dict[str, Any]]:
        from volnux import Event

        if not self.task_profiles:
            raise ValueError("No task profiles available for parallel execution")

        event_config = {}

        for task_profile in self.task_profiles:
            event, event_call_kwargs = self.get_initialized_event(task_profile)

            if not isinstance(event, Event):
                raise TypeError(
                    f"Expected Event instance from task profile "
                    f"{task_profile.get_id()}, got {type(event)}"
                )

            event_config[event] = event_call_kwargs or {}
            logger.debug(
                f"Gathered event {event.__class__.__name__} for parallel execution "
                f"(task_id={task_profile.get_id()})"
            )

        if not event_config:
            raise ValueError("No events gathered for parallel execution")

        logger.info(
            f"Gathered {len(event_config)} events as a unit for parallel execution"
        )

        return event_config

    async def run(self) -> asyncio.Future:
        event_config = None
        executor = None
        try:
            task_profile = self.context.get_decision_task_profile()

            executor_class, executor_config = await asyncio.gather(
                self.get_flow_executor(),
                self.get_flow_executor_config(task_profile),
                return_exceptions=True,
            )

            self.validate_executor_class_and_config(executor_class, executor_config)

            event_config = self.gather_parallel_events()

            logger.info(
                f"Starting parallel execution: {len(event_config)} events as a unit "
                f"using {executor_class.__name__} "
                f"(context: {self.context.state_id}, "
                f"decision_task: {task_profile.get_id()})"
            )

            with self._initialize_executor(executor_class, executor_config) as executor:
                future = await self._map_events_to_executor(
                    executor, event_execution_config=event_config
                )

            return future
        except ValueError as e:
            logger.error(f"Configuration error in ParallelFlow.run(): {e}")
            raise
        except RuntimeError as e:
            logger.error(f"Executor runtime error in ParallelFlow.run(): {e}")
            raise
        except Exception as e:
            event_names = []
            if event_config:
                event_names = [
                    event.__class__.__name__ for event in event_config.keys()
                ]

            logger.error(
                f"Unexpected error in ParallelFlow.run(): {e}\n"
                f"Context: {self.context.state_id}\n"
                f"Events: {', '.join(event_names) if event_names else 'N/A'}",
                exc_info=True,
            )
            raise RuntimeError(
                f"Failed to execute parallel events: {e}\n"
                f"Events: {', '.join(event_names) if event_names else 'N/A'}"
            ) from e
        finally:
            if executor:
                await self.shutdown_executor(executor)
