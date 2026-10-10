import asyncio
import logging
import typing
from typing import Any, Dict, Type

from formax import MiniAnnotated, Attrib

from volnux import Event
from volnux.executors import (
    BaseExecutor,
    DefaultExecutor,
    ThreadPoolExecutor,
    ProcessPoolExecutor,
)
from .base import BaseFlow

logger = logging.getLogger(__name__)


class MetaFlow(BaseFlow):
    """Execution flow for MAP/REDUCE/FILTER meta-events.

    Supports both sequential (DefaultExecutor) and concurrent
    (ThreadPoolExecutor/ProcessPoolExecutor) execution modes based
    on meta-event attributes. Returns raw futures to the coordinator.
    """

    attributes: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]

    async def get_flow_executor(self, *args: Any, **kwargs: Any) -> Type[BaseExecutor]:
        """Resolve executor class based on meta-event concurrency attributes."""
        if not self.attributes.get("concurrent", False):
            return DefaultExecutor

        concurrency_mode = self.attributes.get("concurrency_mode", "thread").lower()
        if concurrency_mode == "process":
            return ProcessPoolExecutor
        return ThreadPoolExecutor

    def gather_multiple_events(self) -> Dict[Event, Dict[str, Any]]:
        """Collect initialized events for all task profiles in this meta-event group."""
        event_config: Dict[Event, Dict[str, Any]] = {}
        for task_profile in self.task_profiles:
            event, event_call_kwargs = self.get_initialized_event(task_profile)
            if isinstance(event, Event):
                event_config[event] = event_call_kwargs or {}
            else:
                logger.warning(
                    "Skipping non-Event object from task profile %s: %s",
                    task_profile.get_id(),
                    type(event).__name__,
                )

        if not event_config:
            raise ValueError("No valid events gathered for meta-event execution")

        return event_config

    async def run(self) -> asyncio.Future:
        """Execute meta-event tasks and return raw future(s).

        Sequential mode uses DefaultExecutor (inline execution).
        Concurrent mode uses ThreadPoolExecutor or ProcessPoolExecutor.
        Executor lifecycle managed by BaseFlow.close().

        Returns:
            Future representing the aggregated result of all meta-event tasks.

        Raises:
            ValueError: If no events gathered or config is invalid.
            RuntimeError: If submission or execution fails critically.
        """
        executor_class = typing.cast(Type[BaseExecutor], await self.get_flow_executor())
        event_config = self.gather_multiple_events()

        logger.info(
            "Starting meta-event execution: %d events using %s (concurrent=%s)",
            len(event_config),
            executor_class.__name__,
            self.attributes.get("concurrent", False),
        )

        # Build executor config with appropriate max_workers for concurrent executors
        if executor_class is DefaultExecutor:
            executor_config = None
        else:
            max_workers = self.attributes.get(
                "max_workers", min(4, len(self.task_profiles))
            )
            executor_config = type("ExecutorConfig", (), {"max_workers": max_workers})()

        # Initialize via BaseFlow — tracks in _flow_executors for cleanup in close()
        executor = self._initialize_executor(
            executor_class, executor_config  # type: ignore[arg-type]
        )

        try:
            return await self._map_events_to_executor(executor, event_config)
        except (ValueError, RuntimeError):
            raise
        except Exception as e:
            event_names = [ev.__class__.__name__ for ev in event_config.keys()]
            logger.error(
                "Unexpected error in MetaFlow.run(): %s\nEvents: %s",
                e,
                ", ".join(event_names),
                exc_info=True,
            )
            raise RuntimeError(
                f"Failed to execute meta-event: {e}\nEvents: {', '.join(event_names)}"
            ) from e
        # NOTE: No manual shutdown — BaseFlow.close() is the single lifecycle owner
