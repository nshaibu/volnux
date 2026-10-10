import asyncio
import logging
from typing import Any, Dict, List, Type, TYPE_CHECKING

from volnux.executors import BaseExecutor
from volnux.executors.utils.registry import get_global_executor_registry
from volnux.flows.base import BaseFlow
from volnux.mixins.event.checkpointing import SubPhaseSuspension

if TYPE_CHECKING:
    from volnux import Event

logger = logging.getLogger(__name__)

_GLOBAL_EXECUTOR_REGISTRY = get_global_executor_registry()


class ParallelFlow(BaseFlow):
    """Parallel execution flow for concurrent task processing.

    Executes all task profiles in the current decision group concurrently
    via a shared executor. Properly propagates SubPhaseSuspension from
    individual event futures to the ExecutionCoordinator.
    """

    async def get_flow_executor(self, *args: Any, **kwargs: Any) -> Type[BaseExecutor]:
        return _GLOBAL_EXECUTOR_REGISTRY.get("process")

    def gather_parallel_events(self) -> Dict["Event", Dict[str, Any]]:
        """Collect initialized events for all task profiles in this parallel group."""
        from volnux import Event

        if not self.task_profiles:
            raise ValueError("No task profiles available for parallel execution")

        event_config: Dict["Event", Dict[str, Any]] = {}
        for task_profile in self.task_profiles:
            event, event_call_kwargs = self.get_initialized_event(task_profile)

            if not isinstance(event, Event):
                raise TypeError(
                    f"Expected Event instance from task profile "
                    f"{task_profile.get_id()}, got {type(event)}"
                )

            event_config[event] = event_call_kwargs or {}
            logger.debug(
                "Gathered event %s for parallel execution (task_id=%s)",
                event.__class__.__name__,
                task_profile.get_id(),
            )

        if not event_config:
            raise ValueError("No events gathered for parallel execution")

        logger.info("Gathered %d events for parallel execution", len(event_config))
        return event_config

    async def run(self) -> Any:
        """Execute all parallel events and propagate SubPhaseSuspension correctly.

        Returns:
            List of results from completed events.

        Raises:
            SubPhaseSuspension: If any event suspends at a sub-phase boundary.
                MUST propagate to ExecutionCoordinator for proper suspension handling.
            ValueError/RuntimeError: For configuration or executor failures.
        """
        task_profile = self.context.get_decision_task_profile()

        # Resolve executor class and config concurrently
        executor_class, executor_config = await asyncio.gather(
            self.get_flow_executor(),
            self.get_flow_executor_config(task_profile),
            return_exceptions=True,
        )
        self.validate_executor_class_and_config(executor_class, executor_config)

        event_config = self.gather_parallel_events()

        logger.info(
            "Starting parallel execution: %d events using %s (context=%s, decision_task=%s)",
            len(event_config),
            executor_class.__name__,
            self.context.state_id,
            task_profile.get_id(),
        )

        # Initialize executor — tracked by BaseFlow for cleanup in close()
        executor = self._initialize_executor(executor_class, executor_config)

        try:
            # Get individual futures — NOT a gathered future
            futures = await self._map_events_to_executor(executor, event_config)

            # Inspect each future individually to propagate SubPhaseSuspension
            results: List[Any] = []
            for future in asyncio.as_completed(futures):
                try:
                    result = await future
                    results.append(result)
                except SubPhaseSuspension:
                    # Cancel remaining futures — no point continuing when suspended
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    raise  # Propagate to ExecutionCoordinator
                except Exception as e:
                    # Non-suspension errors are collected, not raised immediately
                    logger.error("Parallel event failed: %s", e, exc_info=True)
                    results.append(e)

            return results

        except (ValueError, RuntimeError):
            raise
        except SubPhaseSuspension:
            raise  # Re-raise without wrapping
        except Exception as e:
            event_names = [ev.__class__.__name__ for ev in event_config.keys()]
            logger.error(
                "Unexpected error in ParallelFlow.run(): %s\nContext: %s\nEvents: %s",
                e,
                self.context.state_id,
                ", ".join(event_names),
                exc_info=True,
            )
            raise RuntimeError(
                f"Failed to execute parallel events: {e}\n"
                f"Events: {', '.join(event_names)}"
            ) from e
        # NOTE: No finally shutdown here — BaseFlow.close() handles executor cleanup
