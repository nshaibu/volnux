import logging
import asyncio
from datetime import datetime
from typing import Optional, Callable

from .base import TriggerBase, TriggerLifecycle, TriggerType


logger = logging.getLogger(__name__)


class ConditionTrigger(TriggerBase):
    """
    Trigger that activates when a condition becomes true.

    Activation mechanism: Periodic polling of condition function.
    No event bus needed!
    """

    def __init__(
        self,
        workflow_name: str,
        condition_fn: Callable[[], bool],
        poll_interval: int = 60,  # seconds
        debounce: bool = True,  # Prevent rapid re-triggering
        **kwargs,
    ):
        super().__init__(workflow_name, **kwargs)
        self.condition_fn = condition_fn
        self.poll_interval = poll_interval
        self.debounce = debounce
        self._task: Optional[asyncio.Task] = None
        self._last_condition_state = False

    async def start(self):
        """Start polling the condition."""
        logger.info(
            f"ConditionTrigger {self.trigger_id} starting "
            f"(poll interval: {self.poll_interval}s)"
        )

        self._task = asyncio.create_task(self._poll_loop())
        self.lifecycle = TriggerLifecycle.ACTIVE

    async def stop(self):
        """Stop polling."""
        logger.info(f"ConditionTrigger {self.trigger_id} stopping")

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        self.lifecycle = TriggerLifecycle.STOPPED

    async def _poll_loop(self):
        """Poll condition periodically."""
        while True:
            try:
                await asyncio.sleep(self.poll_interval)

                if not self.enabled:
                    continue

                # Evaluate condition
                condition_met = await self._evaluate_condition()

                # With debounce: only trigger on transition from False to True
                # Without debounce: trigger every time condition is True
                should_activate = condition_met and (
                    not self.debounce or not self._last_condition_state
                )

                if should_activate:
                    await self.activate(
                        condition_met=True, checked_at=datetime.now().isoformat()
                    )

                self._last_condition_state = condition_met

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Condition polling error: {e}")
                self.error_count += 1

    async def _evaluate_condition(self) -> bool:
        """Evaluate the condition function."""
        try:
            if asyncio.iscoroutinefunction(self.condition_fn):
                return await self.condition_fn()
            else:
                return self.condition_fn()
        except Exception as e:
            logger.error(f"Condition evaluation failed: {e}")
            return False

    def get_activation_source(self) -> TriggerType:
        return TriggerType.CONDITION
