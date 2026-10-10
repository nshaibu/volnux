import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Union

from .base import TriggerBase, TriggerType


logger = logging.getLogger(__name__)

# Condition callable: may be sync or async, must return a truthy value.
ConditionFn = Callable[[], Union[bool, Awaitable[bool]]]


class ConditionalTrigger(TriggerBase):
    """
    Trigger that activates when a condition function becomes True.

    Activation mechanism
    --------------------
    A background asyncio task polls ``condition_fn`` every ``poll_interval``
    seconds. The first evaluation fires immediately on ``start()`` — there is
    no initial delay.

    Edge-detection (``edge_trigger=True``, default)
        Fires only on a ``False → True`` transition. Does *not* re-fire
        while the condition stays True.

    Level-trigger (``edge_trigger=False``)
        Fires on every poll tick where the condition is True.

    Usage
    -----
    trigger = ConditionalTrigger(
        workflow_name="generate_report",
        condition_fn=lambda: os.path.exists("/tmp/report_ready"),
        poll_interval=10,
        edge_trigger=True,
        workflow_params={"report_type": "monthly"}
    )
    """

    trigger_type = TriggerType.CONDITION

    def __init__(
        self,
        workflow_name: str,
        condition_fn: ConditionFn,
        poll_interval: float = 60.0,
        edge_trigger: bool = True,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            workflow_name,
            workflow_params=workflow_params,
            enabled=enabled,
            metadata=metadata,
        )
        self.condition_fn = condition_fn
        self.poll_interval = poll_interval
        self.edge_trigger = edge_trigger

        self._task: Optional[asyncio.Task] = None
        self._last_condition_state: bool = False

    async def start(self) -> None:
        """Start the polling loop (idempotent)."""
        if self._task and not self._task.done():
            logger.debug(
                "ConditionTrigger %s already polling; skipping start",
                self.trigger_id,
            )
            return

        logger.info(
            "ConditionTrigger %s starting (poll_interval=%.1fs, edge_trigger=%s)",
            self.trigger_id,
            self.poll_interval,
            self.edge_trigger,
        )
        self._task = asyncio.create_task(
            self._poll_loop(),
            name=f"ConditionTrigger.{self.trigger_id}",
        )

    async def stop(self) -> None:
        """Cancel the polling loop and await its completion."""
        logger.info("ConditionTrigger %s stopping", self.trigger_id)
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _poll_loop(self) -> None:
        """Evaluate the condition on every tick, starting immediately."""
        first_tick = True
        while True:
            try:
                if not first_tick:
                    await asyncio.sleep(self.poll_interval)
                first_tick = False

                if not self.enabled:
                    continue

                condition_met = await self._evaluate_condition()

                should_activate = condition_met and (
                    not self.edge_trigger or not self._last_condition_state
                )

                # Update the state before activating so that a re-entrant poll
                # (e.g. very fast activation callback) sees the correct state.
                self._last_condition_state = condition_met

                if should_activate:
                    await self.activate(
                        condition_met=True,
                        checked_at=datetime.now(timezone.utc).isoformat(),
                    )

            except asyncio.CancelledError:
                break
            except Exception:
                self.state.error_count += 1
                await self.state.save_async()
                logger.exception(
                    "ConditionTrigger %s: error during poll (total errors: %d)",
                    self.trigger_id,
                    self.state.error_count,
                )

    async def _evaluate_condition(self) -> bool:
        """
        Invoke ``condition_fn``, supporting sync and async callables.

        Returns ``False`` and logs on any exception so the poll loop
        continues rather than crashing.
        """
        try:
            result = self.condition_fn()
            if asyncio.iscoroutine(result):
                return bool(await result)
            return bool(result)
        except Exception:
            logger.exception(
                "ConditionTrigger %s: condition_fn raised", self.trigger_id
            )
            return False
