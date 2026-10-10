import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

from ..base import TriggerActivation, TriggerBase, TriggerType

logger = logging.getLogger(__name__)


class LinearChainTrigger(TriggerBase):
    """
    Trigger that activates only when a sequence of sub-triggers all fire in order.

    Activation mechanism
    --------------------
    Sub-triggers are armed one at a time. When trigger N fires, its activation
    data is accumulated, it is disarmed, and trigger N+1 is armed. Only when
    the final trigger fires is the workflow activated — with the merged data
    from every step.

    After each successful activation (or a step timeout) the chain resets
    automatically to step 0, making it suitable for recurring window patterns.

    Windowing example
    -----------------
    Replicating Apache Beam-style windowing with an event stream and a
    condition gate::

        # Shared buffer populated by the event handler.
        window: list[dict] = []

        event_trigger = EventTrigger(
            workflow_name="_chained_internal",
            event_bus=bus,
            event_types=["sensor.reading"],
        )

        condition_trigger = ConditionalTrigger(
            workflow_name="_chained_internal",
            condition_fn=lambda: len(window) >= 100,
            poll_interval=5.0,
            edge_trigger=True,
        )

        chained = TriggerChainTrigger(
            workflow_name="process_window",
            triggers=[event_trigger, condition_trigger],
            step_timeout=300.0, # reset if no condition fires within 5 min
        )

    Flow for the example above:

    1. ``event_trigger`` fires when the first sensor reading arrives.
       Activation data is stored as ``step_0_*`` keys.
    2. ``condition_trigger`` starts polling. When ``len(window) >= 100``
       becomes True the condition fires. Activation data is stored as
       ``step_1_*`` keys.
    3. The workflow ``"process_window"`` is triggered with the merged data
       from both steps.
    4. The chain resets and ``event_trigger`` is re-armed for the next window.

    Step timeout
    ------------
    ``step_timeout`` applies to every step *after* the first. If trigger N+1
    does not fire within ``step_timeout`` seconds of being armed, the chain
    resets to step 0 and the accumulated data is discarded. This prevents
    a chain started by a rare event from waiting indefinitely.

    Accumulated activation data
    ---------------------------
    Each step's ``workflow_params`` are merged into a single dict with
    step-prefixed keys (``step_0_event_id``, ``step_1_condition_met``, …)
    to avoid collisions between steps that produce identically named fields.

    Constraints
    -----------
    - At least two triggers are required (a single trigger does not need
      chaining).
    - Sub-triggers must not have an activation callback already set; the
      chain installs its own callbacks at arm time.
    - Sub-triggers should *not* be registered with the engine directly —
      the chain manages their full lifecycle.
    """

    trigger_type = TriggerType.WORKFLOW_CHAIN

    def __init__(
        self,
        workflow_name: str,
        triggers: List[TriggerBase],
        step_timeout: Optional[float] = None,
        reducer: Optional[Callable[[Dict, Dict], Dict]] = None,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if len(triggers) < 2:
            raise ValueError(
                "ChainedTrigger requires at least two triggers in the chain; "
                f"got {len(triggers)}."
            )

        super().__init__(
            workflow_name,
            workflow_params=workflow_params,
            enabled=enabled,
            metadata=metadata,
        )

        self._triggers: List[TriggerBase] = list(triggers)
        self.step_timeout = step_timeout
        self._reducer = reducer or (lambda a, b: {**a, **b})

        # Mutable chain state — all mutations are serialised by _lock.
        self._current_step: int = 0
        self._accumulated_data: Dict[str, Any] = {}
        self._timeout_task: Optional[asyncio.Task] = None
        self._running: bool = False
        self._lock: asyncio.Lock = asyncio.Lock()

    async def start(self) -> None:
        """Arm the first trigger in the chain."""
        logger.info(
            "ChainedTrigger '%s' starting (%d-step chain, step_timeout=%s)",
            self.trigger_id,
            len(self._triggers),
            f"{self.step_timeout:.1f}s" if self.step_timeout else "none",
        )
        self._running = True
        await self._arm_step(0)

    async def stop(self) -> None:
        """Disarm the currently active step and cancel any pending timeout."""
        logger.info(
            "ChainedTrigger '%s' stopping (was on step %d)",
            self.trigger_id,
            self._current_step,
        )
        self._running = False
        self._cancel_timeout()
        await self._disarm_step(self._current_step)

    async def _arm_step(self, step: int) -> None:
        """
        Install the chain callback on ``_triggers[step]`` and run it.

        The timeout is started for every step after the first so that the
        chain does not stall indefinitely if the next condition is never met.
        """
        self._current_step = step
        trigger = self._triggers[step]
        trigger.set_activation_callback(self._make_step_callback(step))
        await trigger.run()

        logger.debug(
            "ChainedTrigger '%s': armed step %d/%d (%s)",
            self.trigger_id,
            step + 1,
            len(self._triggers),
            type(trigger).__name__,
        )

        if self.step_timeout is not None and step > 0:
            self._timeout_task = asyncio.create_task(
                self._run_timeout(step),
                name=f"ChainedTrigger.{self.trigger_id}.timeout.step{step}",
            )

    async def _disarm_step(self, step: int) -> None:
        """
        Stop ``_triggers[step]``, swallowing errors so the chain can always
        recover (e.g. reset after a failed activation callback).
        """
        try:
            await self._triggers[step].end()
        except Exception:
            logger.exception(
                "ChainedTrigger '%s': error while stopping step %d (%s); continuing.",
                self.trigger_id,
                step,
                type(self._triggers[step]).__name__,
            )

    def _make_step_callback(
        self, step: int
    ) -> Callable[[TriggerActivation], Awaitable[None]]:
        """
        Return the async activation callback for ``_triggers[step]``.

        The callback schedules ``_advance_chain`` as a *new* asyncio task
        rather than awaiting it inline. This is critical: triggers such as
        ``ConditionalTrigger`` fire from within their own internal task
        (``_poll_loop``). If ``_advance_chain`` were awaited inline it would
        call ``end()`` → ``stop()`` → ``task.cancel()`` + ``await task`` from
        *within* that task, causing a deadlock. Scheduling a new task lets the
        callback return immediately so the trigger's task exits cleanly.
        """

        async def callback(activation: TriggerActivation) -> None:
            asyncio.create_task(
                self._advance_chain(step, activation),
                name=f"ChainedTrigger.{self.trigger_id}.advance.step{step}",
            )

        return callback

    async def _advance_chain(self, step: int, activation: TriggerActivation) -> None:
        """
        Process a completed step: accumulate its data, disarm it, then either
        arm the next step or fire the final workflow activation.

        The lock serialises concurrent activations (e.g. a high-frequency
        event bus delivering two events before the chain has advanced past
        step 0). The step guard discards any activation that arrives after the
        chain has already moved on.
        """
        async with self._lock:
            if not self._running:
                logger.debug(
                    "ChainedTrigger '%s': activation from step %d received after "
                    "stop(); discarding.",
                    self.trigger_id,
                    step,
                )
                return

            if step != self._current_step:
                logger.debug(
                    "ChainedTrigger '%s': stale activation from step %d "
                    "(current step: %d) — discarding.",
                    self.trigger_id,
                    step,
                    self._current_step,
                )
                return

            # Namespace each step's keys to prevent collisions.
            for key, value in activation.workflow_params.items():
                self._accumulated_data[f"step_{step}_{key}"] = value

            self._cancel_timeout()
            await self._disarm_step(step)

            next_step = step + 1

            reduced_state = self._reducer(
                self._accumulated_data, activation.workflow_params
            )
            if asyncio.iscoroutine(reduced_state):
                reduced_state = await reduced_state

            if next_step < len(self._triggers):
                # Chain incomplete — arm the next step.
                self._triggers[next_step].update_workflow_params(reduced_state)
                await self._arm_step(next_step)
                return

            logger.info(
                "ChainedTrigger '%s': all %d steps satisfied — activating workflow.",
                self.trigger_id,
                len(self._triggers),
            )

            accumulated = dict(self._accumulated_data)
            self._reset_state()

            activation_data = {**reduced_state, "__trigger_history__": accumulated}

            try:
                await self.activate(**activation_data)
            finally:
                # Re-arm for the next window even if the workflow callback
                # raised, so the chain stays live.
                if self._running:
                    await self._arm_step(0)

    async def _run_timeout(self, step: int) -> None:
        """
        Wait ``step_timeout`` seconds, then reset the chain if ``step`` is
        still the active step.

        Cancellation (via ``_cancel_timeout``) is the normal exit path when
        the step fires before the deadline.
        """
        try:
            await asyncio.sleep(self.step_timeout)
        except asyncio.CancelledError:
            return

        async with self._lock:
            if not self._running or self._current_step != step:
                return

            logger.warning(
                "ChainedTrigger '%s': step %d timed out after %.1fs — "
                "resetting chain to step 0.",
                self.trigger_id,
                step,
                self.step_timeout,
            )

            await self._disarm_step(step)
            self._reset_state()

            if self._running:
                await self._arm_step(0)

    def _reset_state(self) -> None:
        """Clear accumulated data and return the step counter to zero."""
        self._accumulated_data.clear()
        self._current_step = 0

    def _cancel_timeout(self) -> None:
        """Cancel the in-flight timeout task if one exists."""
        if self._timeout_task and not self._timeout_task.done():
            self._timeout_task.cancel()
        self._timeout_task = None
