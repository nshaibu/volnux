"""
WindowedTrigger — parallel multi-source accumulation with a condition-gated sink.

Architecture
------------

                  ┌─────────────────────────────┐
                  │        WindowedTrigger        │
                  │                               │
  Event bus ──►  │  aggregator["trades"]  ──►   │
                  │  aggregator["quotes"]  ──►   │  window_state
                  │  aggregator["news"]    ──►   │       │
                  │                               │       ▼
                  │  sink (ConditionTrigger) ◄── │  condition_fn(state)
                  │        fires workflow          │
                  └─────────────────────────────┘

All aggregators are armed *concurrently* at start time. Each one writes
incoming activation data into its named slot in ``window_state`` rather than
activating the workflow directly. The sink polls ``condition_fn(window_state)``
and fires the workflow when the condition is met, passing the full accumulated
state to the workflow.

After firing (or a window timeout) the trigger resets: ``window_state`` is
cleared, a new ``window_epoch`` is stamped, and all aggregators + the sink
are re-armed for the next window.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

from ..base import TriggerActivation, TriggerBase, TriggerType
from ..event import EventTrigger
from ...event_filters.timestamp import TimestampFilter


logger = logging.getLogger(__name__)

# Sink condition: receives the live window_state dict and returns bool (or awaitable bool).
SinkConditionFn = Callable[[Dict[str, List[Any]]], Union[bool, Awaitable[bool]]]

# ── Internal sentinel workflow name for sub-triggers that never directly fire. ──
_INTERNAL_WORKFLOW = "_windowed_internal"


class WindowSink(TriggerBase):
    """
    Polling sink for ``WindowedTrigger``.

    Behaves like ``ConditionalTrigger`` but its ``condition_fn`` receives the
    live ``window_state`` dict instead of taking no arguments. The
    ``WindowedTrigger`` injects the state-reader via ``_inject_state_reader``
    before arming the sink each window cycle.

    Parameters
    ----------
    condition_fn:
        Callable receiving ``Dict[str, List[Any]]`` and returning ``bool``
        (or an awaitable bool). The window closes when it returns ``True``.
    poll_interval:
        Seconds between condition evaluations. Defaults to 1.0.
    edge_trigger:
        If ``True`` (default), the sink fires only on the ``False → True``
        transition. Prevents double-firing if the condition stays True across
        multiple poll ticks before the window can be disarmed.

    Example
    -------
    ::

        sink = WindowSink(
            condition_fn=lambda state: (
                len(state.get("trades", [])) >= 500 and
                len(state.get("quotes", [])) >= 500
            ),
            poll_interval=1.0,
        )
    """

    trigger_type = TriggerType.CONDITION

    def __init__(
        self,
        condition_fn: SinkConditionFn,
        poll_interval: float = 1.0,
        edge_trigger: bool = True,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        # WindowSink is always owned by a WindowedTrigger; workflow_name is
        # a dummy value — the parent trigger drives the actual workflow activation.
        super().__init__(
            _INTERNAL_WORKFLOW,
            workflow_params=workflow_params,
            enabled=enabled,
            metadata=metadata,
        )
        self.condition_fn: SinkConditionFn = condition_fn
        self.poll_interval = poll_interval
        self.edge_trigger = edge_trigger

        self._task: Optional[asyncio.Task] = None
        self._last_state: bool = False
        # Injected by WindowedTrigger before each window cycle.
        self._state_reader: Optional[Callable[[], Dict[str, List[Any]]]] = None

    def _inject_state_reader(self, reader: Callable[[], Dict[str, List[Any]]]) -> None:
        """Called by WindowedTrigger before arming this sink each cycle."""
        self._state_reader = reader

        # Reset edge-detection state for the new window.
        self._last_state = False

    async def start(self) -> None:
        """Start the polling loop (idempotent)."""
        if self._task and not self._task.done():
            logger.debug(
                "WindowSink %s already polling; skipping start", self.trigger_id
            )
            return
        logger.debug(
            "WindowSink %s starting (poll_interval=%.1fs)",
            self.trigger_id,
            self.poll_interval,
        )
        self._task = asyncio.create_task(
            self._poll_loop(),
            name=f"WindowSink.{self.trigger_id}",
        )

    async def stop(self) -> None:
        """Cancel the polling loop."""
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _poll_loop(self) -> None:
        first_tick = True
        while True:
            try:
                if not first_tick:
                    await asyncio.sleep(self.poll_interval)
                first_tick = False

                if not self.enabled or self._state_reader is None:
                    continue

                condition_met = await self._evaluate()

                should_fire = condition_met and (
                    not self.edge_trigger or not self._last_state
                )
                self._last_state = condition_met

                if should_fire:
                    await self.activate(sink_fired=True)

            except asyncio.CancelledError:
                break
            except Exception:
                self.state.error_count += 1
                await self.state.save_async()
                logger.exception(
                    "WindowSink %s: error during poll (total errors: %d)",
                    self.trigger_id,
                    self.state.error_count,
                )

    async def _evaluate(self) -> bool:
        state = self._state_reader()
        try:
            result = self.condition_fn(state)
            if asyncio.iscoroutine(result):
                return bool(await result)
            return bool(result)
        except Exception:
            logger.exception("WindowSink %s: condition_fn raised", self.trigger_id)
            return False


class WindowedTrigger(TriggerBase):
    """
    Trigger that accumulates events from multiple concurrent sources and fires
    a workflow when a condition over the combined window state is met.

    This implements the three stream-processing primitives in a single trigger:

    - **Ingestion** — each aggregator subscribes to its event types and
      accumulates arriving events into a named buffer.
    - **Accumulation** — ``window_state[name]`` holds all activation data
      received by that aggregator since the window opened.
    - **Windowing** — the sink evaluates ``condition_fn(window_state)`` and
      closes the window when the condition is met.

    Parameters
    ----------
    workflow_name:
        Name of the workflow to activate when the sink fires.
    aggregators:
        List of ``EventTrigger`` instances. Each must have a ``name`` attribute
        that keys its slot in ``window_state``. Each aggregator may carry a
        ``TimestampFilter`` in its filter chain; ``WindowedTrigger`` stamps
        ``window_epoch`` on every ``TimestampFilter`` when a window opens.
    sink:
        A ``ConditionTrigger``-style trigger whose ``condition_fn`` receives
        the live ``window_state`` dict and returns ``True`` when the window
        should close.
    window_timeout:
        Optional seconds before an open window is forcibly closed and reset
        without firing the workflow. Prevents unbounded accumulation when
        inbound events stall.
    workflow_params:
        Static params merged into the activation data.
    enabled:
        Whether the trigger starts enabled.
    metadata:
        Arbitrary metadata attached to the trigger state record.

    Usage
    -----
    ::

        trades: list = []
        quotes: list = []

        chained = WindowedTrigger(
            workflow_name="process_market_window",
            aggregators=[
                EventTrigger(
                    name="trades",
                    workflow_name=_INTERNAL_WORKFLOW,
                    event_bus=bus,
                    event_types=["market.trade"],
                    event_filter=TimestampFilter(start=0, end=60),
                ),
                EventTrigger(
                    name="quotes",
                    workflow_name=_INTERNAL_WORKFLOW,
                    event_bus=bus,
                    event_types=["market.quote"],
                    event_filter=TimestampFilter(start=60, end=80),
                ),
            ],
            sink=WindowSink(
                condition_fn=lambda state: (
                    len(state.get("trades", [])) >= 500 and
                    len(state.get("quotes", [])) >= 500
                ),
                poll_interval=1.0,
            ),
            window_timeout=300.0,
        )

    Window state layout
    -------------------
    ``window_state`` is a ``Dict[str, List[Dict[str, Any]]]``.
    Each aggregator appends its ``activation.workflow_params`` as a single item::

        {
            "trades": [{"event_id": "...", "event_data": {...}}, ...],
            "quotes": [{"event_id": "...", "event_data": {...}}, ...],
        }

    The full ``window_state`` is passed to ``activate()`` under the key
    ``"window_state"``, along with ``"window_epoch"`` (ISO string) and
    ``"window_closed_at"`` (ISO string).

    Constraints
    -----------
    - At least one aggregator is required.
    - Aggregators must each have a ``name`` attribute (str).
    - Aggregators and the sink must not be registered with the engine directly.
    - Aggregators' ``workflow_name`` is ignored; it is overridden internally.
    """

    trigger_type = TriggerType.WORKFLOW_CHAIN

    def __init__(
        self,
        workflow_name: str,
        aggregators: List[EventTrigger],
        sink: "WindowSink",
        window_timeout: Optional[float] = None,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if not aggregators:
            raise ValueError("WindowedTrigger requires at least one aggregator.")
        for agg in aggregators:
            if not getattr(agg, "name", None):
                raise ValueError(
                    f"Every aggregator must have a non-empty 'name' attribute. "
                    f"Got: {agg!r}"
                )

        super().__init__(
            workflow_name,
            workflow_params=workflow_params,
            enabled=enabled,
            metadata=metadata,
        )

        self._aggregators: List[EventTrigger] = list(aggregators)
        self._sink: "WindowSink" = sink

        self.window_timeout = window_timeout

        # All mutations are serialised by _lock.
        self._window_state: Dict[str, List[Any]] = {
            agg.name: [] for agg in self._aggregators
        }
        self._window_epoch: Optional[datetime] = None
        self._timeout_task: Optional[asyncio.Task] = None
        self._running: bool = False
        self._lock: asyncio.Lock = asyncio.Lock()

    async def start(self) -> None:
        """Open the first window: stamp epoch, arm aggregators + sink."""
        logger.info(
            "WindowedTrigger '%s' starting (%d aggregator(s), timeout=%s)",
            self.trigger_id,
            len(self._aggregators),
            f"{self.window_timeout:.1f}s" if self.window_timeout else "none",
        )
        self._running = True
        await self._open_window()

    async def stop(self) -> None:
        """Disarm all aggregators and the sink; cancel any pending timeout."""
        logger.info("WindowedTrigger '%s' stopping", self.trigger_id)
        self._running = False
        self._cancel_timeout()
        await self._disarm_all()

    async def _open_window(self) -> None:
        """
        Stamp a fresh window epoch, reset state, arm aggregators + sink.

        Called on first start and after every window close (fire or timeout).
        """
        epoch = datetime.now(timezone.utc)
        self._window_epoch = epoch

        # Reset each aggregator's buffer.
        for name in self._window_state:
            self._window_state[name] = []

        # Stamp epoch on all TimestampFilters carried by the aggregators.
        self._stamp_epoch(epoch)

        # Inject a state-reader into the sink so condition_fn can observe
        # window_state without holding a reference to self.
        self._sink._inject_state_reader(self._read_window_state)

        # Arm aggregators concurrently — they all listen from window open.
        await asyncio.gather(*[self._arm_aggregator(agg) for agg in self._aggregators])

        # Arm the sink — it polls until the window condition is met.
        await self._arm_sink()

        # Start the window timeout if configured.
        if self.window_timeout is not None:
            self._timeout_task = asyncio.create_task(
                self._run_timeout(),
                name=f"WindowedTrigger.{self.trigger_id}.timeout",
            )

        logger.debug(
            "WindowedTrigger '%s': window opened at %s",
            self.trigger_id,
            epoch.isoformat(),
        )

    async def _close_window(self, *, timed_out: bool = False) -> None:
        """
        Close the current window: disarm everything, optionally fire the workflow.

        Args:
            timed_out: If True the window expired without the sink firing;
                       the workflow is NOT activated and the window resets silently.
        """
        self._cancel_timeout()
        await self._disarm_all()

        if timed_out:
            logger.warning(
                "WindowedTrigger '%s': window timed out after %.1fs — "
                "discarding %s events and resetting.",
                self.trigger_id,
                self.window_timeout,
                {k: len(v) for k, v in self._window_state.items()},
            )
        else:
            closed_at = datetime.now(timezone.utc).isoformat()
            snapshot = {name: list(items) for name, items in self._window_state.items()}

            logger.info(
                "WindowedTrigger '%s': window closed — activating workflow "
                "with %s events.",
                self.trigger_id,
                {k: len(v) for k, v in snapshot.items()},
            )

            try:
                await self.activate(
                    window_state=snapshot,
                    window_epoch=self._window_epoch.isoformat(),
                    window_closed_at=closed_at,
                )
            except Exception:
                logger.exception(
                    "WindowedTrigger '%s': activation callback raised; "
                    "re-arming for next window.",
                    self.trigger_id,
                )

        # Re-arm for the next window (regardless of success/timeout).
        if self._running:
            await self._open_window()

    async def _arm_aggregator(self, aggregator: EventTrigger) -> None:
        """Install the write-only accumulation callback and start the aggregator."""
        aggregator.set_activation_callback(
            self._make_aggregator_callback(aggregator.workflow_name)
        )
        try:
            await aggregator.run()
        except Exception:
            logger.exception(
                "WindowedTrigger '%s': failed to arm aggregator '%s'.",
                self.trigger_id,
                aggregator.workflow_name,
            )

    def _make_aggregator_callback(
        self, slot: str
    ) -> Callable[[TriggerActivation], Awaitable[None]]:
        """
        Return a callback that appends an aggregator's activation data to
        ``window_state[slot]``.

        This callback does *not* advance any chain or fire the workflow — it
        only accumulates. The lock is held briefly to keep appends thread-safe
        against concurrent aggregators.
        """

        async def callback(activation: TriggerActivation) -> None:
            if not self._running:
                return
            async with self._lock:
                self._window_state[slot].append(activation.workflow_params)
            logger.debug(
                "WindowedTrigger '%s': aggregator '%s' appended event "
                "(slot size: %d)",
                self.trigger_id,
                slot,
                len(self._window_state[slot]),
            )

        return callback

    async def _arm_sink(self) -> None:
        """Install the sink callback and start it."""
        self._sink.set_activation_callback(self._on_sink_fired)
        try:
            await self._sink.run()
        except Exception:
            logger.exception(
                "WindowedTrigger '%s': failed to arm sink.", self.trigger_id
            )

    async def _on_sink_fired(self, _activation: TriggerActivation) -> None:
        """
        Called when the sink's condition is met.

        Scheduled as a new task (not awaited inline) for the same deadlock
        reason as LinearChainedTrigger: the sink fires from inside its own
        poll loop, and closing the window calls ``sink.end()`` which cancels
        that loop. Scheduling avoids awaiting a cancel from inside the task
        being cancelled.
        """
        asyncio.create_task(
            self._handle_sink_fired(),
            name=f"WindowedTrigger.{self.trigger_id}.sink_fired",
        )

    async def _handle_sink_fired(self) -> None:
        async with self._lock:
            if not self._running:
                return
        await self._close_window(timed_out=False)

    async def _disarm_all(self) -> None:
        """Stop all aggregators and the sink, swallowing errors."""
        targets = [*self._aggregators, self._sink]
        await asyncio.gather(
            *[self._safe_end(t) for t in targets],
            return_exceptions=True,
        )

    @staticmethod
    async def _safe_end(trigger: TriggerBase) -> None:
        try:
            await trigger.end()
        except Exception:
            logger.exception(
                "WindowedTrigger: error while stopping %s; continuing.",
                type(trigger).__name__,
            )

    async def _run_timeout(self) -> None:
        try:
            await asyncio.sleep(self.window_timeout)
        except asyncio.CancelledError:
            return

        async with self._lock:
            if not self._running:
                return

        await self._close_window(timed_out=True)

    def _cancel_timeout(self) -> None:
        if self._timeout_task and not self._timeout_task.done():
            self._timeout_task.cancel()
        self._timeout_task = None

    def _read_window_state(self) -> Dict[str, List[Any]]:
        """Return a shallow snapshot of window_state for the sink to read."""
        return {name: list(items) for name, items in self._window_state.items()}

    def _stamp_epoch(self, epoch: datetime) -> None:
        """Push the current window epoch to all TimestampFilters in aggregators."""
        for agg in self._aggregators:
            # EventTrigger stores its compiled filter as _filter.
            f = getattr(agg, "_filter", None)
            if isinstance(f, TimestampFilter):
                f.set_epoch(epoch)
            # Also check composite filters wrapping a TimestampFilter.
            elif hasattr(f, "filters"):
                for sub in getattr(f, "filters", []):
                    if isinstance(sub, TimestampFilter):
                        sub.set_epoch(epoch)
