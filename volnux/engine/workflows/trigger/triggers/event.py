import asyncio
import logging
from typing import Any, Dict, List, Optional

from ..event import TriggerEvent
from ..event_filters import CompositeFilter, EventFilterBase, PatternFilter
from .base import TriggerBase, TriggerType

logger = logging.getLogger(__name__)

#: Matches every event type.
ANY_EVENT_TYPE = "*"


class EventSubscriptionMixin:
    """Receive ``TriggerEvent`` records through the messaging backend.

    ``TriggerEvent.subscribe`` blocks until it is cancelled, so the subscription
    lives in one background task owned by the trigger. A handler that raises does
    not end it; if the subscription itself fails (the backend went away) the
    trigger is marked ERROR.

    Subclasses implement :meth:`_handle_event`.
    """

    _subscription_task: Optional[asyncio.Task] = None

    @property
    def subscribed(self) -> bool:
        task = self._subscription_task
        return task is not None and not task.done()

    async def _open_subscription(self) -> None:
        if self.subscribed:
            return
        self._subscription_task = asyncio.create_task(
            self._run_subscription(),
            name=f"{type(self).__name__}.{self.trigger_id}.subscription",  # type: ignore[attr-defined]
        )
        # Let the task attach to the channel before the caller carries on.
        await asyncio.sleep(0)

    async def _close_subscription(self) -> None:
        task, self._subscription_task = self._subscription_task, None
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _run_subscription(self) -> None:
        try:
            await TriggerEvent.subscribe(
                self._handle_event, on_error=self._on_handler_error
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "%s %s: subscription failed",
                type(self).__name__,
                self.trigger_id,  # type: ignore[attr-defined]
            )
            try:
                await self.state.mark_error()  # type: ignore[attr-defined]
            except Exception:
                logger.exception("Could not record the failed subscription")

    async def _on_handler_error(self, exc: BaseException, event: Any) -> None:
        # activate() has already counted a failed activation; keep listening.
        logger.error(
            "%s %s: handling event %s failed: %s",
            type(self).__name__,
            self.trigger_id,  # type: ignore[attr-defined]
            getattr(event, "event_id", "?"),
            exc,
        )

    async def _handle_event(self, event: TriggerEvent) -> None:
        raise NotImplementedError


class EventTrigger(EventSubscriptionMixin, TriggerBase):
    """
    Trigger that activates on events published as ``TriggerEvent``.

    Activation mechanism
    --------------------
    Publishers call ``await TriggerEvent.publish(event)``; the event travels over
    the messaging backend the deployment configured. The trigger listens to that
    channel and activates for events whose ``event_type`` is one of ``event_types``
    (``"*"`` accepts every type).

    ``event_pattern`` further filters by matching key/value pairs against
    ``event.data`` (exact equality, flat dict) and ``event_filter`` accepts any
    :class:`EventFilterBase`; when both are given an event must satisfy both.

    :param name: Identifies the trigger inside a ``WindowedTrigger``.
    """

    trigger_type = TriggerType.EVENT

    def __init__(
        self,
        workflow_name: str,
        event_types: List[str],
        event_pattern: Optional[Dict[str, Any]] = None,
        event_filter: Optional[EventFilterBase] = None,
        name: Optional[str] = None,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if not event_types:
            raise ValueError("EventTrigger requires at least one event_type.")

        super().__init__(
            workflow_name,
            workflow_params=workflow_params,
            enabled=enabled,
            metadata=metadata,
        )
        self.name = name
        self.event_types: List[str] = list(event_types)
        self.event_pattern = event_pattern

        filters: List[EventFilterBase] = []
        if event_pattern:
            filters.append(PatternFilter(event_pattern))
        if event_filter is not None:
            filters.append(event_filter)
        # None means "accept everything"
        self._filter: Optional[EventFilterBase] = (
            filters[0]
            if len(filters) == 1
            else CompositeFilter(filters, "AND") if filters else None
        )

    def accepts_type(self, event_type: str) -> bool:
        return ANY_EVENT_TYPE in self.event_types or event_type in self.event_types

    async def start(self) -> None:
        """Start listening for events (idempotent)."""
        if self.subscribed:
            logger.debug(
                "EventTrigger %s already subscribed; skipping start", self.trigger_id
            )
            return
        logger.info(
            "EventTrigger %s subscribing to: %s", self.trigger_id, self.event_types
        )
        await self._open_subscription()

    async def stop(self) -> None:
        """Stop listening (idempotent)."""
        logger.info("EventTrigger %s unsubscribing", self.trigger_id)
        await self._close_subscription()

    async def _handle_event(self, event: TriggerEvent) -> None:
        """Drop events of other types, apply the filters, then activate."""
        if not self.accepts_type(event.event_type):
            return

        if self._filter is not None and not self._filter.matches(event):
            logger.debug(
                "EventTrigger %s: event %s filtered out",
                self.trigger_id,
                event.event_id,
            )
            return

        await self.activate(
            event_id=event.event_id,
            event_type=event.event_type,
            event_source=event.source,
            event_data=event.data,
            event_metadata=event.metadata,
            correlation_id=self.generate_correlation_id(),
        )
