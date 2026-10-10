import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ..event import Event
from ..event_filters import CompositeFilter, EventFilterBase, PatternFilter, TypeFilter
from .base import TriggerBase, TriggerType

if TYPE_CHECKING:
    from ..eventbus.base import EventBusAdapterBase


logger = logging.getLogger(__name__)


class EventTrigger(TriggerBase):
    """
    Trigger that activates on events received from an event bus.

    Activation mechanism
    --------------------
    Subscribes to one or more ``event_types`` on the supplied bus. An optional
    ``event_pattern`` may be provided to further filter events by matching
    key/value pairs against ``event.data`` (exact equality, flat dict).

    The ``TypeFilter`` is intentionally omitted from the in-process filter
    because the bus subscription already constrains delivery to the requested
    types. Only the ``PatternFilter`` runs per-event when a pattern is given.
    """

    trigger_type = TriggerType.EVENT

    def __init__(
        self,
        workflow_name: str,
        event_bus: "EventBusAdapterBase",
        event_types: List[str],
        event_pattern: Optional[Dict[str, Any]] = None,
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
        self.event_bus = event_bus
        self.event_types: List[str] = list(event_types)
        self.event_pattern = event_pattern

        # Build the filter once; None means "accept everything".
        self._filter: Optional[EventFilterBase] = (
            PatternFilter(event_pattern) if event_pattern else None
        )

        # Track which types are currently subscribed for idempotent stop().
        self._subscribed_types: List[str] = []

    async def start(self) -> None:
        """Subscribe to all requested event types (idempotent)."""
        if self._subscribed_types:
            logger.debug(
                "EventTrigger %s already subscribed; skipping start",
                self.trigger_id,
            )
            return

        logger.info(
            "EventTrigger %s subscribing to: %s", self.trigger_id, self.event_types
        )

        for event_type in self.event_types:
            await self.event_bus.subscribe(event_type, self._handle_event)
            self._subscribed_types.append(event_type)

    async def stop(self) -> None:
        """Unsubscribe from all currently subscribed event types."""
        logger.info("EventTrigger %s unsubscribing", self.trigger_id)

        for event_type in list(self._subscribed_types):
            await self.event_bus.unsubscribe(event_type, self._handle_event)
            self._subscribed_types.remove(event_type)

    async def _handle_event(self, event: Event) -> None:
        """
        Receive an event from the bus, apply the pattern filter (if any),
        and activate the trigger.
        """
        if self._filter is not None and not self._filter.matches(event):
            logger.debug(
                "EventTrigger %s: event %s filtered out by pattern",
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
