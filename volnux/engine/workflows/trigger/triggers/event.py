import logging
import typing
from typing import Any, Dict, List, Optional

from .base import TriggerBase, TriggerLifecycle, Event, TriggerType
from ..event_filters import TypeFilter, PatternFilter, CompositeFilter, EventFilterBase

if typing.TYPE_CHECKING:
    from ..event_bus import EventBusAdapterBase


logger = logging.getLogger(__name__)


class EventTrigger(TriggerBase):
    """
    Trigger that activates on events from an event bus.

    Activation mechanism: Subscribe to event bus topics.
    """

    trigger_type = TriggerType.EVENT

    def __init__(
        self,
        workflow_name: str,
        event_bus: "EventBusAdapterBase",
        event_types: List[str],
        event_pattern: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        super().__init__(workflow_name, **kwargs)
        self.event_bus = event_bus
        self.event_types = event_types
        self.event_pattern = event_pattern

    async def start(self):
        """Subscribe to event bus topics."""
        logger.info(
            f"EventTrigger {self.trigger_id} subscribing to: {self.event_types}"
        )

        for event_type in self.event_types:
            await self.event_bus.subscribe(event_type, self._handle_event)

        self.lifecycle = TriggerLifecycle.ACTIVE

    async def stop(self):
        """Unsubscribe from event bus."""
        logger.info(f"EventTrigger {self.trigger_id} unsubscribing")

        for event_type in self.event_types:
            await self.event_bus.unsubscribe(event_type, self._handle_event)

        self.lifecycle = TriggerLifecycle.STOPPED

    def _create_filter(self) -> EventFilterBase:
        """Create event filter."""
        filters: typing.List[EventFilterBase] = [TypeFilter(self.event_types)]
        if self.event_pattern:
            filters.append(PatternFilter(self.event_pattern))

        return CompositeFilter(filters, "AND") if len(filters) > 1 else filters[0]

    async def _handle_event(self, event: "Event"):
        """Internal event handler."""
        # Apply filter if present
        event_filter = self._create_filter()
        if event_filter and not event_filter.matches(event):
            logger.debug(f"Event {event.event_id} filtered out")
            return

        # Activate trigger with event data
        await self.activate(
            event_id=event.event_id,
            event_type=event.event_type,
            event_data=event.data,
            event_source=event.source,
        )
