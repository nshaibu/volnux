"""The event a trigger reacts to.

``TriggerEvent`` travels over the messaging backend the deployment configured
(``MessagingBackendIntegrationMixin``), like the other published records: a
publisher calls ``await TriggerEvent.publish(event)`` and every running
``EventTrigger`` receives it. There is no event bus of its own and no connection
opened here.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from formax import Attrib, BaseModel, MiniAnnotated

from volnux.backends.fields import DateTimeField, DTConfig
from volnux.backends.storage_route import StorageRoute
from volnux.config import VolnuxConfig
from volnux.mixins.messaging import MessagingBackendIntegrationMixin


def _scope() -> Dict[str, Optional[str]]:
    config = VolnuxConfig.get_instance()
    try:
        node_id = config.get_node_id()
    except Exception:
        node_id = None
    return {"project_id": config.get("PROJECT_ID", None), "node_id": node_id}


def _new_id() -> str:
    return uuid.uuid4().hex


class TriggerEvent(MessagingBackendIntegrationMixin, BaseModel):
    """Standard event structure for event-driven triggers.

    ``event_type`` is what ``EventTrigger`` subscribes by (e.g. ``"order.created"``);
    ``data`` is what its pattern matches against.
    """

    event_type: str
    source: str
    event_id: MiniAnnotated[Optional[str], Attrib(default=None)]
    timestamp: DateTimeField[DTConfig(auto_now_add=True)]
    data: MiniAnnotated[Optional[Dict[str, Any]], Attrib(default=None)]
    metadata: MiniAnnotated[Optional[Dict[str, Any]], Attrib(default=None)]
    correlation_id: MiniAnnotated[Optional[str], Attrib(default=None)]

    def __post_init__(self, *args: Any, **kwargs: Any) -> None:
        super().__post_init__(*args, **kwargs)
        # Filled here rather than with ``default_factory``: formax evaluates a
        # factory once, which would give every event one id and one shared dict.
        if self.event_id is None:
            self.event_id = _new_id()
        if self.data is None:
            self.data = {}
        if self.metadata is None:
            self.metadata = {}

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        return StorageRoute(
            components=["volnux", "{project_id}", "{node_id}", "triggers", "events"],
            routing_keys=_scope,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the event to plain JSON-compatible values."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "metadata": self.metadata,
            "correlation_id": self.correlation_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TriggerEvent":
        """Rebuild an event from :meth:`to_dict` output."""
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            source=data["source"],
            timestamp=data.get("timestamp") or datetime.now(timezone.utc),
            data=data.get("data", {}),
            metadata=data.get("metadata", {}),
            correlation_id=data.get("correlation_id"),
        )

    def get_state(self) -> Dict[str, Any]:
        """Wire state: JSON-compatible fields plus the record id."""
        return {**self.to_dict(), "id": self.id}

    def set_state(self, state: Dict[str, Any]) -> None:
        """Restore from wire state; unknown keys are ignored."""
        for key, value in state.items():
            if key in ("id", "_backend_class"):
                continue
            if hasattr(type(self), key):
                setattr(self, key, value)
        if state.get("id"):
            object.__setattr__(self, "_id", state["id"])


# Name the filters and older callers use.
Event = TriggerEvent
