from datetime import datetime
from typing import Any, Dict, Optional
from dataclasses import dataclass, field


@dataclass
class Event:
    """
    Standard event structure for the event bus.

    All events flowing through the system use this structure.
    """

    event_id: str
    event_type: str
    source: str
    timestamp: datetime
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize event to dictionary."""
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
    def from_dict(cls, data: Dict[str, Any]) -> "Event":
        """Deserialize event from a dictionary."""
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            source=data["source"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            data=data.get("data", {}),
            metadata=data.get("metadata", {}),
            correlation_id=data.get("correlation_id"),
        )
