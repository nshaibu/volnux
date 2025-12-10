import logging
import asyncio
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Callable, Awaitable


from volnux.mixins import ObjectIdentityMixin

logger = logging.getLogger(__name__)


class TriggerType(Enum):
    """Types of triggers supported by the framework."""

    SCHEDULE = "schedule"
    EVENT = "event"
    CONDITION = "condition"
    WORKFLOW_CHAIN = "workflow_chain"
    MANUAL = "manual"


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
        """Deserialize event from dictionary."""
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            source=data["source"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            data=data.get("data", {}),
            metadata=data.get("metadata", {}),
            correlation_id=data.get("correlation_id"),
        )


@dataclass
class TriggerActivation:
    """
    Data passed when a trigger activates.

    Contains all context needed for workflow execution.
    """

    trigger_id: str
    activated_at: datetime
    activation_source: TriggerType
    workflow_params: Dict[str, Any]
    metadata: Dict[str, Any]


class TriggerLifecycle(Enum):
    """Lifecycle states of a trigger."""

    CREATED = "created"
    INITIALIZED = "initialized"
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


class TriggerBase(ObjectIdentityMixin, ABC):
    """
    Base trigger class
    """

    def __init__(
        self,
        workflow_name: str,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__()
        self.workflow_name = workflow_name
        self.workflow_params = workflow_params or {}
        self.enabled = enabled
        self.metadata = metadata or {}

        self.lifecycle = TriggerLifecycle.CREATED
        self.fire_count = 0
        self.last_fired: Optional[datetime] = None
        self.error_count = 0

        # Callback to invoke when trigger fires
        self._on_activate: Optional[Callable[[TriggerActivation], Awaitable[None]]] = (
            None
        )

    @property
    def trigger_id(self) -> str:
        return self.id

    def set_activation_callback(
        self, callback: Callable[[TriggerActivation], Awaitable[None]]
    ):
        """
        Set callback to invoke when trigger activates.

        The engine provides this callback during registration.
        Raises:
            TypeError: If callback is not async callable.
        """
        if not asyncio.iscoroutinefunction(callback):
            raise TypeError("Callback must be a coroutine")
        self._on_activate = callback

    @abstractmethod
    async def start(self):
        """
        Start the trigger's activation mechanism.

        Examples:
        - Event trigger: Subscribe to event bus
        - Schedule trigger: Start timer/cron
        - Manual trigger: Register API endpoint
        - Condition trigger: Start polling loop
        """
        pass

    @abstractmethod
    async def stop(self):
        """
        Stop the trigger's activation mechanism.

        Clean up resources (unsubscribe, cancel timers, etc.)
        """
        pass

    async def activate(self, **activation_data):
        """
        Called internally when trigger condition is met.

        Builds TriggerActivation and invokes callback.
        Raises:
            NotImplementedError: Not activation provided.
        """
        if not self.enabled:
            logger.debug(f"Trigger {self.trigger_id} is disabled, skipping activation")
            return

        self.fire_count += 1
        self.last_fired = datetime.now()

        # Build activation context
        activation = TriggerActivation(
            trigger_id=self.trigger_id,
            activated_at=self.last_fired,
            activation_source=self.get_activation_source(),
            workflow_params={**self.workflow_params, **activation_data},
            metadata=self.metadata,
        )

        logger.info(f"Trigger {self.trigger_id} activated (fires: {self.fire_count})")

        if self._on_activate:
            try:
                await self._on_activate(activation)
            except Exception as e:
                self.error_count += 1
                logger.error(f"Activation callback failed: {e}")
                raise

        raise NotImplementedError("No activation callback implemented.")

    @abstractmethod
    def get_activation_source(self) -> TriggerType:
        """Return the source type of activation."""
        pass

    def pause(self):
        """Pause the trigger (temporarily disable)."""
        self.enabled = False
        self.lifecycle = TriggerLifecycle.PAUSED

    def resume(self):
        """Resume a paused trigger."""
        self.enabled = True
        self.lifecycle = TriggerLifecycle.ACTIVE
