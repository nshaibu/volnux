import logging
import asyncio
import secrets
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Callable, Awaitable

from volnux.config import VolnuxConfig


logger = logging.getLogger(__name__)

conf = VolnuxConfig.get_instance()

# Prefix for structured correlation_id
_CORRECTION_ID_PREFIX = conf.get_node_id().strip("node_")[:8]


class TriggerType(Enum):
    """Types of triggers supported by the framework."""

    SCHEDULE = "schedule"
    EVENT = "event"
    ASSET = "asset"
    CONDITION = "condition"
    WORKFLOW_CHAIN = "workflow_chain"
    MANUAL = "manual"
    WEBHOOK = "webhook"
    MANAGER = "manager"


@dataclass
class TriggerActivation:
    """
    Data passed when a trigger activates.

    Contains all context needed for workflow execution.
    """

    trigger_id: str
    activated_at: datetime
    activation_source: TriggerType
    workflow_params: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class TriggerLifecycle(Enum):
    """Lifecycle states of a trigger."""

    CREATED = "created"
    INITIALIZED = "initialized"
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


class TriggerBase(ABC):
    """
    Base trigger class.

    The `state` attribute is a `TriggerStateRecord` that is persisted to SQLite,
    enabling inter-process communication between the engine and CLI. Changes made
    by the CLI are marked with `dirty=True`, allowing the engine to detect and
    apply them during its state synchronization loop.
    """

    trigger_type: TriggerType

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Only enforce on concrete (non-abstract) subclasses
        if not getattr(cls, "__abstractmethods__", None):
            if not isinstance(getattr(cls, "trigger_type", None), TriggerType):
                raise TypeError(
                    f"{cls.__name__} must define a class-level "
                    f"'trigger_type' attribute of type TriggerType."
                )

    def __init__(
        self,
        workflow_name: str,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        from volnux.engine.workflows.trigger.state import TriggerStateRecord

        super().__init__()

        self.state = TriggerStateRecord(
            workflow_name=workflow_name,
            enabled=enabled,
            lifecycle=TriggerLifecycle.CREATED,
            fire_count=0,
            error_count=0,
            last_fired=None,
            dirty=False,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        self.workflow_params = workflow_params or {}
        self.metadata = metadata or {}

        # Callback to invoke when trigger fires
        self._on_activate: Optional[Callable[[TriggerActivation], Awaitable[None]]] = (
            None
        )

    @property
    def trigger_id(self) -> str:
        """Get the unique identifier for this trigger from a persisted state."""
        return self.state.id

    @property
    def workflow_name(self) -> str:
        """Get the workflow name from the persisted state."""
        return self.state.workflow_name

    @property
    def enabled(self) -> bool:
        """Check if the trigger is enabled."""
        return self.state.enabled

    @property
    def lifecycle(self) -> TriggerLifecycle:
        """Get the current lifecycle state."""
        return self.state.lifecycle

    @property
    def fire_count(self) -> int:
        """Get the number of times this trigger has fired."""
        return self.state.fire_count

    @property
    def error_count(self) -> int:
        """Get the number of errors encountered by this trigger."""
        return self.state.error_count

    @property
    def last_fired(self) -> Optional[str]:
        """Get the timestamp of the last activation (ISO format string)."""
        return self.state.last_fired

    def update_workflow_params(self, params: Dict[str, Any]):
        """Update the workflow parameters."""
        self.workflow_params.update(params)

    async def initialize(self):
        """
        Persist initial state to a database.

        Should be called after trigger creation to ensure the state is available
        for CLI interactions.
        """
        await self.state.save()
        logger.debug(f"Initialized state for trigger '{self.trigger_id}'")

    def generate_correlation_id(self) -> str:
        """
        Generates a structured correlation_id: {node_prefix}:{trigger}:{fire_count}:{entropy}
        Example: 8ab2f10c:WE:2:k9a2
        """
        entropy = secrets.token_hex(2)

        return f"{_CORRECTION_ID_PREFIX}:{self.trigger_type.value[0:2].upper()}:{self.fire_count}:{entropy}"

    async def sync_state(self):
        """
        Reload state from the database to pick up CLI changes.

        The engine should call this periodically or when processing dirty records
        to apply changes made by the CLI (pause, resume, stop, etc.).
        """

        await self.state.reload()

    def set_activation_callback(
        self, callback: Callable[[TriggerActivation], Awaitable[None]]
    ):
        """
        Set callback to invoke when the trigger activates.

        The engine provides this callback during registration.

        Args:
            callback: Async callable that handles trigger activation

        Raises:
            TypeError: If callback is not async callable.
        """
        if not asyncio.iscoroutinefunction(callback):
            raise TypeError("Callback must be a coroutine")

        if self._on_activate is not None:
            logger.warning(
                f"Trigger '{self.trigger_id}': replacing existing activation callback. "
                "Ensure this trigger is not registered with multiple engines."
            )

        self._on_activate = callback

    async def run(self):
        """
        Start the trigger and update state to ACTIVE.

        This calls the state record's start() method which updates the lifecycle
        and persists changes, then calls the subclass's start() implementation.
        """
        await self.state.start()
        await self.start()

    async def end(self):
        """
        Stop the trigger and update the state to STOPPED.

        This calls the state record's stop() method, which updates the lifecycle
        and persists changes, then calls the subclass's stop() implementation.
        """
        await self.state.stop()
        await self.stop()

    @abstractmethod
    async def start(self):
        """
        Start the trigger's activation mechanism.

        Examples:
        - Event trigger: Subscribe to event-bus
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
        Called internally when the trigger condition is met.

        Builds TriggerActivation and invokes callback.

        Args:
            **activation_data: Additional data to pass to the workflow

        Raises:
            RuntimeError: If no activation callback is set
        """
        from volnux.engine.workflows.trigger.state import str_to_datetime

        if not self.enabled:
            logger.debug(f"Trigger {self.trigger_id} is disabled, skipping activation")
            return

        if self._on_activate is None:
            self.state.error_count += 1
            await self.state.save()
            logger.error(
                f"Trigger '{self.trigger_id}' has no activation callback set. "
                "Register the trigger with the engine before activating it."
            )
            raise RuntimeError(
                f"Trigger '{self.trigger_id}' has no activation callback set. "
                "Register the trigger with the engine before activating it."
            )

        collisions = set(self.workflow_params) & set(activation_data)
        if collisions:
            logger.warning(
                f"Trigger '{self.trigger_id}': activation_data keys {collisions} "
                "overlap with workflow_params and will override them."
            )

        self.state.fire_count += 1
        self.state.last_fired = datetime.now(timezone.utc)
        await self.state.save()

        activation = TriggerActivation(
            trigger_id=self.trigger_id,
            activated_at=str_to_datetime(self.last_fired),
            activation_source=self.get_activation_source(),
            workflow_params={
                **self.workflow_params,
                **activation_data,
                "__trigger_metadata__": self.metadata,
            },
            metadata=dict(self.metadata),
        )

        logger.info(f"Trigger {self.trigger_id} activated (fires: {self.fire_count})")

        try:
            await self._on_activate(activation)
        except Exception as e:
            self.state.error_count += 1
            await self.state.save()
            logger.error(
                f"Activation callback failed for trigger '{self.trigger_id}': {e}"
            )
            raise

    def get_activation_source(self) -> TriggerType:
        """Return the source type of activation."""
        return self.trigger_type

    async def pause(self):
        """
        Pause the trigger (temporarily disable).

        Delegates to the state record which validates lifecycle transitions
        and persists the change with dirty=True for CLI communication.

        Raises:
            RuntimeError: If the trigger is not in the ACTIVE state.
        """
        await self.state.pause()

    async def resume(self):
        """
        Resume a paused trigger.

        Delegates to the state record which validates lifecycle transitions
        and persists the change with dirty=True for CLI communication.

        Raises:
            RuntimeError: If the trigger is not in the PAUSED state.
        """
        await self.state.resume()
