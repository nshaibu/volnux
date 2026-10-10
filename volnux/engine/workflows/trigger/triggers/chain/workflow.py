import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Set

from ..event import Event
from ..base import TriggerBase, TriggerType

if TYPE_CHECKING:
    from ...eventbus.base import EventBusAdapterBase


logger = logging.getLogger(__name__)

# Topic on which upstream workflows publish completion events.
# The emitter must use this same literal.
WORKFLOW_COMPLETED_TOPIC = "workflow.completed"


class WorkflowStatus(str, Enum):
    """Well-known terminal statuses for a workflow run."""

    SUCCESS = "success"
    FAILURE = "failure"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class WorkflowChainTrigger(TriggerBase):
    """
    Trigger that activates when another workflow reaches a terminal status.

    Activation mechanism
    --------------------
    Subscribes to the ``workflow.completed`` topic on the supplied event bus.
    The emitter is expected to publish: class:`Event` payloads whose ``data``
    contains at minimum::

        {
            "workflow_name": str,
            "status": str, # one of WorkflowStatus values
            "result": Any, # optional
            "execution_id": str, # optional
        }

    Usage
    -----
    trigger = WorkflowChainTrigger(
        workflow_name="generate_report",
        parent_workflow="generate_data",
        event_bus=bus,
        on_status=[WorkflowStatus.SUCCESS],
        workflow_params={"report_type": "monthly"}
    )

    """

    trigger_type = TriggerType.WORKFLOW_CHAIN

    def __init__(
        self,
        workflow_name: str,
        parent_workflow: str,
        event_bus: "EventBusAdapterBase",
        on_status: Optional[Iterable[str]] = None,
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
        if event_bus is None:
            # Explicit: chain triggers are useless without a bus.
            raise ValueError(
                "WorkflowChainTrigger requires an event_bus to subscribe to."
            )

        self.parent_workflow = parent_workflow
        self.on_status: Set[str] = set(on_status or {WorkflowStatus.SUCCESS.value})
        self.event_bus = event_bus
        self._subscribed: bool = False

    async def start(self) -> None:
        """Subscribe to workflow completion events (idempotent)."""
        if self._subscribed:
            logger.debug(
                "WorkflowChainTrigger %s already subscribed; skipping",
                self.trigger_id,
            )
            return

        await self.event_bus.subscribe(
            WORKFLOW_COMPLETED_TOPIC, self._handle_completion
        )
        self._subscribed = True

        logger.info(
            "WorkflowChainTrigger %s watching %r for statuses %s",
            self.trigger_id,
            self.parent_workflow,
            sorted(self.on_status),
        )

    async def stop(self) -> None:
        """Unsubscribe from workflow completion events (idempotent)."""
        if not self._subscribed:
            return
        try:
            await self.event_bus.unsubscribe(
                WORKFLOW_COMPLETED_TOPIC, self._handle_completion
            )
        finally:
            self._subscribed = False

    async def _handle_completion(self, event: Event) -> None:
        """Filter completion events and activate when they match."""
        data = event.data or {}

        if data.get("workflow_name") != self.parent_workflow:
            return

        status = data.get("status")
        if status not in self.on_status:
            logger.debug(
                "Workflow %s completed with status %s; ignoring " "(waiting for %s)",
                self.parent_workflow,
                status,
                sorted(self.on_status),
            )
            return

        await self.activate(
            parent_workflow=self.parent_workflow,
            parent_status=status,
            parent_result=data.get("result"),
            parent_execution_id=data.get("execution_id"),
            parent_correlation_id=event.correlation_id,
        )
