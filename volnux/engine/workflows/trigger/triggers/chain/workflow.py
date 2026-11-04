import logging
from enum import Enum
from typing import Any, Dict, Iterable, Optional, Set

from ...event import TriggerEvent
from ..base import TriggerBase, TriggerType
from ..event import EventSubscriptionMixin


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


class WorkflowChainTrigger(EventSubscriptionMixin, TriggerBase):
    """
    Trigger that activates when another workflow reaches a terminal status.

    Activation mechanism
    --------------------
    Listens for ``TriggerEvent`` records of type ``workflow.completed`` on the
    messaging backend. The emitter publishes them with
    ``await TriggerEvent.publish(...)``; ``data`` contains at minimum::

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
        on_status=[WorkflowStatus.SUCCESS],
        workflow_params={"report_type": "monthly"}
    )

    """

    trigger_type = TriggerType.WORKFLOW_CHAIN

    def __init__(
        self,
        workflow_name: str,
        parent_workflow: str,
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
        self.parent_workflow = parent_workflow
        self.on_status: Set[str] = set(on_status or {WorkflowStatus.SUCCESS.value})

    async def start(self) -> None:
        """Start listening for workflow completion events (idempotent)."""
        if self.subscribed:
            logger.debug(
                "WorkflowChainTrigger %s already subscribed; skipping",
                self.trigger_id,
            )
            return

        await self._open_subscription()

        logger.info(
            "WorkflowChainTrigger %s watching %r for statuses %s",
            self.trigger_id,
            self.parent_workflow,
            sorted(self.on_status),
        )

    async def stop(self) -> None:
        """Stop listening (idempotent)."""
        await self._close_subscription()

    async def _handle_event(self, event: TriggerEvent) -> None:
        if event.event_type == WORKFLOW_COMPLETED_TOPIC:
            await self._handle_completion(event)

    async def _handle_completion(self, event: TriggerEvent) -> None:
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
