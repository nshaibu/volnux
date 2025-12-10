import logging
from typing import List, Optional

from .base import TriggerBase, TriggerLifecycle, Event, TriggerType


logger = logging.getLogger(__name__)


class WorkflowChainTrigger(TriggerBase):
    """
    Trigger that activates when another workflow completes.

    Activation mechanism: Subscribes to workflow completion events.
    Uses event bus BUT could also use direct callback registration.
    """

    def __init__(
        self,
        workflow_name: str,
        parent_workflow: str,
        on_status: List[str] = None,
        event_bus: Optional["EventBusAdapter"] = None,
        **kwargs,
    ):
        super().__init__(workflow_name, **kwargs)
        self.parent_workflow = parent_workflow
        self.on_status = on_status or ["success"]
        self.event_bus = event_bus

    async def start(self):
        """Subscribe to workflow completion events."""
        if self.event_bus:
            logger.info(
                f"WorkflowChainTrigger {self.trigger_id} watching "
                f"for {self.parent_workflow} completion"
            )
            await self.event_bus.subscribe(
                "workflow.completed", self._handle_completion
            )

        self.lifecycle = TriggerLifecycle.ACTIVE

    async def stop(self):
        """Unsubscribe from events."""
        if self.event_bus:
            await self.event_bus.unsubscribe(
                "workflow.completed", self._handle_completion
            )

        self.lifecycle = TriggerLifecycle.STOPPED

    async def _handle_completion(self, event: "Event"):
        """Handle workflow completion event."""
        # Check if this is the parent workflow
        if event.data.get("workflow_name") != self.parent_workflow:
            return

        # Check if status matches
        status = event.data.get("status")
        if status not in self.on_status:
            logger.debug(
                f"Workflow {self.parent_workflow} completed with status "
                f"{status}, not triggering (waiting for {self.on_status})"
            )
            return

        # Activate with parent workflow result
        await self.activate(
            parent_workflow=self.parent_workflow,
            parent_status=status,
            parent_result=event.data.get("result"),
            parent_execution_id=event.data.get("execution_id"),
        )

    def get_activation_source(self) -> TriggerType:
        return TriggerType.WORKFLOW_CHAIN
