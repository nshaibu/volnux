import logging

from .base import TriggerBase, TriggerLifecycle, TriggerType


logger = logging.getLogger(__name__)


class ManualTrigger(TriggerBase):
    """
    Trigger that activates via explicit invocation.

    Activation mechanism: Direct method call (from API, CLI, etc.)
    No event bus or scheduling needed!
    """

    def __init__(
        self, workflow_name: str, require_confirmation: bool = False, **kwargs
    ):
        super().__init__(workflow_name, **kwargs)
        self.require_confirmation = require_confirmation

    async def start(self):
        """Manual triggers are always ready."""
        logger.info(f"ManualTrigger {self.trigger_id} ready for invocation")
        self.lifecycle = TriggerLifecycle.ACTIVE

    async def stop(self):
        """Nothing to stop for manual triggers."""
        self.lifecycle = TriggerLifecycle.STOPPED

    async def invoke(self, **invocation_params):
        """
        Explicitly invoke this trigger.

        Called by:
        - REST API endpoint
        - CLI command
        - Admin UI
        - Test suite
        """
        logger.info(f"ManualTrigger {self.trigger_id} invoked manually")

        if self.require_confirmation:
            if not invocation_params.get("confirmed"):
                raise ValueError("Manual trigger requires confirmation")

        await self.activate(
            invoked_by=invocation_params.get("user", "unknown"),
            invocation_source=invocation_params.get("source", "manual"),
            **invocation_params,
        )

    def get_activation_source(self) -> TriggerType:
        return TriggerType.MANUAL
