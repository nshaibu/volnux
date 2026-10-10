import logging
from typing import Any, Dict, Optional

from .base import TriggerBase, TriggerType


logger = logging.getLogger(__name__)


class ManualTrigger(TriggerBase):
    """
    Trigger that activates via explicit invocation.

    Activation mechanism: Direct method call (from API, CLI, etc.).
    No event bus or scheduling needed.

    Note:
        `require_confirmation` is an in-memory configuration flag and is not
        persisted in the trigger state record.
    """

    trigger_type = TriggerType.MANUAL

    def __init__(
        self,
        workflow_name: str,
        require_confirmation: bool = False,
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
        self.require_confirmation = require_confirmation

    async def start(self):
        """Manual triggers are always ready — nothing to set up."""
        logger.info(f"ManualTrigger {self.trigger_id} ready for invocation")

    async def stop(self):
        """Nothing to stop for manual triggers."""
        logger.info(f"ManualTrigger {self.trigger_id} stopped")

    async def invoke(self, **invocation_params):
        """
        Explicitly invoke this trigger.

        Reserved keys in ``invocation_params``:
            - ``confirmed`` (bool): required when ``require_confirmation`` is set.
            - ``user`` (str):    forwarded as ``invoked_by``.
            - ``source`` (str):  forwarded as ``invocation_source``.

        Any remaining keys are passed through to the workflow as activation data.
        """
        if self.require_confirmation and not invocation_params.pop("confirmed", False):
            raise ValueError("Manual trigger requires confirmation")

        invoked_by = invocation_params.pop("user", "unknown")
        invocation_source = invocation_params.pop("source", "manual")

        logger.info(f"ManualTrigger {self.trigger_id} invoked manually by {invoked_by}")

        await self.activate(
            invoked_by=invoked_by,
            invocation_source=invocation_source,
            **invocation_params,
        )
