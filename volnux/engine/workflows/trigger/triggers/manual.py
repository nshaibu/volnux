import logging
from typing import Any, Dict, Optional

from .base import TriggerBase, TriggerType


logger = logging.getLogger(__name__)


class ManualTrigger(TriggerBase):
    """
    Trigger that activates via explicit invocation.

    Activation mechanism: Direct method call (from API, CLI, etc.).
    Nothing to subscribe to or schedule.

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

    async def invoke(self, **invocation_params) -> None:
        """
        Explicitly invoke this trigger.

        Reserved keys in ``invocation_params`` (never forwarded as such):
            - ``confirmed`` (bool): must be ``True`` when ``require_confirmation``
              is set; ignored otherwise.
            - ``user`` (str):    forwarded as ``invoked_by``.
            - ``source`` (str):  forwarded as ``invocation_source``.

        Any remaining keys are passed through to the workflow as activation data.

        Raises:
            RuntimeError: The trigger is disabled (paused or stopped). An explicit
                request must not vanish silently the way an automatic one does.
            ValueError: Confirmation is required and was not given as ``True``.
        """
        confirmed = invocation_params.pop("confirmed", False)
        invoked_by = invocation_params.pop("user", "unknown")
        invocation_source = invocation_params.pop("source", "manual")

        if not self.enabled:
            raise RuntimeError(
                f"Manual trigger '{self.trigger_id}' is disabled "
                f"(lifecycle: {self.lifecycle.value}); it cannot be invoked."
            )

        # ``is True``: a string such as "false" is truthy but is not a confirmation
        if self.require_confirmation and confirmed is not True:
            raise ValueError("Manual trigger requires confirmation")

        logger.info(f"ManualTrigger {self.trigger_id} invoked manually by {invoked_by}")

        await self.activate(
            invoked_by=invoked_by,
            invocation_source=invocation_source,
            **invocation_params,
        )
