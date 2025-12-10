import logging
import typing
from datetime import datetime
from typing import Any, Dict, List, Optional

from .base import TriggerBase, TriggerLifecycle, TriggerType

logger = logging.getLogger(__name__)


class WebhookTrigger(TriggerBase):
    """
    Trigger that activates via HTTP webhook.

    Activation mechanism: Registers HTTP endpoint to receive webhooks.
    No event bus needed!

    Note: Requires integration with a web framework (FastAPI, Flask, etc.)
    """

    def __init__(
        self,
        workflow_name: str,
        endpoint_path: str,
        secret_token: Optional[str] = None,
        web_app: Optional[Any] = None,  # FastAPI/Flask app
        **kwargs,
    ):
        super().__init__(workflow_name, **kwargs)
        self.endpoint_path = endpoint_path
        self.secret_token = secret_token
        self.web_app = web_app

    async def start(self):
        """Register webhook endpoint."""
        if not self.web_app:
            raise RuntimeError("WebhookTrigger requires web_app parameter")

        logger.info(
            f"WebhookTrigger {self.trigger_id} registering at {self.endpoint_path}"
        )

        # Register route with a web framework
        self._register_endpoint()

        self.lifecycle = TriggerLifecycle.ACTIVE

    async def stop(self):
        """Unregister webhook endpoint."""
        logger.info(f"WebhookTrigger {self.trigger_id} unregistering")
        if self.web_app:
            # TODO: shutdown webhook http server / unhook the endpoint
            pass
        self.lifecycle = TriggerLifecycle.STOPPED

    def _register_endpoint(self):
        """Register endpoint with a web framework."""
        # Example for FastAPI:
        # @self.web_app.post(self.endpoint_path)
        # async def webhook_handler(request: Request):
        #     return await self.handle_webhook(request)
        raise NotImplementedError("No endpoints were registered")

    async def handle_webhook(self, request_data: Dict[str, Any]):
        """
        Handle incoming webhook request.

        Called by the web framework when webhook is received.
        """
        # Verify secret token if configured
        if self.secret_token:
            provided_token = request_data.get("token")
            if provided_token != self.secret_token:
                raise ValueError("Invalid webhook token")

        # Activate trigger with webhook data
        await self.activate(
            webhook_data=request_data, received_at=datetime.now().isoformat()
        )

    def get_activation_source(self) -> TriggerType:
        return "webhook"
