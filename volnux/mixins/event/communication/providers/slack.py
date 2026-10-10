import httpx
from typing import Dict, List

from .base import HumanInterfaceAdapterBase, HumanRequest


class SlackAdapter(HumanInterfaceAdapterBase):
    """
    Sends a Slack message with interactive approve/reject buttons.
    The Slack webhook delivers the human's click back to the Volnux
    coordinator via a WebhookTrigger endpoint, which forwards it as
    a HUMAN_RESPONSE command via the mesh.
    """

    def __init__(self, webhook_url: str, channel: str):
        self._webhook_url = webhook_url
        self._channel = channel

    async def notify(self, request: HumanRequest) -> None:
        blocks = self._build_blocks(request)
        async with httpx.AsyncClient() as client:
            await client.post(
                self._webhook_url,
                json={
                    "channel": self._channel,
                    "blocks": blocks,
                    "text": f"[Volnux] Human input required: {request.title}",
                },
            )

    def _build_blocks(self, request: HumanRequest) -> List[Dict]:
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": request.title},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": request.description},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Task ID:* `{request.task_id}`\n"
                    f"*Request ID:* `{request.request_id}`\n"
                    f"*Timeout:* {request.timeout_hours}h",
                },
            },
        ]
        if request.options:
            blocks.append(
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": opt.capitalize()},
                            "value": f"{request.request_id}:{opt}",
                            "action_id": f"hitl_{opt}",
                            "style": "primary" if opt == "approve" else "danger",
                        }
                        for opt in request.options
                    ],
                }
            )
        return blocks

    async def get_response_url(self, request: HumanRequest) -> str:
        return f"slack://channel/{self._channel}"
