from abc import ABC, abstractmethod

from volnux.exceptions import ExternalCommunicationSuspensionRequest as HumanRequest


class HumanInterfaceAdapterBase(ABC):
    """
    Abstract interface for human notification and response collection.

    Implementations ship for:
    - EmailAdapter         — sends email, response via reply or link
    - SlackAdapter         — posts to channel, response via button click
    - WebhookAdapter       — POSTs to a URL, response via callback
    - VolnuxUIAdapter      — native web UI review queue
    - CLIAdapter           — blocks volnux shell, operator types response
    """

    @abstractmethod
    async def notify(self, request: HumanRequest) -> None:
        """
        Notify the human reviewer that input is required.
        Must be idempotent — may be called again if the worker
        crashes after notify but before checkpoint.
        """

    @abstractmethod
    async def get_response_url(self, request: HumanRequest) -> str:
        """
        Return the URL or reference the human uses to submit a response.
        Included in the notification.
        """
