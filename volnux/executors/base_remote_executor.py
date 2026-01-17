import concurrent
import time
import typing
import uuid
from concurrent.futures import Executor

from volnux.conf import ConfigLoader
from volnux.types import (
    QueryEventPayload,
    QueryEventResponse,
    TaskExecutionErrorResponse,
    TaskExecutionSuccessResponse,
    Payload,
)

CONF = ConfigLoader.get_lazily_loaded_config()
ALGORITHM = "sha256"


def get_secret_key() -> bytes:
    """Retrieve the secret key from configuration and ensure it is bytes."""
    key = CONF.SECRET_KEY
    if isinstance(key, str):
        return key.encode("utf-8")
    return key


class BaseRemoteExecutor(Executor):
    def construct_payload(
        self, event_name: str, args: typing.Dict[str, typing.Any]
    ) -> Payload:
        """Construct the payload to send to the remote manager."""
        from volnux.utils import generate_hmac

        dict_data = {
            "type": "submission_event",
            "event_name": event_name,
            "args": args,
            "correlation_id": str(uuid.uuid4()),
            "timeout": CONF.REMOTE_EVENT_TIMEOUT or None,
            "timestamp": time.time(),
            "client_id": self._get_client_id(),
        }

        hmac, _ = generate_hmac(dict_data, get_secret_key())
        dict_data["hmac"] = hmac

        return Payload(**dict_data)

    def _get_client_id(self) -> str:
        """Get the client ID for the newly constructed payload."""
        import socket

        return socket.gethostname()

    def query_event_exists(self, data: QueryEventPayload) -> QueryEventResponse:
        """Query the remote manager for the existence of an event."""
        raise NotImplementedError

    def parse_task_execution_response(
        self,
        response: typing.Union[
            TaskExecutionSuccessResponse, TaskExecutionErrorResponse
        ],
    ):
        """Parse the response from the remote manager."""
        from volnux.exceptions import RemoteExecutionError
        from volnux.utils import verify_hmac

        if isinstance(response, TaskExecutionErrorResponse):
            raise RemoteExecutionError(f"{response.code}: {response.message}")

        # Verify HMAC
        response_data = response.dump(_format="dict")
        if not verify_hmac(response_data, get_secret_key()):
            raise RemoteExecutionError("INVALID_HMAC")

        return response.result

    def submit(
        self, fn: typing.Callable, /, *args, **kwargs
    ) -> concurrent.futures.Future:
        raise NotImplementedError
