from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone

from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


@dataclass
class ExternalCommunicationQueueEntry(KeyValueStoreIntegrationMixin):
    """
    Represents an entry in the external communication queue.

    This class encapsulates all necessary details related to a workflow's external
    communication suspension request. It includes attributes to track the request
    information, related workflow and task details, payload, options, and timeout
    information. It is designed to integrate with a key-value store backend and
    provides functionality to retrieve configuration and schema details related
    to this backend.

    :ivar request_id: Identifier that matches the external communication suspension request.
    :type request_id: str
    :ivar workflow_name: Name of the workflow associated with the request.
    :type workflow_name: str
    :ivar workflow_id: Identifier for the specific workflow run instance.
    :type workflow_id: str
    :ivar task_id: Identifier of the task that was suspended.
    :type task_id: str
    :ivar checkpoint_key: Redis key of the full checkpoint snapshot for the request.
    :type checkpoint_key: str
    :ivar request_title: Title of the external communication request.
    :type request_title: str
    :ivar request_payload: Payload associated with the external communication request.
    :type request_payload: Dict[str, Any]
    :ivar options: List of options related to the external communication request, if any.
    :type options: Optional[List[str]]
    :ivar timeout_at: ISO timestamp indicating when the request times out.
        A value of None indicates no timeout is set.
    :type timeout_at: Optional[str]
    :ivar enqueued_at: ISO timestamp capturing when the request was enqueued.
    :type enqueued_at: str
    """

    request_id: str  # matches ExternalCommunicationSuspensionRequest.request_id
    workflow_name: str
    workflow_id: str  # specific run instance
    task_id: str  # the suspended task
    checkpoint_key: str  # Redis key of the full checkpoint snapshot
    request_title: str
    request_payload: Dict[str, Any]
    options: Optional[List[str]]
    timeout_at: Optional[str]  # ISO timestamp — None means no timeout
    enqueued_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    is_persisted: bool = False

    @classmethod
    def get_backend_config(cls) -> Dict[str, Any]:
        return {"redis": {"host": "localhost", "port": 6379, "db": 0}}

    @classmethod
    def get_schema_name(cls) -> str:
        return "volnux:workflow:external_communication:queue"


@dataclass
class ExternalCommunicationResponse(KeyValueStoreIntegrationMixin):
    """
    The human's response — published to Redis pub/sub when received.
    The bootloader subscribes to response_channel and receives this.
    """

    request_id: str
    decision: str  # "approve", "reject", or custom option
    data: Dict[str, Any]  # labelled/modified data if applicable
    reviewer: str  # human identity
    responded_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    is_persisted: bool = False

    @classmethod
    def get_backend_config(cls) -> Dict[str, Any]:
        return {"redis": {"host": "localhost", "port": 6379, "db": 0}}

    @classmethod
    def get_schema_name(cls) -> str:
        return "volnux:workflow:external_communication:response"
