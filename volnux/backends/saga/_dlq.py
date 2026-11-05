from typing import Any, Union, Dict, List, Optional
from formax import BaseModel, MiniAnnotated, Attrib

from volnux.backends.storage_route import StorageRoute
from volnux.mixins.messaging import MessagingBackendIntegrationMixin
from volnux.context import get_current_node_id, get_current_project_id


class DeadLetterEntry(MessagingBackendIntegrationMixin, BaseModel):
    saga_id: str
    saga_name: str
    timestamp: Union[float, int]
    failed_step_index: int
    failed_step_name: str
    original_error: str
    compensation_failures: List[Dict[str, Any]]
    steps_state: List[Dict[str, Any]]

    # project identifier
    workflow_id: Optional[str] = None
    node_id: MiniAnnotated[
        Optional[str], Attrib(default_factory=lambda: get_current_node_id())
    ]
    project_id: MiniAnnotated[
        Optional[str], Attrib(default_factory=lambda: get_current_project_id())
    ]

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        config = cls.get_volnux_config()
        return StorageRoute(
            components=["volnux", "{project_id}", "{node_id}", "saga", "dlq"],
            routing_keys=lambda: {
                "project_id": config.get("PROJECT_ID"),
                "node_id": config.get("NODE_ID"),
            },
        )

    #: ``DATABASES["saga"]``, else ``"default"``. (It used to be a Redis at
    #: localhost whatever the configuration said, so a saga could not run without one.)
    @classmethod
    def get_database_alias(cls) -> str:
        return "saga"
