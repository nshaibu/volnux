import typing
from enum import IntEnum
from datetime import datetime, timezone
from typing import Dict, Any

from formax import BaseModel, MiniAnnotated, Attrib, ValidationFlags

from volnux.constants import MAX_RETRIES
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


class InitArgsTemplate(typing.TypedDict, total=False):
    # The context in which the task was created
    execution_context_id: typing.Optional[str]

    # task identity
    task_id: typing.Optional[str]

    previous_result: typing.List[typing.Union[str, dict]]

    stop_condition: typing.List[str]
    run_bypass_event_checks: bool

    # task configuration
    options: typing.Optional[dict]
    sequence_number: typing.Optional[int]
    kwargs: typing.Dict[str, typing.Any]


class CallArgsTemplate(typing.TypedDict, total=False):
    args: typing.List[typing.Any]  # Positional arguments (already serialized)
    kwargs: typing.Dict[str, typing.Any]  # Keyword arguments (already serialized)


class EventPhase(IntEnum):
    INITIALIZED = 0
    COMMUNICATING = 1
    PRE_PROCESS = 2
    PROCESSING = 3
    POST_PROCESS = 4
    COMPLETED = 5


class ResourceState(typing.TypedDict, total=False):
    """Schema for user-registered external states (e.g., DB cursors, file offsets)."""

    resource_name: str
    data: dict
    provider_path: str  # The import path to the restoration logic


class EventCheckpointSnapshot(KeyValueStoreIntegrationMixin, BaseModel):
    """The 'Source of Truth' persisted in Redis for a specific Task ID."""

    task_id: str
    class_path: str  # e.g., "myapp.events.SendEmailTask"
    phase: EventPhase

    # Arguments used to re-instantiate the class via __init__
    init_args: MiniAnnotated[InitArgsTemplate, Attrib(default_factory=dict)]

    call_args: MiniAnnotated[CallArgsTemplate, Attrib(default_factory=dict)]

    # User-defined external states registered during the 'process' step typing.Dict[str, ResourceState]
    external_resources: MiniAnnotated[
        typing.Dict[str, ResourceState], Attrib(default_factory=dict)
    ]

    # User-bound serializable attributes captured from self.__dict__
    # (framework internals and already-tracked keys are excluded)
    attribs: MiniAnnotated[typing.Dict[str, typing.Any], Attrib(default_factory=dict)]

    # Metadata for the orchestrator (e.g., when it was last touched)
    timestamp: MiniAnnotated[
        float, Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    ]

    ## `process` method return value
    # Execution state captured at the end of the PROCESSING phase
    exec_status: bool = False

    # The result of process() or the error raised
    # This might be a Dict, List, or a Serialized Exception Dict
    exec_result: typing.Any = None

    # Capturing the retry state so preemption doesn't reset attempt counters
    retry_count: int = 0

    max_retry_attempts: int = MAX_RETRIES

    class Config:
        validation = ValidationFlags.NONE

    @classmethod
    def get_schema_name(cls) -> str:
        return "volnux:event:checkpoint"

    @classmethod
    def get_backend_config(cls) -> Dict[str, Any]:
        return {
            "ENGINE": "volnux.backends.stores.redis.RedisStoreBackend",
            "CONNECTOR_CONFIG": {
                "host": "localhost",
                "port": 6379,
                "db": 0,
            },
        }
