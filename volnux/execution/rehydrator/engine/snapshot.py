import typing
import logging
import threading
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from formax import BaseModel, MiniAnnotated, Attrib, ValidationFlags

from volnux.engine.base import WorkflowEngine
from volnux import __version__ as volnux_version
from volnux.execution.context import ExecutionContext
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin

if typing.TYPE_CHECKING:
    from volnux.parser.protocols import TaskType
    from volnux.parser.operator import PipeType


logger = logging.getLogger(__name__)


class QueueTaskTemplate(typing.TypedDict, total=True):
    task_id: str
    previous_context_id: str
    position_in_queue: int


class TaskSnapshot(KeyValueStoreIntegrationMixin, BaseModel):
    # The context in which the task was created
    context_id: str

    # task identity
    task_id: str
    task_type: typing.Literal["normal", "group"]

    # For idempotency (i.e., The internal state of the task when it was checkpointed)
    task_checkpoint: typing.Optional[dict]

    # Event information
    event_name: str
    event_class_import_path: str

    # Sink/Deferred Task Information
    sink_task_id: typing.Optional[str]
    sink_task_pipe: typing.Optional["PipeType"]

    # task configuration and states
    options: typing.Dict[str, typing.Any]
    condition_node: typing.Dict[str, typing.Any]
    sequence_number: typing.Optional[int]
    descriptor: typing.Optional[int]
    descriptor_pipe_type: typing.Optional[str]

    # TODO: Grouped chain information.
    #  Groups are mini-orchestrators for grouping graph of task.
    #  We can use TaskSnapshot to store the cotext of the ControlFlowEvent.
    #  Which in turn will keep the states of the individual task in the current task checkpoint
    # chains_task_ids: typing.Optional[typing.List[str]]
    # strategy: typing.Optional[str]  # choices: "single", "multiple"

    snapshot_timestamp: MiniAnnotated[
        float, Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    ]
    snapshot_version: str = volnux_version

    class Config:
        validation = ValidationFlags.NONE

    @classmethod
    def get_schema_name(cls) -> str:
        return "volnux:snapshot:task"

    async def restore(self) -> "TaskType":
        pass


@dataclass
class TraversalSnapshot:
    """
    Captures the state of the execution queue at snapshot time.
    """

    engine_class_path: str

    # Current task being executed (maybe mid-flight)
    current_task: typing.Optional[
        QueueTaskTemplate
    ]  # The active task when the snapshot was taken
    current_task_checkpoint: typing.Optional[
        dict
    ]  # For idempotency (i.e. The internal state of the task when it was checkpointed)

    # Remaining tasks in the queue (LIFO order preserved)
    # Serialized PipelineTask objects
    # Remaining tasks in the queue at snapshot time
    task_queue_snapshot: typing.List[QueueTaskTemplate]

    # Queue position tracking
    # Position in the original queue
    queue_index: int  # The index of the current task in the original queue
    current_task_queue_size: int  # The total size of the original queue
    current_sink_queue_size: int  # The total size of the sink queue

    # Sink nodes (deferred execution)
    # Serialized sink tasks
    sink_queue_snapshot: typing.List[QueueTaskTemplate]

    # Engine state markers
    tasks_processed: int  # How many tasks completed before snapshot?
    # is_multitask_context: bool # Was this a parallel execution group?

    async def restore(self) -> WorkflowEngine:
        pass


class ContextSnapshot(KeyValueStoreIntegrationMixin, BaseModel):
    """
    Serializable snapshot of ExecutionContext state.
    This is the canonical data structure for rehydration.
    """

    # Identity
    state_id: str
    workflow_id: str

    # Hierarchy (Tree Structure)
    parent_id: typing.Optional[str]
    child_ids: typing.List[str]
    depth: int

    # Horizontal Links (Doubly-Linked List)
    previous_context_id: typing.Optional[str]
    next_context_id: typing.Optional[str]

    # Task Queue State
    traversal: TraversalSnapshot

    # Pipeline Reference
    pipeline_id: str
    pipeline_state: typing.Optional[dict]
    pipeline_class_path: str  # e.g., "myapp.pipelines.DataProcessingPipeline"

    # Execution State
    status: str
    errors: typing.List[dict]  # Serialized exception messages
    results: typing.List[dict]  # Serialized EventResult objects

    # Metrics
    metrics: typing.Dict[str, typing.Any]

    # Metadata
    snapshot_timestamp: MiniAnnotated[
        float, Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    ]
    snapshot_version: str = volnux_version
    aggregated_result: typing.Optional[dict] = None

    class Config:
        validation = ValidationFlags.NONE

    @classmethod
    def get_schema_name(cls) -> str:
        return "volnux:snapshot:context"

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: typing.Dict[str, typing.Any]) -> "ContextSnapshot":
        data["traversal"] = TraversalSnapshot(**data["traversal"])
        return cls(**data)

    def set_state(self, state: typing.Dict[str, typing.Any]) -> None:
        data = state.copy()
        data["traversal"] = TraversalSnapshot(**data["traversal"])
        data.pop("_objectid_lock", None)
        self.__dict__.update(data)
        self._objectid_lock = threading.Lock()

    def get_state(self) -> typing.Dict[str, typing.Any]:
        state = self.__dict__.copy()
        state.pop("_objectid_lock", None)
        traversal_state = state["traversal"].__dict__.copy()
        state["traversal"] = traversal_state
        return state

    async def restore(self) -> ExecutionContext:
        pass
