import typing
import logging
import threading
from datetime import datetime, timezone
from formax import BaseModel, MiniAnnotated, Attrib, ValidationFlags

from volnux.engine.base import WorkflowEngine
from volnux import __version__ as volnux_version
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
    # The context in which the task was created (None when not tied to one)
    context_id: typing.Optional[str]

    # task identity
    task_id: str
    task_type: typing.Literal["normal", "group"]

    # For idempotency (i.e., The internal state of the task when it was checkpointed)
    task_checkpoint: typing.Optional[dict]

    # Event information
    event_name: str
    # None for task groupings, which have no event class
    event_class_import_path: typing.Optional[str]

    # Sink/Deferred Task Information
    sink_task_id: typing.Optional[str]
    sink_task_pipe: typing.Optional["PipeType"]

    # task configuration and states
    options: typing.Dict[str, typing.Any]
    condition_node: typing.Dict[str, typing.Any]
    sequence_number: typing.Optional[int]
    descriptor: typing.Optional[int]
    descriptor_pipe_type: typing.Optional[str]

    # Whole-graph restoration (a task's sink subtree; a group's chains)
    sink_task: MiniAnnotated[typing.Optional[dict], Attrib(default=None)]
    chains: MiniAnnotated[typing.Optional[list], Attrib(default=None)]
    strategy: MiniAnnotated[typing.Optional[str], Attrib(default=None)]

    # namespace and descriptor labels (label -> value) copied from the program
    namespace: MiniAnnotated[str, Attrib(default="local")]
    descriptors: MiniAnnotated[dict, Attrib(default_factory=dict)]

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


class EngineStateSnapshot(KeyValueStoreIntegrationMixin, BaseModel):
    """
    The engine's position in a workflow at checkpoint time.

    This is engine state, not context state: which tasks are queued, which one
    was running and how far the engine got. It is keyed by the id of the
    ``ExecutionContext`` the engine was working on, so the pair
    ``ExecutionContext.get(id)`` + ``EngineStateSnapshot.get(id)`` is everything
    needed to resume. Queued tasks are stored as ``TaskSnapshot`` records and
    referenced by id.
    """

    # The context the engine was working on
    context_id: str
    engine_class_path: str

    # Current task being executed (maybe mid-flight)
    current_task: typing.Optional[QueueTaskTemplate]

    # Queue position tracking
    task_queue_snapshot: typing.List[QueueTaskTemplate]
    queue_index: int  # The index of the current task in the original queue
    current_task_queue_size: int  # The total size of the original queue

    # Sink nodes (deferred execution)
    sink_queue_snapshot: typing.List[QueueTaskTemplate]
    current_sink_queue_size: int  # The total size of the sink queue

    # How many tasks completed before the snapshot
    tasks_processed: int

    snapshot_timestamp: MiniAnnotated[
        float, Attrib(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    ]
    snapshot_version: str = volnux_version

    class Config:
        validation = ValidationFlags.NONE

    @classmethod
    def get_schema_name(cls) -> str:
        return "volnux:snapshot:engine"

    async def restore(self) -> WorkflowEngine:
        pass
