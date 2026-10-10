import typing
from formax import BaseModel, ValidationFlags, MiniAnnotated, Attrib

from volnux.backends.storage_route import StorageRoute
from volnux.config import VolnuxConfig
from volnux.context import (
    get_current_correlation_id,
    get_current_project_id,
    get_current_workflow_id,
    get_current_node_id,
)
from volnux.backends.fields import DateTimeField, DTConfig
from volnux.mixins.messaging import MessagingBackendIntegrationMixin


project_config = VolnuxConfig.get_instance()


class StreamChunk(MessagingBackendIntegrationMixin, BaseModel):
    """A single unit of streaming data pushed to the queue by a streaming event.

    The producing event's reduce loop calls StreamChunk.push(chunk) for
    each yielded (success, data) pair. The downstream event calls
    StreamChunk.pop() in a loop to consume them.

    StreamChunk is the only entity that interacts with the streaming queue.
    EventResult is never pushed or popped in the streaming path.

    Attributes:
        task_id:      Producing task ID. Determines the queue key.
        order:        Chunk position in the stream (0-indexed).
                      -1 is the reserved sentinel value signalling
                      end-of-stream. The downstream stops popping when
                      it receives order == -1.
        success:      The bool yielded alongside this chunk by process().
        content:      The chunk payload yielded by process().
        event_name:   Name of the producing event class (for tracing).
        workflow_id:  Workflow this chunk belongs to (for tracing).
        created_at:   UTC timestamp of chunk creation.
    """

    task_id: str
    order: int
    success: bool
    content: typing.Any
    event_name: str
    workflow_id: typing.Optional[str]
    created_at: DateTimeField[DTConfig(auto_now=True, tz_aware=True)]

    # project identifier
    node_id: MiniAnnotated[str, Attrib(default_factory=get_current_node_id)]
    project_id: MiniAnnotated[str, Attrib(default_factory=get_current_project_id)]

    class Config:
        validation = ValidationFlags.NONE

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        config = cls.get_volnux_config()
        return StorageRoute(
            components=[
                "volnux",
                "{project_id}",
                "{node_id}",
                "{workflow_id}",
                "{correlation_id}",
                "stream",
                "events",
            ],
            routing_keys=lambda: {
                "project_id": config.PROJECT_ID,
                "node_id": config.get_node_id(),
                "workflow_id": typing.cast(str, get_current_workflow_id()),
                "correlation_id": typing.cast(str, get_current_correlation_id()),
            },
        )

    @classmethod
    def get_backend_config(cls) -> typing.Dict[str, typing.Any]:
        return {
            "ENGINE": "volnux.backends.stores.redis.RedisStoreBackend",
            "CONNECTOR_CONFIG": {
                "host": "localhost",
                "port": 6379,
                "db": 0,
            },
        }

    @classmethod
    def make(
        cls,
        task_id: str,
        order: int,
        success: bool,
        content: typing.Any,
        event_name: str,
        workflow_id: typing.Optional[str] = None,
    ) -> "StreamChunk":
        """Convenience constructor — all required fields in one call."""
        return cls(
            task_id=task_id,
            order=order,
            success=success,
            content=content,
            event_name=event_name,
            workflow_id=workflow_id,
        )

    @classmethod
    def make_sentinel(
        cls,
        task_id: str,
        exec_status: bool,
        event_name: str,
        workflow_id: typing.Optional[str] = None,
    ) -> "StreamChunk":
        """Create the end-of-stream sentinel chunk.

        order=-1 is the reserved sentinel marker. The downstream's pop
        loop stops when it receives a chunk with order == -1.
        success=exec_status carries the producer's final aggregate status
        so the downstream knows whether the full stream succeeded.
        """
        return cls(
            task_id=task_id,
            order=-1,
            success=exec_status,
            content=None,
            event_name=event_name,
            workflow_id=workflow_id,
        )

    @property
    def is_sentinel(self) -> bool:
        return self.order == -1
