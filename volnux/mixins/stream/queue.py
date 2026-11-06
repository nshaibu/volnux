"""The unit that travels on a stream, and where streams live.

``StreamChunk`` is a model on :class:`MessagingBackendIntegrationMixin`, so a stream is
an append-only log on whichever messaging backend the deployment configured (Redis
streams, Kafka). There is one stream per *producing task*: the route carries a
``{stream_id}`` placeholder, and callers address a stream with
``route_override={"stream_id": ...}`` (see ``StreamChunk.route``).

Backend
-------
``DATABASES["streams"]`` selects the backend for streams. When it is not set the
project's ``DATABASES["default"]`` is used, so a project that runs on Redis gets
streams on Redis without more configuration. A backend without stream support
(SQLite, PostgreSQL) fails with a clear ``TypeError`` the first time it is used.
"""

import typing
from datetime import datetime, timezone

from formax import Attrib, BaseModel, MiniAnnotated

from volnux.backends.fields import DateTimeField, DTConfig
from volnux.backends.storage_route import StorageRoute
from volnux.config import VolnuxConfig
from volnux.mixins.messaging import MessagingBackendIntegrationMixin

#: ``StreamChunk.order`` of the end-of-stream marker.
SENTINEL_ORDER = -1


def _scope() -> typing.Dict[str, typing.Optional[str]]:
    config = VolnuxConfig.get_instance()
    try:
        node_id = config.get_node_id()
    except Exception:
        node_id = None
    return {"project_id": config.get("PROJECT_ID", None), "node_id": node_id}


class StreamChunk(MessagingBackendIntegrationMixin, BaseModel):
    """One unit of streaming data, appended to its producer's stream.

    The producing event appends a chunk for every ``(success, data)`` it yields,
    then one sentinel (``order == -1``) whose ``success`` is the stream's final
    status. A consumer reads chunks in order and stops at the sentinel.

    Attributes:
        task_id:     Producing task id; also the stream's id.
        order:       Position in the stream, from 0. ``-1`` is the sentinel.
        success:     The bool the producer yielded with this chunk (for the
                     sentinel, the aggregate status of the whole stream).
        content:     The payload the producer yielded.
        event_name:  Producing event class name (tracing).
        workflow_id: Workflow the chunk belongs to (tracing).
        created_at:  UTC time the chunk was created.
    """

    task_id: str
    order: int
    success: bool
    content: typing.Any
    event_name: str
    workflow_id: MiniAnnotated[typing.Optional[str], Attrib(default=None)]
    created_at: DateTimeField[DTConfig(auto_now_add=True)]

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        return StorageRoute(
            components=[
                "volnux",
                "{project_id}",
                "{node_id}",
                "stream",
                "{stream_id}",
            ],
            routing_keys=lambda: {**_scope(), "stream_id": "default"},
        )

    #: ``DATABASES["streams"]``, else ``"default"``.
    @classmethod
    def get_database_alias(cls) -> str:
        return "streams"

    @staticmethod
    def route(stream_id: str) -> typing.Dict[str, str]:
        """The ``route_override`` that addresses the stream of task ``stream_id``."""
        return {"stream_id": stream_id}

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
        """The end-of-stream marker; ``success`` carries the stream's final status."""
        return cls(
            task_id=task_id,
            order=SENTINEL_ORDER,
            success=exec_status,
            content=None,
            event_name=event_name,
            workflow_id=workflow_id,
        )

    @property
    def is_sentinel(self) -> bool:
        return self.order == SENTINEL_ORDER

    # --- wire form -------------------------------------------------------

    def get_state(self) -> typing.Dict[str, typing.Any]:
        return {
            "id": self.id,
            "task_id": self.task_id,
            "order": self.order,
            "success": self.success,
            "content": self.content,
            "event_name": self.event_name,
            "workflow_id": self.workflow_id,
            "created_at": self.created_at.isoformat(),
        }

    def set_state(self, state: typing.Dict[str, typing.Any]) -> None:
        for key, value in state.items():
            if key in ("id", "_backend_class"):
                continue
            if hasattr(type(self), key):
                setattr(self, key, value)
        if state.get("id"):
            object.__setattr__(self, "_id", state["id"])
