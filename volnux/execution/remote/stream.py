"""
Result streaming for remote tasks, over the messaging backend.

A worker pushes each flushed batch (``{"type": "result_batch"|"error_batch", ...}``)
as a :class:`RemoteBatch` onto a queue and ends the stream with one ``done`` entry.
The requester reads the queue with :class:`BatchStreamReader`. The queue lives in
the ``messaging`` entry of ``DATABASES`` (``MessagingBackendIntegrationMixin``), so
the backend can be changed in the settings; nothing here opens a connection.

The queue is per *task* (state id + task id): two tasks of one state used to share a
queue, so the first sentinel ended the stream while the other task was still writing.
"""

import time
import typing

import orjson
from formax import Attrib, BaseModel, InitStrategy, MiniAnnotated

from volnux.backends.storage_route import StorageRoute
from volnux.mixins.messaging import MessagingBackendIntegrationMixin

__all__ = [
    "BATCH_TTL_SECONDS",
    "batch_ttl_seconds",
    "BatchStreamReader",
    "RemoteBatch",
    "StreamTimeout",
    "batch_key_for",
    "batch_route",
    "done_entry",
]

BATCH_TTL_SECONDS = 3600  # (the default of the REMOTE_BATCH_TTL_SECONDS setting)


def batch_ttl_seconds() -> int:
    """Seconds a stream's queue lives (the REMOTE_BATCH_TTL_SECONDS setting)."""
    from volnux.config import setting

    return int(setting("REMOTE_BATCH_TTL_SECONDS", BATCH_TTL_SECONDS))


_BATCH_KEY_TEMPLATE = "volnux:batches:{state_id}:{task_id}"
DONE = "done"
RESULT_BATCH = "result_batch"
ERROR_BATCH = "error_batch"


class StreamTimeout(TimeoutError):
    """The stream produced no ``done`` entry in time."""


def batch_key_for(state_id: str, task_id: str) -> str:
    """The name of a task's stream (recorded on its ``ContextDelta``). The queue
    itself is addressed with :func:`batch_route`."""
    return _BATCH_KEY_TEMPLATE.format(state_id=state_id, task_id=task_id)


def batch_route(state_id: str, task_id: str) -> typing.Dict[str, str]:
    """The ``route_override`` that addresses one task's queue."""
    return {"state_id": state_id, "task_id": task_id}


class RemoteBatch(MessagingBackendIntegrationMixin, BaseModel):
    """One entry of a task's stream: a batch of results or errors, or the end of
    the stream (``kind == "done"``, with the delta's final ``status``)."""

    class Config:
        init_strategy = InitStrategy.DATACLASS

    kind: str
    data: MiniAnnotated[typing.List[typing.Any], Attrib(default_factory=list)]
    count: MiniAnnotated[int, Attrib(default=0)]
    status: MiniAnnotated[typing.Optional[str], Attrib(default=None)]

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        # no {node_id}: the worker and the requester are different nodes
        return StorageRoute(
            components=["volnux", "remote", "batches", "{state_id}", "{task_id}"],
            routing_keys=lambda: {"state_id": "default", "task_id": "default"},
        )

    @classmethod
    def from_batch(cls, batch: typing.Dict) -> "RemoteBatch":
        # : an event result may hold a value JSON does not know (datetime,
        # Decimal, UUID...); it used to fail the whole flush.
        data = orjson.loads(orjson.dumps(batch.get("data", []), default=str))
        return cls(kind=batch["type"], data=data, count=batch.get("count", len(data)))

    def to_batch(self) -> typing.Dict:
        return {"type": self.kind, "data": self.data, "count": self.count}


def done_entry(status: typing.Optional[str] = None) -> RemoteBatch:
    """The end-of-stream entry. ``status`` is the delta's final status (``None``
    when the task died before it could compute one)."""
    return RemoteBatch(kind=DONE, status=status)


class BatchStreamReader:
    """Reads the batches of one task, in order, up to its ``done`` entry."""

    def __init__(self, state_id: str, task_id: str):
        self.state_id = state_id
        self.task_id = task_id
        self.key = batch_key_for(state_id, task_id)
        self._route = batch_route(state_id, task_id)
        self.status: typing.Optional[str] = None
        self.done = False

    async def batches(self, timeout: float = 30.0) -> typing.AsyncIterator[typing.Dict]:
        """Yield batches until the ``done`` entry (which is not yielded; its status
        is left on ``self.status``). A blocking pop waits for each entry.

        :raises StreamTimeout: no entry arrived within ``timeout`` seconds of the
            last one.
        """
        if timeout <= 0:
            raise ValueError(f"timeout must be > 0, got {timeout!r}")
        while not self.done:
            entry = await RemoteBatch.pop(timeout=timeout, route_override=self._route)
            if entry is None:
                raise StreamTimeout(
                    f"no 'done' entry on {self.key!r} within {timeout}s"
                )
            if entry.kind == DONE:
                self.done = True
                self.status = entry.status
                return
            yield entry.to_batch()

    async def read_all(
        self, timeout: float = 30.0
    ) -> typing.Tuple[typing.List, typing.List]:
        """(results, errors) of the whole stream."""
        results: typing.List = []
        errors: typing.List = []
        async for batch in self.batches(timeout=timeout):
            target = results if batch.get("type") == RESULT_BATCH else errors
            target.extend(batch.get("data", []))
        return results, errors
