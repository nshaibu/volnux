"""
The Redis side of result streaming for remote tasks.

A worker appends each flushed batch (``{"type": "result_batch"|"error_batch", ...}``)
to a Redis list and ends the stream with one ``{"type": "done", ...}`` sentinel. The
requester reads the list with :class:`BatchStreamReader`.

The list is per *task* (state id + task id): two tasks of one state used to share a
list, so the first sentinel ended the stream while the other task was still writing.
"""

import time
import typing

import orjson

__all__ = [
    "BATCH_TTL_SECONDS",
    "batch_ttl_seconds",
    "BatchStreamReader",
    "StreamTimeout",
    "batch_key_for",
    "done_sentinel",
    "encode_batch",
]

BATCH_TTL_SECONDS = 3600  # (the default of the REMOTE_BATCH_TTL_SECONDS setting)


def batch_ttl_seconds() -> int:
    """Seconds a stream's Redis list lives (the REMOTE_BATCH_TTL_SECONDS setting)."""
    from volnux.config import setting

    return int(setting("REMOTE_BATCH_TTL_SECONDS", BATCH_TTL_SECONDS))


_BATCH_KEY_TEMPLATE = "volnux:batches:{state_id}:{task_id}"
DONE = "done"


class StreamTimeout(TimeoutError):
    """The stream produced no ``done`` sentinel in time."""


def batch_key_for(state_id: str, task_id: str) -> str:
    return _BATCH_KEY_TEMPLATE.format(state_id=state_id, task_id=task_id)


def encode_batch(batch: typing.Dict) -> bytes:
    # : an event result may hold a value JSON does not know (datetime,
    # Decimal, UUID...); it used to fail the whole flush.
    return orjson.dumps(batch, default=str)


def done_sentinel(status: typing.Optional[str] = None) -> bytes:
    """The end-of-stream entry. ``status`` is the delta's final status (``None``
    when the task died before it could compute one)."""
    return encode_batch({"type": DONE, "status": status})


class BatchStreamReader:
    """Reads the batches of one task, in order, up to its ``done`` sentinel."""

    def __init__(self, redis_client: typing.Any, key: str):
        self._redis = redis_client
        self.key = key
        self.status: typing.Optional[str] = None
        self.done = False

    def batches(
        self, timeout: float = 30.0, poll: float = 1.0
    ) -> typing.Iterator[typing.Dict]:
        """Yield batches until the sentinel (which is not yielded; its status is
        left on ``self.status``).

        :raises StreamTimeout: no sentinel arrived within ``timeout`` seconds of
            the last entry.
        """
        deadline = time.monotonic() + timeout
        while not self.done:
            item = self._redis.lpop(self.key)
            if item is None:
                if time.monotonic() >= deadline:
                    raise StreamTimeout(
                        f"no 'done' entry on {self.key!r} within {timeout}s"
                    )
                time.sleep(min(poll, 0.05))
                continue
            deadline = time.monotonic() + timeout
            entry = orjson.loads(item)
            if entry.get("type") == DONE:
                self.done = True
                self.status = entry.get("status")
                return
            yield entry

    def read_all(self, timeout: float = 30.0) -> typing.Tuple[typing.List, typing.List]:
        """(results, errors) of the whole stream."""
        results: typing.List = []
        errors: typing.List = []
        for batch in self.batches(timeout=timeout):
            target = results if batch.get("type") == "result_batch" else errors
            target.extend(batch.get("data", []))
        return results, errors
