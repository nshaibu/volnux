"""Streaming events: a generator ``process()`` whose output flows to the next task.

An event whose ``process`` is a generator (``async def`` + ``yield``, or a plain
generator) is a *streaming* event. Instead of returning one ``(success, data)`` it
yields many, and each is appended to the event's stream (a ``StreamChunk`` on the
messaging backend) as it is produced. A consumer task, linked with ``|->``, reads
the stream while the producer is still running.

An event whose ``process`` is not a generator is untouched: it runs exactly as
before, and ``A |-> B`` with such an ``A`` behaves like ``A -> B``.

Producer
--------
``_process_wrapper`` drives the generator: every yielded ``(success, data)`` becomes
a chunk, ``order`` counts from 0, and at the end a sentinel (``order == -1``) carries
the aggregate status. The event's own result is a ``StreamResultRef`` (a dict:
queue id, chunk count, status, summary), so the runtime evaluates it like any other
result and a ``->`` consumer receives the reference instead of the data.

If the generator raises, or the task is cancelled, a failing sentinel is still
written so a consumer fails instead of waiting for ever.

Consumer
--------
``async for chunk in self.input_stream()`` reads the upstream stream through a
consumer group, in order, acknowledging each chunk after the loop body has handled
it (at-least-once delivery). A chunk whose ``order`` was already handled is skipped,
so redelivery and a restarted producer do not duplicate work. A stage that
transforms the stream is a generator that consumes one::

    async def process(self):
        async for chunk in self.input_stream():
            yield True, transform(chunk.content)

Each consumer has its own group, so two consumers of one stream each see every
chunk. ``self.replay()`` reads the retained log from the beginning (for a task
that was linked with ``->`` and so only received the reference).

Resume
------
A generator cannot be checkpointed. The number of chunks already pushed is kept in
``_stream_pushed`` (checkpointed with the event's attributes); on a restart
``process()`` runs again, and chunks below that count are consumed without being
pushed again. ``process()`` must therefore produce the same sequence for the same
inputs.
"""

import asyncio
import inspect
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from volnux.backends.messaging.base import StreamOffset
from volnux.concurrency.async_utils import to_thread
from .queue import SENTINEL_ORDER, StreamChunk
from .summary import StreamSummary

logger = logging.getLogger(__name__)


class StreamChunkLimitExceeded(Exception):
    """The generator produced more than ``StreamPolicy.max_chunks``."""


class StreamingNotAllowed(Exception):
    """A streaming ``process()`` was declared on an incompatible event."""


class UpstreamStreamFailed(Exception):
    """The stream being consumed ended with a failing status (or was never closed)."""


class StreamIdleTimeout(UpstreamStreamFailed):
    """No chunk and no sentinel arrived within ``StreamPolicy.idle_timeout``."""


@dataclass
class StreamPolicy:
    """How a streaming event writes and reads its stream.

    Attributes:
        fail_fast:        Stop the generator at the first ``success=False`` chunk.
        max_chunks:       Hard ceiling on chunks a producer may push (0: unlimited).
        checkpoint_every: Checkpoint the event every N chunks (0: only at the end).
        max_in_flight:    Backpressure: the producer waits while this many chunks are
                          waiting in the stream (0: unlimited).
        read_batch:       Chunks a consumer fetches per read.
        read_block_ms:    How long one read waits for new chunks.
        idle_timeout:     Seconds a consumer waits with nothing arriving before it
                          gives up (None: wait for ever).
        retain:           Keep the stream after the run, so ``replay()`` can read it.
    """

    fail_fast: bool = True
    max_chunks: int = 0
    checkpoint_every: int = 100
    max_in_flight: int = 0
    read_batch: int = 10
    read_block_ms: int = 1000
    idle_timeout: Optional[float] = 600.0
    retain: bool = True


# Name used by the first draft of this module.
StreamReducePolicy = StreamPolicy


@dataclass
class StreamResultRef:
    """What a streaming event leaves as its result: a pointer, not the data.

    Kept as a plain dict in ``EventResult.content`` (``to_dict``/``from_dict``) so it
    survives persistence. ``stream_id`` addresses the stream; ``summary`` describes
    the data without replaying it.
    """

    task_id: str
    stream_id: str
    chunk_count: int = 0
    exec_status: bool = True
    complete: bool = False
    summary: StreamSummary = field(default_factory=StreamSummary)

    MARKER = "__volnux_stream__"

    @staticmethod
    def stream_id_for(task_id: str) -> str:
        return task_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.MARKER: True,
            "task_id": self.task_id,
            "stream_id": self.stream_id,
            "chunk_count": self.chunk_count,
            "exec_status": self.exec_status,
            "complete": self.complete,
            "summary": self.summary.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StreamResultRef":
        return cls(
            task_id=data["task_id"],
            stream_id=data["stream_id"],
            chunk_count=data.get("chunk_count", 0),
            exec_status=data.get("exec_status", True),
            complete=data.get("complete", False),
        )

    @classmethod
    def is_ref(cls, value: Any) -> bool:
        return isinstance(value, dict) and value.get(cls.MARKER) is True


async def _as_async(generator: Any) -> AsyncIterator[Any]:
    """Iterate an async generator, or a sync one without blocking the loop."""
    if inspect.isasyncgen(generator):
        async for item in generator:
            yield item
        return

    done = object()
    while True:
        item = await to_thread(next, generator, done)
        if item is done:
            return
        yield item


def _split(item: Any) -> Tuple[bool, Any]:
    """A yielded value must be ``(success, data)``."""
    if not (isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], bool)):
        raise ValueError(
            "A streaming process() must yield (success: bool, data) tuples, "
            f"got {item!r}"
        )
    return item


class StreamingMixin:
    """The streaming half of ``EventBase`` (see the module docstring)."""

    #: Set by ``EventMeta`` when ``process`` is a generator function.
    _volnux_streaming: bool = False
    stream_policy: StreamPolicy = StreamPolicy()

    # Wiring, set by the flow that links this event into a stream segment.
    _stream_id: Optional[str] = None  # where this event writes
    _upstream_stream_id: Optional[str] = None  # where it reads (one source)
    _upstream_stream_ids: Optional[List[str]] = None  # ... or several (fan-in)
    _consumer_group: Optional[str] = None  # its group on the upstream stream

    # Progress, checkpointed with the event's attributes.
    _stream_pushed: int = 0
    _stream_consumed: Optional[Dict[str, int]] = None  # stream id -> last order

    #: Set by the flow when everything reading this stream has stopped.
    _stream_stop: bool = False

    # --- producer --------------------------------------------------------

    @property
    def stream_id(self) -> str:
        return self._stream_id or StreamResultRef.stream_id_for(self._task_id)

    def _stream_route(self, stream_id: Optional[str] = None) -> Dict[str, str]:
        return StreamChunk.route(stream_id or self.stream_id)

    async def _process_wrapper(self, *args: Any, **kwargs: Any):
        if not self._volnux_streaming:
            return await super()._process_wrapper(*args, **kwargs)  # type: ignore[misc]
        self._assert_not_agent()
        return await self._produce(*args, **kwargs)

    async def _produce(self, *args: Any, **kwargs: Any) -> Tuple[bool, Dict[str, Any]]:
        policy = self.stream_policy
        ref = StreamResultRef(task_id=self._task_id, stream_id=self.stream_id)
        exec_status = True
        completed = False
        resume_from = self._stream_pushed
        index = 0

        generator = self.process(*args, **kwargs)  # type: ignore[attr-defined]
        if not (inspect.isasyncgen(generator) or inspect.isgenerator(generator)):
            raise StreamingNotAllowed(
                f"'{type(self).__name__}.process' is marked as streaming but did not "
                "return a generator"
            )

        try:
            async for item in _as_async(generator):
                success, data = _split(item)

                if policy.max_chunks and index >= policy.max_chunks:
                    raise StreamChunkLimitExceeded(
                        f"'{type(self).__name__}' exceeded max_chunks={policy.max_chunks}"
                    )

                ref.summary.update(success, data)
                exec_status = exec_status and success

                if index >= resume_from:  # chunks below were pushed before a restart
                    await self._wait_for_room()
                    await self._push(index, success, data)
                    self._stream_pushed = index + 1
                index += 1

                ref.chunk_count = index
                ref.exec_status = exec_status
                if policy.checkpoint_every and index % policy.checkpoint_every == 0:
                    await self.enqueue_checkpoint()  # type: ignore[attr-defined]

                if self._stream_stop:
                    logger.info(
                        "'%s': stopping the stream at chunk %d, its consumer is gone",
                        type(self).__name__,
                        index,
                    )
                    exec_status = False
                    ref.exec_status = False
                    break

                if not success and policy.fail_fast:
                    logger.warning(
                        "'%s': fail_fast, stopping the stream at chunk %d",
                        type(self).__name__,
                        index,
                    )
                    break
            completed = True
        except BaseException:
            exec_status = False
            raise
        finally:
            # A stream is always closed, so no consumer waits on one that is gone.
            if not completed:
                exec_status = False
            try:
                await asyncio.shield(self._close_stream(exec_status))
            except BaseException:  # best effort; never mask the real error
                logger.exception("Could not close the stream of '%s'", self._task_id)

        ref.summary.finalise()
        ref.exec_status = exec_status
        ref.complete = True
        return exec_status, ref.to_dict()

    async def publish_single(self, success: bool, data: Any) -> None:
        """Publish a plain (non-generator) event's result as a stream of one chunk.

        Used when such an event feeds a stream consumer next to producers that
        stream (``(Streaming || Plain) |-> Consumer``): the consumer then reads
        one merged stream whatever kind each producer is.
        """
        await self._push(0, success, data)
        await self._close_stream(success)

    async def _push(self, order: int, success: bool, data: Any) -> None:
        chunk = StreamChunk.make(
            task_id=self._task_id,
            order=order,
            success=success,
            content=data,
            event_name=type(self).__name__,
            workflow_id=getattr(self, "workflow_id", None),
        )
        await StreamChunk.stream_append(chunk, route_override=self._stream_route())

    async def _close_stream(self, exec_status: bool) -> None:
        sentinel = StreamChunk.make_sentinel(
            self._task_id,
            exec_status,
            type(self).__name__,
            getattr(self, "workflow_id", None),
        )
        await StreamChunk.stream_append(sentinel, route_override=self._stream_route())

    async def _wait_for_room(self) -> None:
        """Backpressure: wait while ``max_in_flight`` chunks are still unread."""
        limit = self.stream_policy.max_in_flight
        if not limit:
            return
        while (
            await StreamChunk.stream_length(route_override=self._stream_route())
            >= limit
        ):
            await asyncio.sleep(0.01)

    # --- consumer --------------------------------------------------------

    def _upstream_ids(self) -> List[str]:
        """The streams this event reads: the ones the flow wired, else those a
        ``->`` predecessor left references to."""
        if self._upstream_stream_ids:
            return list(self._upstream_stream_ids)
        if self._upstream_stream_id:
            return [self._upstream_stream_id]
        refs = self._upstream_refs()
        if refs:
            return [ref.stream_id for ref in refs]
        raise ValueError(
            f"'{type(self).__name__}' reads a stream but has no upstream: link it "
            "to a streaming event with '|->'"
        )

    def _upstream_refs(self) -> List[StreamResultRef]:
        """The references left by streaming predecessors (``->`` after generators)."""
        previous = getattr(self, "previous_result", None)
        if previous is None:
            return []
        items = list(previous) if hasattr(previous, "__iter__") else [previous]
        refs = []
        for item in items:
            content = getattr(item, "content", item)
            if StreamResultRef.is_ref(content):
                refs.append(StreamResultRef.from_dict(content))
        return refs

    def _upstream_ref(self) -> Optional[StreamResultRef]:
        """The first reference left by a streaming predecessor, if any."""
        refs = self._upstream_refs()
        return refs[0] if refs else None

    async def input_stream(self) -> AsyncIterator[StreamChunk]:
        """Chunks of the upstream stream(s), as they arrive.

        With one upstream the chunks come in order. With several (fan-in,
        ``(A || B) |-> C``) they are merged in arrival order: each source keeps its
        own order, ``chunk.task_id`` says which source a chunk came from, and the
        stream ends when every source has ended.

        Raises :class:`UpstreamStreamFailed` as soon as any source ends failing, and
        :class:`StreamIdleTimeout` if a source goes quiet for
        ``stream_policy.idle_timeout`` without ending.
        """
        group = self._consumer_group or f"consumer:{self._task_id}"
        async for chunk in self._read_streams(self._upstream_ids(), group, resume=True):
            yield chunk

    async def input_data(self) -> AsyncIterator[Any]:
        """Like :meth:`input_stream` but yields just each chunk's payload."""
        async for chunk in self.input_stream():
            yield chunk.content

    async def replay(self) -> AsyncIterator[StreamChunk]:
        """Read the whole retained stream(s) from the first chunk (fresh groups).

        For a task linked with ``->``, which receives only the references. Bounded by
        what the backend still retains.
        """
        # a fresh group every time, so each replay starts at the first chunk
        group = f"replay:{self._task_id}:{uuid.uuid4().hex[:8]}"
        async for chunk in self._read_streams(
            self._upstream_ids(), group, resume=False
        ):
            yield chunk

    def _progress(self) -> Dict[str, int]:
        """Last chunk handled per upstream stream (checkpointed with the event)."""
        if not isinstance(self._stream_consumed, dict):
            self._stream_consumed = {}
        return self._stream_consumed

    async def _read_streams(
        self, stream_ids: List[str], group: str, *, resume: bool
    ) -> AsyncIterator[StreamChunk]:
        if len(stream_ids) == 1:
            async for chunk in self._read_stream(stream_ids[0], group, resume=resume):
                yield chunk
            return

        # Fan-in: one reader per source feeds a queue; a source waits until its last
        # chunk was handled, so each chunk is acknowledged after the consumer is done
        # with it, exactly as with one source.
        queue: "asyncio.Queue[Tuple[Optional[StreamChunk], Optional[BaseException], Optional[asyncio.Event]]]" = (asyncio.Queue())

        async def pump(stream_id: str) -> None:
            try:
                async for chunk in self._read_stream(stream_id, group, resume=resume):
                    handled = asyncio.Event()
                    await queue.put((chunk, None, handled))
                    await handled.wait()
                await queue.put((None, None, None))  # this source ended cleanly
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                await queue.put((None, exc, None))

        pumps = [asyncio.ensure_future(pump(stream_id)) for stream_id in stream_ids]
        remaining = len(pumps)
        try:
            while remaining:
                chunk, error, handled = await queue.get()
                if error is not None:
                    raise error
                if chunk is None:
                    remaining -= 1
                    continue
                yield chunk
                handled.set()  # type: ignore[union-attr]
        finally:
            for task in pumps:
                task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)

    async def _read_stream(
        self, stream_id: str, group: str, *, resume: bool
    ) -> AsyncIterator[StreamChunk]:
        policy = self.stream_policy
        route = StreamChunk.route(stream_id)
        consumer = f"{self._task_id}"
        await StreamChunk.stream_ensure_consumer_group(
            group, StreamOffset.BEGINNING, route_override=route
        )

        progress = self._progress()
        last = progress.get(stream_id, -1) if resume else -1
        idle = 0.0
        if resume and last >= 0:
            # A restart: take back what a previous attempt read but never acknowledged.
            claimed = await StreamChunk.stream_claim_pending(
                group, consumer, min_idle_ms=0, route_override=route
            )
            # in stream order, the end-of-stream marker (order -1) last
            for entry in sorted(
                claimed, key=lambda e: (e.record.is_sentinel, e.record.order)
            ):
                chunk = entry.record
                if chunk.is_sentinel:
                    await self._ack(group, route, entry.entry_id)
                    self._finish(chunk)
                    return
                if chunk.order <= last:
                    await self._ack(group, route, entry.entry_id)
                    continue
                yield chunk
                last = chunk.order
                progress[stream_id] = last
                await self._ack(group, route, entry.entry_id)

        while True:
            entries = await StreamChunk.stream_read(
                group,
                consumer,
                count=policy.read_batch,
                block_ms=policy.read_block_ms,
                route_override=route,
            )
            if not entries:
                idle += policy.read_block_ms / 1000
                if policy.idle_timeout is not None and idle >= policy.idle_timeout:
                    raise StreamIdleTimeout(
                        f"No data on stream '{stream_id}' for {policy.idle_timeout:g}s "
                        "and it was not closed"
                    )
                continue
            idle = 0.0

            for entry in entries:
                chunk = entry.record
                if chunk.is_sentinel:
                    await self._ack(group, route, entry.entry_id)
                    self._finish(chunk)
                    return
                if chunk.order <= last:  # a redelivery, or a restarted producer
                    await self._ack(group, route, entry.entry_id)
                    continue

                yield chunk
                # the loop body has handled the chunk: now it counts as consumed
                last = chunk.order
                if resume:
                    progress[stream_id] = last
                await self._ack(group, route, entry.entry_id)

    @staticmethod
    async def _ack(group: str, route: Dict[str, str], entry_id: str) -> None:
        await StreamChunk.stream_ack(group, entry_id, route_override=route)

    @staticmethod
    def _finish(sentinel: StreamChunk) -> None:
        if not sentinel.success:
            raise UpstreamStreamFailed(
                f"The stream of task '{sentinel.task_id}' ended with a failure"
            )

    # --- housekeeping ----------------------------------------------------

    async def release_stream(self) -> bool:
        """Delete this event's own stream (consumers are done with it)."""
        return await StreamChunk.stream_delete(route_override=self._stream_route())

    def _assert_not_agent(self) -> None:
        if getattr(self, "_volnux_agent", False):
            raise StreamingNotAllowed(
                "AgentEventBase subclasses may not use a streaming process(). "
                "Use a downstream streaming event to fan out agent output."
            )


__all__ = [
    "SENTINEL_ORDER",
    "StreamChunkLimitExceeded",
    "StreamIdleTimeout",
    "StreamPolicy",
    "StreamReducePolicy",
    "StreamResultRef",
    "StreamingMixin",
    "StreamingNotAllowed",
    "UpstreamStreamFailed",
]
