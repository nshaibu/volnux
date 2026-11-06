"""StreamFlow: run the tasks of a ``|->`` segment together, wired by streams.

``A |-> B |-> C`` is one segment. Its members run at the same time: ``A`` streams
chunks to its stream (see ``StreamingMixin``), ``B`` reads them while ``A`` is still
producing and streams its own output, and so on. ``A |-> (B || C)`` is a fan-out:
``B`` and ``C`` each read every chunk of ``A``.

Wiring
------
The stream of a task is named after the task, so the flow only has to tell each
consumer which task it reads from (``BaseFlow._stream_upstream``); consumers read
through a group of their own.

Failure
-------
* A producer that fails closes its stream as failed; its consumers see that and
  fail on their own, which is what a consumer should report.
* A consumer that fails stops reading, so everything upstream of it is cancelled
  (a producer's ``finally`` still closes its stream).
* The segment's status is the AND of its members.

Cleanup
-------
When every member is done, the streams that were read inside the segment are
deleted. The stream of the last task is kept: a following ``->`` task may
``replay()`` it.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Type

from volnux.executors import BaseExecutor
from volnux.executors.shared import pool_size
from volnux.executors.utils.registry import get_global_executor_registry
from volnux.mixins.stream.queue import StreamChunk
from volnux.parser.operator import PipeType
from .parallel import ParallelFlow

logger = logging.getLogger(__name__)

_GLOBAL_EXECUTOR_REGISTRY = get_global_executor_registry()


def stream_producers_of(task: Any) -> List[Any]:
    """The tasks whose streams ``task`` reads (see ``TaskBase.get_stream_producers``)."""
    return list(task.get_stream_producers())


def _streams(task: Any) -> bool:
    try:
        return bool(getattr(task.get_event_class(), "_volnux_streaming", False))
    except Exception:
        return False


class StreamFlow(ParallelFlow):
    """Runs a stream segment: every member concurrently, streams between them."""

    def __post_init__(self, *args: Any, **kwargs: Any) -> None:
        super().__post_init__(*args, **kwargs)
        members = {p.get_id() for p in self.task_profiles}
        for profile in self.task_profiles:
            producers = stream_producers_of(profile)
            if not producers:
                continue
            self._stream_upstream[profile.get_id()] = [p.get_id() for p in producers]
            for producer in producers:
                # Only producers in this flow can be published here; the tails of a
                # ``{}`` group run in sub-engines, which publish through the engine
                # (see DefaultWorkflowEngine._execute_stream_group).
                if producer.get_id() in members and not _streams(producer):
                    # a plain producer in a fan-in: its result is one chunk
                    self._stream_publish_plain.add(producer.get_id())

    async def get_flow_executor(self, *args: Any, **kwargs: Any) -> Type[BaseExecutor]:
        """The executor chosen in pointy for the segment's last task, else the
        default (async) executor: the members then share the engine's loop, and
        they spend their time waiting on streams, not computing."""
        default = _GLOBAL_EXECUTOR_REGISTRY.get("default")
        decision = self.context.get_decision_task_profile()
        if decision is None:
            return default
        chosen = self.get_task_executor_from_options(decision)
        if chosen is None:
            return default

        # Members that wait on each other need a worker each: on a pool with fewer
        # workers than members, a consumer could hold the only worker while its
        # producer waits in the queue for ever.
        size = pool_size(chosen) if isinstance(chosen, type) else None
        if size is not None and size < len(self.task_profiles):
            logger.warning(
                "The %d tasks of a stream segment cannot all run on the shared %s "
                "pool (%d worker(s)): using the async executor instead",
                len(self.task_profiles),
                chosen.__name__,
                size,
            )
            return default
        return chosen

    def _members_upstream_of(self, task_id: str) -> List[str]:
        """Every task that, directly or not, feeds ``task_id``."""
        found: List[str] = []
        pending = list(self._stream_upstream.get(task_id, []))
        while pending:
            current = pending.pop()
            if current in found:
                continue
            found.append(current)
            pending.extend(self._stream_upstream.get(current, []))
        return found

    async def run(self) -> asyncio.Future:
        group = await super().run()
        futures = self._tracked_futures()

        def on_member_done(task_id: str, future: asyncio.Future) -> None:
            if future.cancelled() or not self._failed(future):
                return
            if task_id not in self._stream_upstream:
                return  # a failing producer is handled by its consumers
            for upstream_id in self._members_upstream_of(task_id):
                self._stop_producer(upstream_id, futures.get(upstream_id))

        for task_id, future in futures.items():
            future.add_done_callback(
                lambda fut, task_id=task_id: on_member_done(task_id, fut)
            )

        return asyncio.ensure_future(self._finish(group))

    #: Seconds a producer gets to notice that its consumer has gone before it is
    #: cancelled outright (it notices between chunks).
    STOP_GRACE = 5.0

    def _stop_producer(self, task_id: str, future: Optional[asyncio.Future]) -> None:
        """Ask a producer whose consumer failed to stop; cancel it if it does not.

        Stopping it lets it close its stream and finish with a result of its own;
        a cancellation would end the whole context as cancelled. A producer that
        runs outside this flow (the tail of a ``{}`` group) is reached through the
        engine, which knows the running streaming events.
        """
        entry = self._initialized_events.get(task_id)
        event = entry[0] if entry is not None else None
        if event is None:
            engine = self.context.get_engine()
            event = engine.get_stream_event(task_id) if engine is not None else None
        if event is not None:
            event._stream_stop = True
        if future is None:
            return
        if future.done():
            return
        logger.warning("A stream consumer failed: stopping its producer '%s'", task_id)
        asyncio.get_running_loop().call_later(
            self.STOP_GRACE, lambda: None if future.done() else future.cancel()
        )

    async def _finish(self, group: asyncio.Future) -> Any:
        try:
            return await group
        finally:
            await self._release_consumed_streams()

    def _tracked_futures(self) -> Dict[str, asyncio.Future]:
        return dict(self._task_futures)

    @staticmethod
    def _failed(future: asyncio.Future) -> bool:
        if future.exception() is not None:
            return True
        result = future.result()
        return bool(getattr(result, "error", False))

    async def _release_consumed_streams(self) -> None:
        """Delete the streams that were read inside the segment (not the last one)."""
        producers = {p for ids in self._stream_upstream.values() for p in ids}
        for producer_id in producers:
            try:
                await StreamChunk.stream_delete(
                    route_override=StreamChunk.route(producer_id)
                )
            except Exception:
                logger.debug(
                    "Could not delete stream of '%s'", producer_id, exc_info=True
                )
