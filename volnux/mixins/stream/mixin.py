import asyncio
import inspect
import logging
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, AsyncGenerator, Callable, Awaitable, List, Optional, Tuple
from .summary import StreamSummary

logger = logging.getLogger(__name__)


class EventPhase(IntEnum):
    INITIALIZED = 0
    COMMUNICATING = 1
    PRE_PROCESS = 2
    PROCESSING = 3
    REDUCING = 4  # streaming events only; transparent no-op for all others
    POST_PROCESS = 5
    COMPLETED = 6


@dataclass
class StreamResultRef:
    """Queue addressing token stored as EventResult.content for streaming events.

    The runtime reads EventResult.error for state machine decisions and
    never inspects this directly. The downstream resolves queue_key via
    EventResult.pop() to consume the stream.

    summary is a statistical description of the streamed data — not raw
    chunks. It is populated incrementally by the reduce loop and finalised
    when the generator is exhausted. Operators and monitoring systems read
    this from the terminal EventResult without replaying the stream.

    Attributes:
        task_id:     Producing task ID.
        queue_key:   Queue name the downstream pops from.
                     Derived via StreamResultRef.queue_key_for(task_id).
        chunk_count: Total chunks pushed to the queue.
        exec_status: AND of all yielded success bools (mirrors EventResult.error).
        complete:    True once all chunks and the sentinel are pushed.
                     The downstream polls this to know when to stop popping.
        summary:     Incremental statistical summary of the streamed data.
                     NumericStreamSummary for int/float streams, otherwise
                     CategoricalStreamSummary. Finalised at generator exhaustion.
    """

    task_id: str
    queue_key: str
    chunk_count: int = 0
    exec_status: bool = True
    complete: bool = False
    summary: StreamSummary = field(default_factory=StreamSummary)

    @staticmethod
    def queue_key_for(task_id: str) -> str:
        return "volnux:stream:%s" % task_id


@dataclass
class StreamReducePolicy:
    """Controls the REDUCING phase behaviour.

    Attributes:
        fail_fast:        Halt the generator on the first success=False chunk.
        max_chunks:       Hard ceiling on accepted chunks. 0 = unlimited.
        checkpoint_every: Checkpoint sub_phase_index every N chunks.
        sample_size:      Maximum chunks kept in StreamResultRef.sample.
        pop_timeout:      Seconds the downstream blocks on each pop() call
                          waiting for the next chunk. 0 = non-blocking.
    """

    fail_fast: bool = True
    max_chunks: int = 0
    checkpoint_every: int = 1
    pop_timeout: float = 5.0


class StreamChunkLimitExceeded(Exception):
    """Raised when the generator exceeds StreamReducePolicy.max_chunks."""


class StreamingNotAllowed(Exception):
    """Raised when a streaming process() is declared on an incompatible event."""


_SENTINEL_ORDER = -1  # EventResult.order value that marks end-of-stream


class StreamingMixin:
    """Adds the REDUCING phase step to EventBase.

    _reduce() is decorated with @phase_step(EventPhase.REDUCING) and called
    by the phase runner between PROCESSING and POST_PROCESS.

    For non-streaming events _reduce() returns immediately — zero overhead.

    For streaming events _reduce() drives self._stream_generator, pushes
    each chunk via EventResult.push(), accumulates exec_status, and updates
    StreamResultRef. When the generator is exhausted it pushes a sentinel
    and writes the final checkpoint with complete=True.

    _process_wrapper companion
    --------------------------
    StreamingMixin overrides _process_wrapper. When _volnux_streaming=True
    it calls process() once to obtain the generator, stores it on
    self._stream_generator, and returns (True, None). _reduce then drives
    the stored generator. process() is called exactly once per execution.

    Resume
    ------
    Generators are not serialisable. On resume self._stream_generator is
    None. _reduce detects sub_phase_index > 0 as the resume signal and
    re-calls process() to obtain a fresh generator. The fast-forward loop
    consumes chunks below sub_phase_index without re-pushing them.
    process() must be a pure replay function — same inputs, same sequence.
    """

    _volnux_streaming: bool = False
    stream_reduce_policy: StreamReducePolicy = StreamReducePolicy()
    _stream_generator: Optional[AsyncGenerator] = None

    @phase_step(EventPhase.REDUCING)
    async def _reduce(self, *args, **kwargs) -> None:
        """REDUCING phase step. No-op for non-streaming events."""
        if not self._volnux_streaming:
            return

        self._assert_not_agent()

        # Resume: generator lost across crash boundary.
        if self._stream_generator is None and self.sub_phase_index > 0:
            logger.info(
                "StreamingMixin '%s': resuming from chunk %d.",
                self.__class__.__name__,
                self.sub_phase_index,
            )
            self._stream_generator = self.process(*args, **kwargs)

        elif self._stream_generator is None:
            raise RuntimeError(
                "StreamingMixin '%s': _reduce entered on a fresh run but "
                "_stream_generator is None. _process_wrapper must store "
                "the generator before REDUCING is entered." % self.__class__.__name__
            )

        policy = self.stream_reduce_policy
        resume_from = self.sub_phase_index

        ref: StreamResultRef = (
            self.exec_result
            if isinstance(self.exec_result, StreamResultRef)
            else StreamResultRef(
                task_id=self._task_id,
                queue_key=StreamResultRef.queue_key_for(self._task_id),
            )
        )
        exec_status = ref.exec_status

        logger.info(
            "StreamingMixin '%s': entering REDUCING (resume_from=%d)",
            self.__class__.__name__,
            resume_from,
        )

        try:
            chunk_index = 0

            async for success, data in self._stream_generator:

                # Fast-forward past already-pushed chunks on resume.
                if chunk_index < resume_from:
                    chunk_index += 1
                    continue

                if policy.max_chunks > 0 and chunk_index >= policy.max_chunks:
                    raise StreamChunkLimitExceeded(
                        "Event '%s' exceeded max_chunks=%d."
                        % (self.__class__.__name__, policy.max_chunks)
                    )

                # Push chunk to the queue — generator pays for this directly.
                await self._push_chunk(chunk_index, success, data)

                exec_status = exec_status and success
                chunk_index += 1

                ref.chunk_count = chunk_index
                ref.exec_status = exec_status
                ref.summary.update(success, data)
                self.exec_result = ref
                self.exec_status = exec_status
                self.sub_phase_index = chunk_index

                if chunk_index % policy.checkpoint_every == 0:
                    await self.enqueue_checkpoint()

                if not success and policy.fail_fast:
                    logger.warning(
                        "StreamingMixin '%s': fail_fast — halting at chunk %d.",
                        self.__class__.__name__,
                        chunk_index,
                    )
                    break

        except StreamChunkLimitExceeded:
            exec_status = False
            logger.error(
                "StreamingMixin '%s': chunk limit exceeded at chunk %d.",
                self.__class__.__name__,
                self.sub_phase_index,
            )
            raise

        except Exception as exc:
            exec_status = False
            logger.exception(
                "StreamingMixin '%s': generator raised at chunk %d: %s",
                self.__class__.__name__,
                self.sub_phase_index,
                exc,
            )
            raise

        finally:
            # Always update state and checkpoint before leaving the phase.
            ref.exec_status = exec_status
            self.exec_result = ref
            self.exec_status = exec_status
            self._stream_generator = None  # release — not serialisable
            await self.enqueue_checkpoint()

        # Generator exhausted cleanly. Finalise summary, push sentinel, mark complete.
        ref.summary.finalise()
        await self._push_sentinel(exec_status)

        ref.complete = True
        ref.exec_status = exec_status
        self.exec_result = ref
        self.exec_status = exec_status
        await self.enqueue_checkpoint()

        logger.info(
            "StreamingMixin '%s': REDUCING complete — %d chunk(s), "
            "exec_status=%s, summary=%s",
            self.__class__.__name__,
            ref.chunk_count,
            exec_status,
            ref.summary.to_dict(),
        )

    async def _push_chunk(self, chunk_index: int, success: bool, data: Any) -> None:
        """Enqueue one chunk via EventResult.push().

        EventResult is used here as the queue message envelope.
        It is not the event's terminal result — that is built by
        _post_process from exec_status + StreamResultRef.
        """
        from volnux.event.result import EventResult

        chunk = EventResult(
            error=not success,
            event_name=self.event_name,
            content=data,
            workflow_id=getattr(self, "workflow_id", None),
            task_id=self._task_id,
            order=chunk_index,
            autosave=False,
            storage_backend=self._storage_backend,
        )
        await EventResult.push(chunk)
        logger.debug(
            "StreamingMixin '%s': pushed chunk %d (success=%s)",
            self.__class__.__name__,
            chunk_index,
            success,
        )

    async def _push_sentinel(self, exec_status: bool) -> None:
        """Push order=-1 sentinel so the downstream knows the stream is done.

        The downstream pops this in sequence after all data chunks and
        stops its pop loop. Pushed after the generator is exhausted and
        after the penultimate checkpoint, so it is always the last item
        in the queue for this task_id.
        """
        from volnux.event.result import EventResult

        sentinel = EventResult(
            error=not exec_status,
            event_name=self.event_name,
            content=None,
            workflow_id=getattr(self, "workflow_id", None),
            task_id=self._task_id,
            order=_SENTINEL_ORDER,
            autosave=False,
            storage_backend=self._storage_backend,
        )
        await EventResult.push(sentinel)
        logger.debug(
            "StreamingMixin '%s': sentinel pushed (exec_status=%s)",
            self.__class__.__name__,
            exec_status,
        )

    # ------------------------------------------------------------------
    # _process_wrapper override
    # ------------------------------------------------------------------

    async def _process_wrapper(self, *args, **kwargs):
        """Override of EventBase._process_wrapper for streaming events.

        When _volnux_streaming=True: calls process() once, stores the
        async generator on self._stream_generator, returns (True, None)
        so _process writes provisional exec_status=True, exec_result=None.
        _reduce overwrites both with the real values after driving the
        generator.

        When _volnux_streaming=False: delegates to super()._process_wrapper
        unchanged — zero impact on non-streaming events.
        """
        if not self._volnux_streaming:
            return await super()._process_wrapper(*args, **kwargs)

        generator = self.process(*args, **kwargs)

        if not inspect.isasyncgen(generator):
            # process() declared as streaming but returned a coroutine.
            # Fall back to the normal result path and warn.
            logger.warning(
                "StreamingMixin '%s': _volnux_streaming=True but process() "
                "did not return an async generator. Falling back.",
                self.__class__.__name__,
            )
            return await generator

        self._stream_generator = generator
        logger.debug(
            "StreamingMixin '%s': generator stored, deferring to REDUCING.",
            self.__class__.__name__,
        )
        # Provisional values — _reduce overwrites after driving the generator.
        return True, None

    # ------------------------------------------------------------------
    # Downstream consumer
    # ------------------------------------------------------------------

    async def input_stream(
        self,
        callback: Callable[["EventResult"], Awaitable[None]],
        timeout: Optional[float] = None,
    ) -> None:
        """Pop chunks from the upstream queue and invoke callback for each.

        Resolves previous_result.content to a StreamResultRef and pops
        from the queue via EventResult.pop(timeout=...) in a loop.

        Blocking pop() provides the I/O wait between chunks — compute is
        not held. The loop ends when the sentinel (order == -1) is popped.

        Args:
            callback: Async callable invoked for each non-sentinel chunk.
            timeout:  Seconds to block on each pop(). Defaults to
                      stream_reduce_policy.pop_timeout. 0 = non-blocking.

        Usage::

            @event()
            async def transform(task):
                chunks = []

                async def collect(result: EventResult) -> None:
                    chunks.append(result.content)

                await task.input_stream(collect)

                for chunk in chunks:
                    yield True, transform_fn(chunk)
        """
        from volnux.event.result import EventResult

        ref = self._resolve_upstream_ref()
        pop_timeout = (
            timeout if timeout is not None else self.stream_reduce_policy.pop_timeout
        )

        logger.info(
            "StreamingMixin '%s': consuming stream from queue '%s'",
            self.__class__.__name__,
            ref.queue_key,
        )

        while True:
            result: Optional[EventResult] = await EventResult.pop(
                timeout=pop_timeout,
                side=QueueSide.LEFT,
            )

            if result is None:
                # Timeout expired with no chunk. Check whether the producer
                # has marked the stream complete. If so the queue is drained
                # and we can exit cleanly without waiting for another timeout.
                if ref.complete:
                    logger.debug(
                        "StreamingMixin '%s': queue drained and stream complete.",
                        self.__class__.__name__,
                    )
                    break
                # Producer still running — keep waiting.
                continue

            if result.order == _SENTINEL_ORDER:
                # Sentinel received — stream is done.
                break

            await callback(result)

    def _resolve_upstream_ref(self) -> StreamResultRef:
        """Extract StreamResultRef from previous_result.

        previous_result is the terminal EventResult from the upstream event.
        Its content is the StreamResultRef carrying the queue_key.
        """
        previous = getattr(self, "previous_result", None)

        if isinstance(previous, StreamResultRef):
            return previous

        content = getattr(previous, "content", previous)
        if isinstance(content, StreamResultRef):
            return content

        if isinstance(content, dict) and "task_id" in content:
            return StreamResultRef(**content)

        raise ValueError(
            "Event '%s': input_stream() called but previous_result does not "
            "contain a StreamResultRef. Ensure the upstream event is a "
            "streaming event." % self.__class__.__name__
        )

    # ------------------------------------------------------------------
    # Guard
    # ------------------------------------------------------------------

    def _assert_not_agent(self) -> None:
        if getattr(self, "_volnux_agent", False):
            raise StreamingNotAllowed(
                "AgentEventBase subclasses may not use streaming process(). "
                "Use a downstream streaming event to fan out agent output."
            )
