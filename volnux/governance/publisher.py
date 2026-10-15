"""Non-blocking delivery of governance events off the engine's hot path.

The reporter runs inside synchronous signal handlers on the execution path, so
it must never block on the transport. It hands each built ``GovernanceEvent`` to
this publisher through a non-blocking ``submit()``; a single background thread
drains the buffer and performs the async send over the provisioned backend.

If the buffer fills or the transport stalls, the engine is unaffected:
``submit()`` never waits, and an overflow drops the event with a warning rather
than back-pressuring a running workflow. Reporting is observational.

The send step is injected as a ``sink`` so the publisher's buffering and
threading can be exercised with a fake, and so the default sink (which enqueues
over the backend) is the only code that depends on a provisioned backend.
"""

import asyncio
import logging
import queue
import threading
import time
from typing import Awaitable, Callable, List, Optional

from .events import GovernanceEvent

logger = logging.getLogger(__name__)

# A sink receives one event and delivers it. Async because the messaging
# backend is async.
SinkType = Callable[[GovernanceEvent], Awaitable[None]]


async def enqueue_governance_event(event: GovernanceEvent) -> None:
    """Default sink: enqueue an event over the provisioned backend.

    The event arrives already built and validated — the reporter constructs it
    in ``_event`` — so there is nothing to normalise here.
    """
    await GovernanceEvent.enqueue(event)


class GovernanceEventPublisher:
    """Buffer governance events and deliver them from a background thread.

    Args:
        sink: Coroutine that delivers one event. Defaults to enqueuing it over
            the provisioned backend.
        max_buffer: Bound on the in-process buffer. On overflow, ``submit`` drops
            the event rather than block.
    """

    def __init__(
        self,
        sink: Optional[SinkType] = None,
        *,
        max_buffer: int = 10000,
        flush_source: Optional[Callable[[], List[GovernanceEvent]]] = None,
        flush_interval: float = 1.0,
    ) -> None:
        self._sink = sink or enqueue_governance_event
        self._buffer: "queue.Queue[GovernanceEvent]" = queue.Queue(maxsize=max_buffer)
        # Optional periodic pull, e.g. draining a sampler's reservoir. Delivered
        # on the same thread as buffered events, so no extra thread is spun up.
        self._flush_source = flush_source
        self._flush_interval = flush_interval
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._started = False

    @property
    def started(self) -> bool:
        return self._started

    def start(self) -> None:
        """Start the delivery thread. Idempotent."""
        if self._started:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="governance-publisher", daemon=True
        )
        self._thread.start()
        self._started = True

    def submit(self, event: GovernanceEvent) -> None:
        """Hand an event to the delivery thread. Never blocks."""
        try:
            self._buffer.put_nowait(event)
        except queue.Full:
            logger.warning(
                "Governance event buffer full; dropping %s",
                event.event_type,
            )

    def _run(self) -> None:
        # One loop for the thread's whole lifetime. ``asyncio.run`` owns its
        # creation and teardown — including cancelling pending tasks and
        # ``shutdown_asyncgens()``, which a bare ``loop.close()`` skips. It must
        # wrap the entire drain, never a single send: a loop per event would
        # stop the messaging backend from reusing its (loop-bound) connections.
        asyncio.run(self._drain())

    async def _drain(self) -> None:
        last_flush = time.monotonic()
        # Keep draining while running, and flush whatever remains on stop.
        while not self._stop.is_set() or not self._buffer.empty():
            if self._flush_source is not None:
                now = time.monotonic()
                if now - last_flush >= self._flush_interval:
                    await self._flush()
                    last_flush = now
            try:
                # Blocking get, so the loop stalls for up to the timeout. There
                # is nothing else scheduled on it, and keeping the buffer a
                # thread-safe ``queue.Queue`` is what lets ``submit`` be called
                # from any engine thread without blocking or raising.
                event = self._buffer.get(timeout=0.2)
            except queue.Empty:
                continue
            await self._deliver(event)
        # Final flush on stop so sampled telemetry is not lost.
        if self._flush_source is not None:
            await self._flush()

    async def _flush(self) -> None:
        try:
            pending = self._flush_source()  # type: ignore[misc]
        except Exception:  # noqa: BLE001 - a flush failure must not kill delivery
            logger.exception("Governance flush source failed")
            return
        for event in pending:
            await self._deliver(event)

    async def _deliver(self, event: GovernanceEvent) -> None:
        try:
            await self._sink(event)
        except Exception:  # noqa: BLE001 - one bad event must not kill delivery
            logger.exception(
                "Failed to publish governance event %s", event.event_type
            )

    def stop(self, *, timeout: float = 5.0) -> None:
        """Signal the thread to flush the buffer and stop. Idempotent."""
        if not self._started:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._started = False
