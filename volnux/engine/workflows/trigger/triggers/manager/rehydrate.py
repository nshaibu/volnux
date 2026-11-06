"""The rehydration manager: brings suspended tasks back when they are ready.

A task that asked the outside world for something was checkpointed and released,
and recorded as an ``ExternalCommunicationQueueEntry``. This trigger listens on the
queue where answers arrive (``ExternalCommunicationResponse``) and, for each, resumes
the workflow the answer belongs to.

Everything it touches goes through the models and so through whichever backends the
deployment configured: the answers travel over the messaging backend, the suspension
records live in the key-value store. It opens no connection of its own.

How a wake-up goes
------------------
1. An answer is taken off the queue. A pop hands each message to exactly one consumer.
2. The entry for its ``request_id`` is looked up. An unknown or already-handled
   request is dropped: delivery is at-least-once, so a duplicate answer is harmless.
3. A ``ResumeClaim`` is taken. Two different answers for one request (a human
   reply racing a timeout) cannot both resume it: exactly one claim succeeds.
4. The entry is marked READY and keeps the answer, so a crash from here on can be
   recovered: on start the manager re-delivers READY entries whose claim is gone.
5. ``resume_handler`` resumes the workflow; the entry is then marked RESUMED (or
   EXPIRED for a timeout), or FAILED, kept for a retry, if resuming raised.

Timeouts are produced here too: a sweeper turns every entry past its ``timeout_at``
into a ``source="timeout"`` answer on the same queue, so a timeout is resumed through
the exact path a human answer takes.

One manager serves a project/node: every workflow shares the queue, and the entry
names the workflow to resume.
"""

import asyncio
import logging
import os
import socket
import time
import uuid
from typing import Any, Awaitable, Callable, Dict, Optional, Set, TYPE_CHECKING

from volnux.mixins.event.communication.datastructures import (
    ExternalCommunicationQueueEntry,
    ExternalCommunicationResponse,
    RequestStatus,
    ResumeClaim,
)
from volnux.signal.signals import task_rehydrated
from ..base import TriggerBase, TriggerType

if TYPE_CHECKING:
    from volnux.engine.base import EngineResult

logger = logging.getLogger(__name__)

ResumeHandler = Callable[
    [ExternalCommunicationQueueEntry, ExternalCommunicationResponse],
    Awaitable["EngineResult"],
]


class WakeOutcome:
    """What became of one answer taken off the queue."""

    #: The workflow was resumed.
    RESUMED = "resumed"
    #: Nothing to do: unknown request, or already resumed / being resumed.
    IGNORED = "ignored"
    #: Another worker holds the claim and is resuming it.
    CLAIMED_ELSEWHERE = "claimed_elsewhere"
    #: Resuming raised; the entry is FAILED and can be retried.
    FAILED = "failed"


class RehydrationManager(TriggerBase):
    """Resume suspended workflows as their answers arrive.

    :param poll_timeout: Seconds one wait for the next answer lasts (also how soon a
        stop is noticed).
    :param timeout_sweep_interval: Seconds between sweeps for requests past their
        ``timeout_at``.
    :param claim_lease_seconds: How long a worker may hold the claim on a request. A
        claim whose worker died is taken over after this.
    :param max_concurrent_resumes: How many workflows may be resumed at once.
    :param max_attempts: How many times one request may be attempted before
        recovery stops re-delivering it. A resume that kills the process every
        time (a poison answer) would otherwise be re-delivered at each start for
        ever; past this it is marked FAILED with the reason.
    :param resume_handler: ``async (entry, response) -> EngineResult``. Defaults to
        ``resume_suspended_workflow``; replaceable for tests or a custom runner.
    """

    trigger_type = TriggerType.MANAGER

    #: A manager serves the node, not one workflow
    WORKFLOW_SCOPE = "*"

    def __init__(
        self,
        *,
        poll_timeout: float = 1.0,
        timeout_sweep_interval: float = 30.0,
        claim_lease_seconds: float = 300.0,
        max_concurrent_resumes: int = 4,
        max_attempts: int = 5,
        resume_handler: Optional[ResumeHandler] = None,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if max_concurrent_resumes < 1:
            raise ValueError("max_concurrent_resumes must be at least 1")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        for name, value in (
            ("poll_timeout", poll_timeout),
            ("timeout_sweep_interval", timeout_sweep_interval),
            ("claim_lease_seconds", claim_lease_seconds),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be positive, got {value!r}")

        super().__init__(
            workflow_name=self.WORKFLOW_SCOPE,
            workflow_params=workflow_params,
            enabled=enabled,
            metadata=metadata,
        )
        self._poll_timeout = poll_timeout
        self._timeout_sweep_interval = timeout_sweep_interval
        self._claim_lease_seconds = claim_lease_seconds
        self._max_attempts = max_attempts
        self._resume_handler = resume_handler
        self._slots = asyncio.Semaphore(max_concurrent_resumes)
        self._owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"

        self._consumer_task: Optional[asyncio.Task] = None
        self._sweeper_task: Optional[asyncio.Task] = None
        self._wake_tasks: Set[asyncio.Task] = set()
        # requests whose timeout answer is on the queue (this run only) -> when it
        # was put there. Forgotten once handled, or after a while in case the
        # answer was lost, so the request is not left waiting for good.
        self._timeouts_enqueued: Dict[str, float] = {}
        self._clock = time.monotonic
        self._timeout_retry_after = max(
            claim_lease_seconds, 10 * timeout_sweep_interval
        )

    @property
    def running(self) -> bool:
        return self._consumer_task is not None and not self._consumer_task.done()

    async def start(self) -> None:
        if self.running:
            return
        await self.recover()
        self._consumer_task = asyncio.create_task(
            self._consume_loop(), name="rehydration-consumer"
        )
        self._sweeper_task = asyncio.create_task(
            self._sweep_loop(), name="rehydration-timeout-sweeper"
        )
        logger.info("RehydrationManager started (%s)", self._owner)

    async def stop(self) -> None:
        tasks = [self._consumer_task, self._sweeper_task, *self._wake_tasks]
        for task in tasks:
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(t for t in tasks if t is not None), return_exceptions=True
        )
        self._consumer_task = None
        self._sweeper_task = None
        self._wake_tasks.clear()
        logger.info("RehydrationManager stopped (%s)", self._owner)

    # --- consuming answers -------------------------------------------------

    async def _consume_loop(self) -> None:
        while True:
            if not self.enabled:
                # Paused from the CLI: leave answers on the queue rather than pop
                # and lose them.
                await asyncio.sleep(self._poll_timeout)
                continue

            await self._slots.acquire()
            try:
                response = await ExternalCommunicationResponse.dequeue(
                    timeout=self._poll_timeout
                )
            except asyncio.CancelledError:
                self._slots.release()
                raise
            except Exception:
                self._slots.release()
                logger.exception("RehydrationManager: could not read the answer queue")
                await asyncio.sleep(1.0)  # a failing backend must not become a hot loop
                continue

            if response is None:
                self._slots.release()
                continue

            task = asyncio.create_task(
                self._run_wake(response), name=f"rehydrate-{response.request_id}"
            )
            self._wake_tasks.add(task)
            task.add_done_callback(self._wake_tasks.discard)

    async def _run_wake(self, response: ExternalCommunicationResponse) -> None:
        try:
            await self.wake(response)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "RehydrationManager: unexpected error handling request %s",
                response.request_id,
            )
        finally:
            self._timeouts_enqueued.pop(response.request_id, None)
            self._slots.release()

    # --- waking one task ---------------------------------------------------

    async def wake(self, response: ExternalCommunicationResponse) -> str:
        """Resume the workflow ``response`` answers; return a :class:`WakeOutcome`."""
        request_id = response.request_id
        entry = await ExternalCommunicationQueueEntry.get_or_none(request_id)
        if entry is None:
            logger.warning(
                "RehydrationManager: answer for unknown request %s dropped", request_id
            )
            return WakeOutcome.IGNORED
        if entry.status not in (
            RequestStatus.WAITING.value,
            RequestStatus.FAILED.value,
        ):
            logger.info(
                "RehydrationManager: request %s is %s; duplicate answer dropped",
                request_id,
                entry.status,
            )
            return WakeOutcome.IGNORED

        claim = await ResumeClaim.acquire(
            request_id, owner=self._owner, lease_seconds=self._claim_lease_seconds
        )
        if claim is None:
            logger.info(
                "RehydrationManager: request %s is being resumed elsewhere", request_id
            )
            return WakeOutcome.CLAIMED_ELSEWHERE

        try:
            # the entry may have moved on between the first read and the claim
            entry = await ExternalCommunicationQueueEntry.get(request_id)
            if entry.status not in (
                RequestStatus.WAITING.value,
                RequestStatus.FAILED.value,
            ):
                return WakeOutcome.IGNORED
            return await self._resume(entry, response)
        finally:
            await claim.release()

    async def _resume(
        self,
        entry: ExternalCommunicationQueueEntry,
        response: ExternalCommunicationResponse,
    ) -> str:
        entry.status = RequestStatus.READY.value
        entry.response = response.dump("dict")  # kept so a crash can re-deliver it
        entry.attempts += 1
        await entry.save()

        try:
            await self._handler()(entry, response)
        except asyncio.CancelledError:
            raise  # stays READY with its answer: recovered on the next start
        except Exception as exc:
            logger.exception(
                "RehydrationManager: resuming request %s failed", entry.request_id
            )
            entry.status = RequestStatus.FAILED.value
            entry.last_error = f"{type(exc).__name__}: {exc}"
            await entry.save()
            return WakeOutcome.FAILED

        entry.status = (
            RequestStatus.EXPIRED.value
            if response.source == "timeout"
            else RequestStatus.RESUMED.value
        )
        entry.last_error = None
        await entry.save()

        await self._announce(entry, response)
        return WakeOutcome.RESUMED

    def _handler(self) -> ResumeHandler:
        if self._resume_handler is not None:
            return self._resume_handler
        # imported here: the resume machinery pulls in the whole engine
        from volnux.engine.resume import resume_suspended_workflow

        return resume_suspended_workflow

    async def _announce(
        self,
        entry: ExternalCommunicationQueueEntry,
        response: ExternalCommunicationResponse,
    ) -> None:
        try:
            await task_rehydrated.emit_async(
                sender=self.__class__,
                workflow_name=entry.workflow_name,
                task_id=entry.task_id,
                request_id=entry.request_id,
                decision=response.decision,
                reviewer=response.reviewer,
            )
        except Exception:  # a listener must not undo a resume
            logger.warning("task_rehydrated listener failed", exc_info=True)

    # --- timeouts ----------------------------------------------------------

    async def _sweep_loop(self) -> None:
        while True:
            try:
                await self.sweep_timeouts()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("RehydrationManager: timeout sweep failed")
            await asyncio.sleep(self._timeout_sweep_interval)

    async def sweep_timeouts(self) -> int:
        """Queue a timeout answer for every waiting request past its deadline.

        Returns how many were queued. A timeout is resumed through the same path as
        any answer, so the task decides what a missing answer means.
        """
        waiting = await ExternalCommunicationQueueEntry.filter(
            status=RequestStatus.WAITING.value
        )
        queued = 0
        now = self._clock()
        for entry in await waiting.to_list():
            if not entry.is_expired():
                continue
            sent_at = self._timeouts_enqueued.get(entry.request_id)
            if sent_at is not None and now - sent_at < self._timeout_retry_after:
                continue
            await ExternalCommunicationResponse.enqueue(
                ExternalCommunicationResponse.timeout(entry.request_id)
            )
            self._timeouts_enqueued[entry.request_id] = now
            queued += 1
        return queued

    # --- recovery ----------------------------------------------------------

    async def recover(self) -> int:
        """Re-deliver answers that were claimed but never finished.

        An entry still READY whose claim is gone or expired belongs to a worker that
        died mid-resume. Its answer was kept on the entry, so it is put back on the
        queue. Returns how many were re-delivered.
        """
        import time

        ready = await ExternalCommunicationQueueEntry.filter(
            status=RequestStatus.READY.value
        )
        redelivered = 0
        for entry in await ready.to_list():
            claim = await ResumeClaim.get_or_none(entry.request_id)
            if claim is not None and claim.expires_at > time.time():
                continue  # a live worker is on it
            if not entry.response:
                continue
            if entry.attempts >= self._max_attempts:
                entry.status = RequestStatus.FAILED.value
                entry.last_error = (
                    f"gave up after {entry.attempts} interrupted attempt(s)"
                )
                await entry.save()
                logger.error(
                    "RehydrationManager: request %s interrupted %d time(s); "
                    "not re-delivering it again",
                    entry.request_id,
                    entry.attempts,
                )
                continue
            # WAITING first: an answer picked up before the entry says so is ignored
            entry.status = RequestStatus.WAITING.value
            await entry.save()
            try:
                await ExternalCommunicationResponse.enqueue(
                    ExternalCommunicationResponse(**entry.response)
                )
            except Exception:
                # Not queued: keep the entry READY so the next recovery retries,
                # instead of a WAITING entry whose answer is nowhere.
                entry.status = RequestStatus.READY.value
                await entry.save()
                raise
            redelivered += 1
        if redelivered:
            logger.warning(
                "RehydrationManager: re-delivered %d interrupted resume(s)", redelivered
            )
        return redelivered

    async def retry_failed(self) -> int:
        """Put the answer of every FAILED entry back on the queue; returns how many."""
        failed = await ExternalCommunicationQueueEntry.filter(
            status=RequestStatus.FAILED.value
        )
        retried = 0
        for entry in await failed.to_list():
            if entry.response:
                await ExternalCommunicationResponse.enqueue(
                    ExternalCommunicationResponse(**entry.response)
                )
                retried += 1
        return retried
