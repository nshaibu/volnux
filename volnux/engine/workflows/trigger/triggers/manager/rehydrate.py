import logging
import asyncio
import orjson as json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ..event import Event
from ..base import TriggerBase, TriggerType

if TYPE_CHECKING:
    from ...eventbus.base import EventBusAdapterBase


logger = logging.getLogger(__name__)


class RehydrationManager(TriggerBase):
    """
    Trigger responsible for rehydrating and resuming suspended workflows.

    Replaces the standalone bootloader with a first-class framework
    trigger managed by the TriggerEngine alongside all other triggers.

    Responsibilities
    ----------------
    1. Subscribe to the HITL response event bus channel —
       receives human responses, external event completions,
       and timeout signals.

    2. Maintain the HITL queue — a Redis-backed registry of all
       tasks suspended via communicate() awaiting external input.

    3. On matching event arrival — locate the suspended task's
       queue entry, rehydrate its checkpoint, inject the response
       into the previous_result stream, and resume execution via
       the coordinator.

    4. Poll for timed-out queue entries — injects a timeout response
       and resumes on the timeout descriptor path.

    5. On startup — scan Redis for existing queue entries from
       before the last shutdown. Re-subscribe to their channels
       so no suspension is lost across restarts.

    Integration
    -----------
    Registered in WorkflowConfig.ready() like any other trigger::

        def ready(self):
            self.register_trigger(
                RehydrationManager(
                    workflow_name=self.name,
                    event_bus=self.event_bus,
                    redis_client=self.redis,
                    checkpoint_store=self.checkpoint_store,
                    timeout_poll_interval=60.0,
                )
            )

    Because it inherits TriggerBase, it is visible via the CLI:

        volnux triggers list my_workflow
        NAME                  TYPE                 STATUS
        daily_schedule        schedule             ✅ enabled
        training_requests     event                ✅ enabled
        rehydration_manager   rehydration          ✅ enabled  ← here

    And controllable::

        volnux triggers pause my_workflow rehydration_manager
        # Suspends wake-ups — useful during maintenance windows.
        # Tasks remain in the HITL queue safely; Redis TTLs preserve them.
    """

    trigger_type = TriggerType.MANAGER

    QUEUE_PREFIX = "volnux:hitl:queue"
    RESPONSE_PREFIX = "volnux:hitl:response"
    CLAIM_PREFIX = "volnux:hitl:claim"

    def __init__(
        self,
        workflow_name: str,
        event_bus: "EventBusAdapterBase",
        redis_client,
        checkpoint_store: "VolnuxCheckPointManager",
        coordinator_registry: "CoordinatorRegistry",
        timeout_poll_interval: float = 60.0,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            workflow_name,
            workflow_params=workflow_params,
            enabled=enabled,
            metadata=metadata,
        )
        self._event_bus = event_bus
        self._redis = redis_client
        self._checkpoint_store = checkpoint_store
        self._coordinator_registry = coordinator_registry
        self._timeout_poll_interval = timeout_poll_interval

        # Background tasks managed within the trigger lifecycle
        self._timeout_poll_task: Optional[asyncio.Task] = None
        self._subscribed_channels: List[str] = []

    async def start(self) -> None:
        """
        Start the rehydration manager:
        1. Recover existing suspensions from Redis (crash recovery)
        2. Subscribe to the HITL response channel on the event bus
        3. Start the timeout poller
        """
        logger.info(
            "RehydrationManager '%s': starting",
            self.workflow_name,
        )

        await self._recover_existing_suspensions()

        # Subscribe to HITL response events on the event bus
        # Convention: event type "volnux.hitl.response.{workflow_name}"
        response_event_type = f"volnux.hitl.response.{self.workflow_name}"
        await self._event_bus.subscribe(
            response_event_type,
            self._handle_response_event,
        )
        self._subscribed_channels.append(response_event_type)

        # Subscribe to timeout signals
        timeout_event_type = f"volnux.hitl.timeout.{self.workflow_name}"
        await self._event_bus.subscribe(
            timeout_event_type,
            self._handle_timeout_event,
        )
        self._subscribed_channels.append(timeout_event_type)

        # Start timeout poller as a background task
        self._timeout_poll_task = asyncio.create_task(
            self._timeout_poller(),
            name=f"RehydrationManager.{self.workflow_name}.timeout_poller",
        )

        logger.info(
            "RehydrationManager '%s': started — subscribed to %s, "
            "polling timeouts every %.0fs",
            self.workflow_name,
            self._subscribed_channels,
            self._timeout_poll_interval,
        )

    async def stop(self) -> None:
        """
        Stop the rehydration manager cleanly.
        Unsubscribe from event bus channels, cancel timeout poller.
        Queued entries remain safely in Redis — they will be recovered
        on next start.
        """
        logger.info(
            "RehydrationManager '%s': stopping",
            self.workflow_name,
        )

        for channel in list(self._subscribed_channels):
            try:
                await self._event_bus.unsubscribe(channel, self._handle_response_event)
            except Exception:
                pass
        self._subscribed_channels.clear()

        if self._timeout_poll_task and not self._timeout_poll_task.done():
            self._timeout_poll_task.cancel()
            try:
                await self._timeout_poll_task
            except asyncio.CancelledError:
                pass
        self._timeout_poll_task = None

    async def enqueue(self, entry: "HITLQueueEntry") -> None:
        """
        Add a suspended task to the HITL queue.

        Called by the coordinator when it catches HITLSuspensionRequest.
        Also subscribes to the entry's specific response channel so
        responses arriving on that channel wake the correct task.
        """
        key = self._queue_key(entry.entry_id)
        await self._redis.set(
            key,
            self._serialise(entry),
            ex=self._compute_ttl(entry.timeout_at),
        )

        # Subscribe to this entry's specific response channel
        # so the event bus delivers responses directly
        await self._event_bus.subscribe(
            entry.response_channel,
            self._handle_response_event,
        )
        self._subscribed_channels.append(entry.response_channel)

        logger.info(
            "RehydrationManager '%s': enqueued HITL entry %s "
            "for task %s (channel=%s)",
            self.workflow_name,
            entry.entry_id,
            entry.task_id,
            entry.response_channel,
        )

    async def dequeue(self, entry_id: str) -> None:
        """Remove a completed entry and unsubscribe its channel."""
        entry = await self._get_entry(entry_id)
        if entry and entry.response_channel in self._subscribed_channels:
            await self._event_bus.unsubscribe(
                entry.response_channel,
                self._handle_response_event,
            )
            self._subscribed_channels.remove(entry.response_channel)

        await self._redis.delete(self._queue_key(entry_id))

    async def _handle_response_event(self, event: "Event") -> None:
        """
        Receive a HITL response from the event bus and wake the
        corresponding suspended task.

        The event bus delivers this when:
        - A human submits a response via any configured adapter
          (Slack button click, web UI form, API call, CLI command)
        - An external async event matching a wait_for_event() call arrives
        - A timeout signal fires

        The event.data contains the HITLResponse payload.
        """
        if not self.enabled:
            logger.debug(
                "RehydrationManager '%s': disabled, ignoring response event",
                self.workflow_name,
            )
            return

        try:
            response = HITLResponse(
                request_id=event.data.get("request_id", ""),
                decision=event.data.get("decision", ""),
                data=event.data.get("data", {}),
                reviewer=event.data.get("reviewer", "system"),
                responded_at=event.data.get(
                    "responded_at",
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            await self._wake_workflow(response)

        except Exception:
            logger.exception(
                "RehydrationManager '%s': error handling response event",
                self.workflow_name,
            )

    async def _handle_timeout_event(self, event: "Event") -> None:
        """Receive a timeout signal and inject a timeout response."""
        request_id = event.data.get("request_id", "")
        if not request_id:
            return

        timeout_response = HITLResponse(
            request_id=request_id,
            decision="timeout",
            data={},
            reviewer="system",
            responded_at=datetime.now(timezone.utc).isoformat(),
        )
        await self._wake_workflow(timeout_response)

    async def _wake_workflow(self, response: "HITLResponse") -> None:
        """
        Rehydrate and resume a suspended workflow task.

        This is the equivalent of the bootloader's core logic —
        now running inside the trigger's event handler rather than
        a standalone process.

        Steps:
        1. Find queue entry by request_id
        2. Claim it via Redis SETNX (prevents duplicate processing
           when multiple RehydrationManager instances share Redis)
        3. Load checkpoint snapshot
        4. Inject a response into the previous_result stream
        5. Resume via coordinator
        6. Dequeue entry
        """
        entry = await self._find_entry_by_request_id(response.request_id)
        if entry is None:
            logger.debug(
                "RehydrationManager '%s': no entry for request_id=%s "
                "(may have been processed by another instance)",
                self.workflow_name,
                response.request_id,
            )
            return

        # Distributed claim — prevents double-wake
        claim_key = f"{self.CLAIM_PREFIX}:{entry.entry_id}"
        claimed = await self._redis.set(claim_key, "1", nx=True, ex=30)
        if not claimed:
            logger.debug(
                "RehydrationManager '%s': entry %s already claimed",
                self.workflow_name,
                entry.entry_id,
            )
            return

        logger.info(
            "RehydrationManager '%s': waking task '%s' " "(decision=%s, reviewer=%s)",
            self.workflow_name,
            entry.task_id,
            response.decision,
            response.reviewer,
        )

        try:

            snapshot = await self._checkpoint_store.load(entry.checkpoint_key)

            await self._inject_response(snapshot, response, entry.task_id)

            # The coordinator rehydrates the full workflow state and resumes
            # execution. The COMMUNICATING phase re-runs communicate() which
            # now finds the response in previous_result and returns normally.
            # Then _process runs for the first time with the injected kwargs.
            coordinator = await self._coordinator_registry.get(
                entry.workflow_name, entry.workflow_id
            )
            await coordinator.resume_suspended_task(
                snapshot=snapshot,
                entry=entry,
                response=response,
            )

            await self.dequeue(entry.entry_id)

            # Fire soft signal — observable via OTel and Grafana
            self._emit_rehydration_signal(entry, response)

        except Exception:
            logger.exception(
                "RehydrationManager '%s': failed to wake task '%s'",
                self.workflow_name,
                entry.task_id,
            )
            await self._redis.delete(claim_key)

    async def _timeout_poller(self) -> None:
        """
        Poll for timed-out queue entries every timeout_poll_interval seconds.
        Injects a timeout response — the task resumes on descriptor 2
        (or whichever descriptor the event author maps to timeout handling).
        """
        while True:
            try:
                await asyncio.sleep(self._timeout_poll_interval)
                await self._process_timeouts()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception(
                    "RehydrationManager '%s': error in timeout poller",
                    self.workflow_name,
                )

    async def _process_timeouts(self) -> None:
        now = datetime.now(timezone.utc)
        pattern = f"{self.QUEUE_PREFIX}:{self.workflow_name}:*"

        async for key in self._redis.scan_iter(pattern):
            raw = await self._redis.get(key)
            if not raw:
                continue
            entry = self._deserialise(raw, HITLQueueEntry)
            if not entry.timeout_at:
                continue
            deadline = datetime.fromisoformat(entry.timeout_at)
            if now >= deadline:
                logger.info(
                    "RehydrationManager '%s': entry %s timed out " "(task=%s)",
                    self.workflow_name,
                    entry.entry_id,
                    entry.task_id,
                )
                await self._wake_workflow(
                    HITLResponse(
                        request_id=entry.request_id,
                        decision="timeout",
                        data={},
                        reviewer="system",
                        responded_at=now.isoformat(),
                    )
                )

    async def _recover_existing_suspensions(self) -> None:
        """
        On start, scan Redis for queue entries from before the last shutdown.
        Re-subscribe to their response channels so no suspension is lost.

        This is the equivalent of the bootloader's startup scan —
        now happening inside the trigger's start() method.
        """
        pattern = f"{self.QUEUE_PREFIX}:{self.workflow_name}:*"
        recovered = 0

        async for key in self._redis.scan_iter(pattern):
            raw = await self._redis.get(key)
            if not raw:
                continue
            try:
                entry = self._deserialise(raw, HITLQueueEntry)
                # Re-subscribe to this entry's response channel
                if entry.response_channel not in self._subscribed_channels:
                    await self._event_bus.subscribe(
                        entry.response_channel,
                        self._handle_response_event,
                    )
                    self._subscribed_channels.append(entry.response_channel)
                recovered += 1
            except Exception:
                logger.warning(
                    "RehydrationManager '%s': could not recover entry " "from key %s",
                    self.workflow_name,
                    key,
                )

        if recovered:
            logger.info(
                "RehydrationManager '%s': recovered %d suspended "
                "task(s) from Redis on startup",
                self.workflow_name,
                recovered,
            )

    def _queue_key(self, entry_id: str) -> str:
        return f"{self.QUEUE_PREFIX}:{self.workflow_name}:{entry_id}"

    async def _find_entry_by_request_id(
        self, request_id: str
    ) -> Optional["HITLQueueEntry"]:
        pattern = f"{self.QUEUE_PREFIX}:{self.workflow_name}:*"
        async for key in self._redis.scan_iter(pattern):
            raw = await self._redis.get(key)
            if raw:
                data = json.loads(raw)
                if data.get("request_id") == request_id:
                    return HITLQueueEntry(**data)
        return None

    async def _get_entry(self, entry_id: str) -> Optional["HITLQueueEntry"]:
        raw = await self._redis.get(self._queue_key(entry_id))
        return self._deserialise(raw, HITLQueueEntry) if raw else None

    async def _inject_response(
        self,
        snapshot: Any,
        response: "HITLResponse",
        task_id: str,
    ) -> None:
        """Write the HITLResponse as an EventResult into previous_result."""
        result = EventResult(
            error=False,
            event_name="human_response",
            content={
                "decision": response.decision,
                "data": response.data,
                "reviewer": response.reviewer,
                "responded_at": response.responded_at,
                "type": "human_response",
            },
            task_id=task_id,
        )
        await self._checkpoint_store.inject_result(task_id, result)

    def _emit_rehydration_signal(
        self,
        entry: "HITLQueueEntry",
        response: "HITLResponse",
    ) -> None:
        try:
            from volnux.signal.signals import get_signal

            sig = get_signal("task_rehydrated")
            if sig:
                sig.emit(
                    sender=self.__class__,
                    workflow_name=entry.workflow_name,
                    task_id=entry.task_id,
                    decision=response.decision,
                    reviewer=response.reviewer,
                )
        except Exception:
            pass

    @staticmethod
    def _serialise(obj: Any) -> bytes:
        from dataclasses import asdict

        return json.dumps(asdict(obj))

    @staticmethod
    def _deserialise(raw: str, cls: type) -> Any:
        return cls(**json.loads(raw))

    @staticmethod
    def _compute_ttl(timeout_at: Optional[str]) -> Optional[int]:
        if not timeout_at:
            return None
        deadline = datetime.fromisoformat(timeout_at)
        remaining = (deadline - datetime.now(timezone.utc)).total_seconds()
        # Add a 10-minute grace period so the entry is readable
        # during timeout processing even after the deadline passes
        return max(int(remaining) + 600, 600)
