"""The persisted records behind a suspended task.

A task that asks the outside world for something (a human decision, an external
event, a condition) is checkpointed and released. Three records tie that
suspension to its resumption, all stored through the backends the deployment
configured (no storage is hard-wired here):

* ``ExternalCommunicationQueueEntry``: the *source of truth* for one suspension,
  a key-value record keyed by ``request_id``. It says which workflow, task,
  checkpoint and engine state to resume, and tracks where the suspension is.
* ``ExternalCommunicationResponse``: the answer (or a timeout), delivered over the
  messaging backend. The rehydration manager consumes these from a queue.
* ``ResumeClaim``: a short lease, one per request, so exactly one worker resumes a
  given suspension even when several responses arrive.

The pattern follows ``GovernanceEvent`` and ``DeadLetterEntry``.
"""

import enum
import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from formax import Attrib, BaseModel, MiniAnnotated

from volnux.backends.storage_route import StorageRoute
from volnux.config import VolnuxConfig
from volnux.exceptions import ObjectDoesNotExist, ObjectExistError
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin
from volnux.mixins.messaging import MessagingBackendIntegrationMixin


def _scope() -> Dict[str, Optional[str]]:
    """Project and node of this process; ``None`` when not configured (the route
    component is then dropped, as for a standalone engine)."""
    config = VolnuxConfig.get_instance()
    try:
        node_id = config.get_node_id()
    except Exception:
        node_id = None
    return {"project_id": config.get("PROJECT_ID", None), "node_id": node_id}


def _route(*tail: str) -> StorageRoute:
    return StorageRoute(
        components=[
            "volnux",
            "{project_id}",
            "{node_id}",
            "external_communication",
            *tail,
        ],
        routing_keys=_scope,
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


#: ``event_name`` of the result injected into a resumed task's ``previous_result``,
#: by kind of request.
RESPONSE_EVENT_NAMES = {
    "hitl": "human_response",
    "event": "external_event",
    "condition": "condition_met",
}


class RequestStatus(str, enum.Enum):
    """Where one suspension is in its life."""

    #: Suspended; nothing has answered yet.
    WAITING = "waiting"
    #: An answer was claimed and is being resumed.
    READY = "ready"
    #: The workflow was resumed with the answer.
    RESUMED = "resumed"
    #: The request timed out and was resumed with a timeout answer.
    EXPIRED = "expired"
    #: Resuming failed; the entry is kept so it can be retried.
    FAILED = "failed"


class ExternalCommunicationQueueEntry(KeyValueStoreIntegrationMixin, BaseModel):
    """One suspended task, keyed by ``request_id``.

    Build it with :meth:`create` so the record id is the request id.

    :ivar request_id: Matches ``ExternalCommunicationSuspensionRequest.request_id``.
    :ivar workflow_name: Name of the workflow (the pipeline class) to resume.
    :ivar workflow_id: Id of that run (the pipeline id).
    :ivar task_id: The suspended task.
    :ivar request_type: ``"hitl"``, ``"event"`` or ``"condition"``.
    :ivar checkpoint_key: Id of the ``EventCheckpointSnapshot`` of the suspended event.
    :ivar context_id: ``state_id`` of the suspended ``ExecutionContext``; it is also
        the key of the saved ``EngineStateSnapshot``.
    :ivar request_title: Title shown to whoever must answer.
    :ivar request_payload: Data the answerer needs.
    :ivar options: Allowed answers, if the request is a choice.
    :ivar timeout_at: ISO timestamp after which the request expires (``None``: never).
    :ivar status: A :class:`RequestStatus` value.
    :ivar response: The answer that was claimed, kept so a crash can re-deliver it.
    :ivar attempts: How many times resuming was attempted.
    :ivar last_error: The error of the last failed attempt.
    """

    request_id: str
    workflow_name: str
    workflow_id: str
    task_id: str
    request_type: str
    checkpoint_key: str
    context_id: str
    request_title: str
    request_payload: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]
    options: Optional[List[str]] = None
    timeout_at: Optional[str] = None
    status: str = RequestStatus.WAITING.value
    response: Optional[Dict[str, Any]] = None
    attempts: int = 0
    last_error: Optional[str] = None
    enqueued_at: MiniAnnotated[Optional[str], Attrib(default=None)]

    def __post_init__(self, *args: Any, **kwargs: Any) -> None:
        super().__post_init__(*args, **kwargs)
        # Stamped per instance here: a ``default_factory`` is evaluated once, so
        # every entry would carry the moment the module was imported.
        if self.enqueued_at is None:
            self.enqueued_at = _now_iso()

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        return _route("requests")

    @classmethod
    def create(cls, **fields: Any) -> "ExternalCommunicationQueueEntry":
        """Build an entry whose record id is its ``request_id``."""
        entry = cls(**fields)
        entry.change_object_id(entry.request_id)
        return entry

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        """True when ``timeout_at`` has passed (never, if there is no timeout)."""
        if not self.timeout_at:
            return False
        deadline = datetime.fromisoformat(self.timeout_at)
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        return (now or datetime.now(timezone.utc)) >= deadline


class ExternalCommunicationResponse(MessagingBackendIntegrationMixin, BaseModel):
    """The answer to a suspension, delivered over the messaging backend.

    ``source`` says where it came from: ``"human"``, ``"event"``, ``"condition"``
    or ``"timeout"`` (synthesised when a request expires).
    """

    request_id: str
    decision: str
    data: MiniAnnotated[Dict[str, Any], Attrib(default_factory=dict)]
    reviewer: str = ""
    source: str = "human"
    responded_at: MiniAnnotated[Optional[str], Attrib(default=None)]

    def __post_init__(self, *args: Any, **kwargs: Any) -> None:
        super().__post_init__(*args, **kwargs)
        # Stamped per instance here: a ``default_factory`` is evaluated once, so
        # every response would carry the moment the module was imported.
        if self.responded_at is None:
            self.responded_at = _now_iso()

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        return _route("responses")

    @classmethod
    def timeout(cls, request_id: str) -> "ExternalCommunicationResponse":
        """The answer a request that ran out of time is resumed with."""
        return cls(
            request_id=request_id,
            decision="timeout",
            data={},
            reviewer="system",
            source="timeout",
        )

    def get_state(self) -> Dict[str, Any]:
        """Wire state: the declared fields plus the record id (the queue needs it)."""
        return {**self.dump("dict"), "id": self.id}

    def set_state(self, state: Dict[str, Any]) -> None:
        """Restore from wire state; unknown keys are ignored so an older consumer
        can read what a newer producer wrote."""
        for key, value in state.items():
            if key in ("id", "_backend_class"):
                continue
            if hasattr(type(self), key):
                setattr(self, key, value)
        if state.get("id"):
            object.__setattr__(self, "_id", state["id"])


class ResumeClaim(KeyValueStoreIntegrationMixin, BaseModel):
    """A lease on resuming one request, keyed by ``request_id``.

    ``insert`` never overwrites, so of several workers racing to claim a request
    exactly one succeeds. The lease is also recorded in the record
    (``expires_at``) because not every store honours TTLs: a claim whose worker
    died is taken over once it has expired.
    """

    request_id: str
    owner: str
    claimed_at: float
    expires_at: float

    @classmethod
    def get_storage_route(cls) -> StorageRoute:
        return _route("claims")

    @classmethod
    async def acquire(
        cls,
        request_id: str,
        owner: str,
        lease_seconds: float,
        now: Optional[float] = None,
    ) -> Optional["ResumeClaim"]:
        """Claim ``request_id``; ``None`` if someone else holds a live claim."""
        now = time.time() if now is None else now
        claim = cls(
            request_id=request_id,
            owner=owner,
            claimed_at=now,
            expires_at=now + lease_seconds,
        )
        claim.change_object_id(request_id)

        for _ in range(2):
            try:
                await claim.save(
                    force_insert=True, ttl=max(1, math.ceil(lease_seconds))
                )
                return claim
            except ObjectExistError:
                existing = await cls.get_or_none(request_id)
                if existing is not None and existing.expires_at > now:
                    return None
                if existing is not None:  # the previous holder's lease ran out
                    try:
                        await existing.delete()
                    except ObjectDoesNotExist:
                        pass
        return None

    async def release(self) -> None:
        """Give the claim up (idempotent)."""
        try:
            await self.delete()
        except ObjectDoesNotExist:
            pass
