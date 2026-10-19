"""Reservoir sampling for governance telemetry.

Not every event needs to reach the backend (and from there the frontend). A busy
run can fire thousands of task-level pings and node heartbeats; forwarding all of
them floods the transport and the UI with noise. Reservoir sampling keeps a
bounded, uniformly-random *representative* subset of that high-frequency
telemetry instead.

Two classes of event:

* **State-critical** events (execution lifecycle, task completions/failures, HITL,
  decommission) are never sampled. They change governance records and must all
  arrive, so ``offer`` returns them immediately for delivery.

* **Telemetry** events (task started/retried, node heartbeats) go into a
  reservoir. ``drain`` returns the current sample and resets, so a periodic flush
  forwards a representative slice rather than the whole firehose.

The reservoir itself is ``volnux.execution.remote.context.ReservoirSample`` —
the same Algorithm R implementation the remote-execution mini-contexts use —
kept in one place rather than duplicated. Its ``sample()`` does not reset the
window, so ``EventSampler`` discards each reservoir object after draining:
every flush starts a fresh window by construction.

The sampler is pure and thread-safe: ``offer`` is called from the reporter's
signal handlers and ``drain`` from the publisher's flush timer, so both take a
lock. It performs no I/O and needs no backend, which makes it fully testable.
"""

import random
import threading
from typing import Any, Dict, List, Optional

from volnux.execution.remote.context import ReservoirSample

from .events import EventType, GovernanceEvent

# Governance state — always delivered, never sampled. Task completions/failures
# are here (not sampled) so the projector always sees every trace's terminal
# fact; only the high-frequency *progress* pings below are sampled.
DEFAULT_CRITICAL = frozenset(
    {
        EventType.EXECUTION_STARTED,
        EventType.EXECUTION_COMPLETED,
        EventType.EXECUTION_FAILED,
        EventType.EXECUTION_PAUSED,
        EventType.EXECUTION_RESUMED,
        EventType.EXECUTION_STOPPED,
        EventType.TASK_COMPLETED,
        EventType.TASK_FAILED,
        EventType.HITL_REQUESTED,
        EventType.NODE_DECOMMISSIONED,
    }
)

# Terminal execution events. When one passes, the execution's telemetry reservoir
# is flushed first, so the run's sampled progress arrives just before its end.
_TERMINAL = frozenset(
    {
        EventType.EXECUTION_COMPLETED,
        EventType.EXECUTION_FAILED,
        EventType.EXECUTION_STOPPED,
    }
)


class EventSampler:
    """Classify governance events and reservoir-sample the telemetry ones.

    Args:
        capacity: Reservoir size per window, per key. The flushed sample holds at
            most this many telemetry events per execution (or per node) per flush.
        critical: Event types that bypass sampling and are always delivered.
        rng: Injectable RNG for deterministic tests.
    """

    def __init__(
        self,
        *,
        capacity: int = 32,
        critical: frozenset = DEFAULT_CRITICAL,
        rng: Optional[random.Random] = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("reservoir capacity must be >= 1")
        self._capacity = capacity
        self._critical = critical
        self._rng = rng or random.Random()
        self._reservoirs: Dict[Any, ReservoirSample] = {}
        self._lock = threading.Lock()

    def offer(self, event: GovernanceEvent) -> List[GovernanceEvent]:
        """Feed one event in; return the events to deliver *now*.

        Critical events are returned immediately (a terminal one first flushes
        its execution's telemetry). Telemetry events return nothing now — they
        wait in the reservoir until ``drain``.
        """
        event_type = event.event_type
        with self._lock:
            if event_type in self._critical:
                if event_type in _TERMINAL:
                    return self._drain_key(self._key(event)) + [event]
                return [event]

            key = self._key(event)
            reservoir = self._reservoirs.get(key)
            if reservoir is None:
                reservoir = ReservoirSample(k=self._capacity, _rng=self._rng)
                self._reservoirs[key] = reservoir
            reservoir.add(event)
            return []

    def drain(self) -> List[GovernanceEvent]:
        """Flush every reservoir's sample (called periodically) and reset."""
        with self._lock:
            out: List[GovernanceEvent] = []
            for reservoir in self._reservoirs.values():
                out.extend(reservoir.sample())
            self._reservoirs.clear()
            return out

    # -- internals ----------------------------------------------------------

    @staticmethod
    def _key(event: GovernanceEvent) -> Any:
        """Group telemetry so each execution (or node) gets its own fair sample."""
        if event.execution_id:
            return ("exec", event.execution_id)
        node_id = (event.payload or {}).get("node_id")
        subject = (event.event_type or "").split(".", 1)[0]
        return ("node", node_id or subject)

    def _drain_key(self, key: Any) -> List[GovernanceEvent]:
        reservoir = self._reservoirs.pop(key, None)
        return reservoir.sample() if reservoir else []
