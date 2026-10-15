"""Outbound governance event feed.

This package is the engine's *outbound* side of the boundary with the Volnux
platform (control-plane) backend. The engine reports facts about what it is
doing — executions starting and finishing, tasks running, human-in-the-loop
pauses, node heartbeats — as ``GovernanceEvent`` messages. A separate backend
process consumes them and materializes governance records (``Execution``,
``ExecutionTrace``, ``HITLRequest``, ``NodeHeartbeat``, audit entries, ...).

Design boundary
---------------
The engine emits only *execution/task/node facts*. It holds no governance
concepts — organizations, users, approvals, tenancy — those are supplied and
resolved entirely by the consumer. Keeping this direction one-way and free of
governance semantics is what lets the engine run standalone with no backend at
all: if nothing is publishing (the reporter is not installed), the signals the
engine already fires simply have no listener and the engine behaves as before.

Transport
---------
Events are delivered over the engine's pluggable messaging backend, not a
hard-wired transport: ``GovernanceEvent`` is a messaging-integrated model
(keyed by ``get_schema_name()``), so whatever the deployment provisions
(Redis, Postgres, in-memory, ...) carries it. Delivery is durable (an enqueued
event survives the backend being down and is drained on recovery) and happens
off the engine's hot path: the reporter hands each event to a
``GovernanceEventPublisher`` whose background thread performs the send, so a
slow or failing transport never blocks a running workflow.
"""

from .events import GOVERNANCE_SCHEMA, EventType, GovernanceEvent
from .publisher import GovernanceEventPublisher, enqueue_governance_event
from .reporter import SignalGovernanceReporter, install_governance_reporter
from .sampling import DEFAULT_CRITICAL, EventSampler, ReservoirSampler

__all__ = [
    "EventType",
    "GovernanceEvent",
    "GOVERNANCE_SCHEMA",
    "GovernanceEventPublisher",
    "enqueue_governance_event",
    "SignalGovernanceReporter",
    "install_governance_reporter",
    "EventSampler",
    "ReservoirSampler",
    "DEFAULT_CRITICAL",
]
