"""Contract tests for :class:`GovernanceEvent`.

These lock down the wire shape that crosses the engine/backend boundary, since
both sides depend on it staying stable: the platform vendors its own copy of
this contract and projects whatever arrives. They are pure — no transport, no
provisioned backend.
"""

import asyncio

from volnux.governance.events import GOVERNANCE_SCHEMA, EventType, GovernanceEvent


# --- Vocabulary -------------------------------------------------------------


def test_event_types_are_namespaced_by_subject():
    # Consumers route on the subject prefix, so the "<subject>.<verb>" shape is
    # part of the contract, not a naming preference.
    assert EventType.EXECUTION_STARTED.startswith("execution.")
    assert EventType.TASK_COMPLETED.startswith("task.")
    assert EventType.HITL_REQUESTED.startswith("hitl.")
    assert EventType.NODE_HEARTBEAT.startswith("node.")


def test_schema_name_is_the_shared_queue_identity():
    # Producer and consumer must agree on this exact string or nothing is
    # delivered; it is keyed off the model, not configured per deployment.
    assert asyncio.run(GovernanceEvent.get_schema_name()) == GOVERNANCE_SCHEMA
    assert GOVERNANCE_SCHEMA == "volnux:governance:events"


# --- Field handling ---------------------------------------------------------


def test_all_fields_round_trip(make_event):
    event = make_event()

    assert event.event_type == EventType.TASK_COMPLETED
    assert event.event_id == "evt-1"
    assert event.occurred_at == 1_700_000_000.5
    assert event.execution_id == "run-1"
    assert event.task_id == "ExtractCustomerData"
    assert event.workflow_id == "wf-1"
    assert event.workflow_name == "Customer ETL"
    assert event.sequence == 3
    assert event.payload == {"status": "completed"}


def test_absent_correlation_stays_none(make_event):
    # A node heartbeat belongs to no run. "Not applicable" must survive as None
    # and never be coerced to the string "None" or flattened to a sentinel the
    # consumer could mistake for a real id.
    event = make_event(
        event_type=EventType.NODE_HEARTBEAT,
        execution_id=None,
        task_id=None,
        workflow_id=None,
        workflow_name=None,
        sequence=None,
        payload={"node_id": "node-a"},
    )

    assert event.execution_id is None
    assert event.task_id is None
    assert event.workflow_id is None
    assert event.workflow_name is None
    assert event.sequence is None


def test_payload_keeps_nested_structure(make_event):
    payload = {
        "status": "completed",
        "rows": 12847,
        "nested": {"a": 1, "b": [1, 2, 3]},
        "empty": None,
        "flag": True,
        "ratio": 1.5,
    }

    event = make_event(payload=payload)

    assert event.payload == payload


def test_empty_payload_is_allowed(make_event):
    # Most lifecycle events carry no payload at all.
    assert make_event(payload={}).payload == {}


def test_provenance_is_populated_from_config(make_event):
    # Governance events are tied to the project that produced them, and name the
    # mesh node that emitted them.
    from volnux.config import VolnuxConfig

    event = make_event()

    assert event.project_id == "test-project"
    assert event.node_id == VolnuxConfig.get_instance().get_node_id()


def test_node_id_is_always_available(make_event):
    # The config generates a node id when the environment supplies none, so this
    # can never be blank — unlike PROJECT_ID it needs no deployment step.
    assert make_event().node_id


# --- Serialisation ----------------------------------------------------------


def test_json_round_trip_preserves_the_payload(make_event):
    event = make_event(
        payload={"prompt": "Approve?", "options": ["approve", "reject"], "n": 3}
    )

    restored = GovernanceEvent.loads(event.dump("json"), "json")

    assert restored.payload == event.payload
    assert restored.event_type == event.event_type
    assert restored.execution_id == event.execution_id


def test_json_round_trip_preserves_absent_correlation(make_event):
    event = make_event(execution_id=None, task_id=None, sequence=None)

    restored = GovernanceEvent.loads(event.dump("json"), "json")

    assert restored.execution_id is None
    assert restored.task_id is None
    assert restored.sequence is None
