"""Tests for rehydrator/engine/deserializer — StateDeserializer.

Covers:
  - deserialize_engine_state (dict -> EngineStateSnapshot).
  - deserialize_task (dict -> TaskSnapshot).
  - rebuild_task (serialized task graph -> live tasks): options, namespace,
    descriptor names, branches, sink subtree and groups, through JSON.

An ExecutionContext needs no deserializer; its fields restore themselves.
"""

from __future__ import annotations

import json
from typing import Any, Dict

import pytest

from volnux import EventBase
from volnux.execution.rehydrator.engine.deserializer import StateDeserializer
from volnux.execution.rehydrator.engine.serializer import StateSerializer
from volnux.execution.rehydrator.engine.snapshot import (
    EngineStateSnapshot,
    TaskSnapshot,
)
from volnux.parser.operator import PipeType
from volnux.parser.options import StopCondition
from volnux.task import PipelineTask, PipelineTaskGrouping, build_pipeline_flow_from_pointy_code


class StepA(EventBase):
    def process(self, *args, **kwargs):
        return True, None


class StepB(StepA):
    pass


class StepC(StepA):
    pass


class StepS(StepA):
    pass


def _make_engine_state_data(**overrides: Any) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "context_id": "ctx-1",
        "engine_class_path": "volnux.engine.base.WorkflowEngine",
        "current_task": None,
        "task_queue_snapshot": [],
        "queue_index": 0,
        "current_task_queue_size": 0,
        "sink_queue_snapshot": [],
        "current_sink_queue_size": 0,
        "tasks_processed": 0,
    }
    defaults.update(overrides)
    return defaults


def _make_task_data(**overrides: Any) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "context_id": "ctx-td",
        "task_id": "task-deser",
        "task_type": "normal",
        "task_checkpoint": None,
        "event_name": "process_data",
        "event_class_import_path": "myapp.events.ProcessDataTask",
        "sink_task_id": None,
        "sink_task_pipe": None,
        "options": {"size": 10},
        "condition_node": {},
        "sequence_number": 0,
        "descriptor": None,
        "descriptor_pipe_type": None,
    }
    defaults.update(overrides)
    return defaults


class TestDeserializeEngineState:

    def test_from_dict(self) -> None:
        state = StateDeserializer.deserialize_engine_state(_make_engine_state_data())

        assert isinstance(state, EngineStateSnapshot)
        assert state.context_id == "ctx-1"
        assert state.engine_class_path == "volnux.engine.base.WorkflowEngine"

    def test_queues_stay_plain_entries(self) -> None:
        entry = {"task_id": "t1", "previous_context_id": "c0", "position_in_queue": 0}
        state = StateDeserializer.deserialize_engine_state(
            _make_engine_state_data(
                task_queue_snapshot=[entry],
                current_task=entry,
                current_task_queue_size=1,
                tasks_processed=3,
            )
        )

        assert state.task_queue_snapshot == [entry]
        assert state.current_task == entry
        assert state.tasks_processed == 3


class TestDeserializeTask:

    def test_from_dict(self) -> None:
        snapshot = StateDeserializer.deserialize_task(_make_task_data())

        assert isinstance(snapshot, TaskSnapshot)
        assert snapshot.task_id == "task-deser"
        assert snapshot.event_name == "process_data"
        assert snapshot.task_type == "normal"

    def test_group_task(self) -> None:
        snapshot = StateDeserializer.deserialize_task(_make_task_data(task_type="group"))
        assert snapshot.task_type == "group"

    def test_with_checkpoint(self) -> None:
        snapshot = StateDeserializer.deserialize_task(
            _make_task_data(task_checkpoint={"offset": 50})
        )
        assert snapshot.task_checkpoint == {"offset": 50}


EVENTS = {"StepA": StepA, "StepB": StepB, "StepC": StepC, "StepS": StepS}


def build(code):
    """Build a task graph from pointy code with the event classes attached.

    Tasks parsed from text only carry event names; resolving them needs the
    global event registry, which other tests reset, so bind the classes here.
    """
    root, _ = build_pipeline_flow_from_pointy_code(code)

    def bind(task):
        if not task.is_grouping and isinstance(task.event, str):
            task.event = EVENTS[task.event]
        for child in task.get_children():
            bind(child)
        for chain in getattr(task, "chains", []):
            bind(chain)

    bind(root)
    return root


def through_json(task):
    """Serialize a task graph and rebuild it from the JSON text."""
    data = json.loads(json.dumps(StateSerializer.serialize_task(task), default=str))
    return StateDeserializer.rebuild_task(data)


class TestRebuildTask:

    def test_single_task(self) -> None:
        task = PipelineTask(event=StepA)
        task.namespace = "billing"
        task.descriptors = {"odd": 2}
        task.sequence_number = 4

        rebuilt = through_json(task)

        assert isinstance(rebuilt, PipelineTask)
        assert rebuilt.id == task.id
        assert rebuilt.event is StepA
        assert rebuilt.namespace == "billing"
        assert rebuilt.descriptors == {"odd": 2}
        assert rebuilt.sequence_number == 4
        assert rebuilt.options is None

    def test_options_survive_json(self) -> None:
        root = build(
            'StepA[stop_condition = "on_error", extra = 1] * 3'
        )

        rebuilt = through_json(root)

        assert rebuilt.options.retry_attempts == 3
        assert rebuilt.options.stop_condition == StopCondition.ON_ERROR
        assert rebuilt.options.extras == {"extra": 1}

    def test_sequence_with_pipes(self) -> None:
        root = build("StepA -> StepB |-> StepC")

        rebuilt = through_json(root)

        second = rebuilt.condition_node.on_success_event
        third = second.condition_node.on_success_event
        assert [t.event for t in (rebuilt, second, third)] == [StepA, StepB, StepC]
        assert rebuilt.condition_node.on_success_pipe == PipeType.POINTER
        assert second.condition_node.on_success_pipe == PipeType.PIPE_POINTER
        assert second.parent_node is rebuilt
        assert third.get_root() is rebuilt

    def test_branches_and_sink(self) -> None:
        root = build(
            "@descriptor: odd\n"
            "StepA (failure -> StepB, odd |-> StepC) -> StepS"
        )

        rebuilt = through_json(root)

        assert rebuilt.is_conditional
        assert rebuilt.get_descriptor(0).event is StepB
        odd = rebuilt.get_descriptor(2)
        assert odd.event is StepC
        assert odd.is_descriptor_task
        assert odd.descriptor_pipe == PipeType.PIPE_POINTER
        assert odd.parent_node is rebuilt
        assert rebuilt.sink_node.event is StepS
        assert rebuilt.sink_pipe == PipeType.POINTER
        assert rebuilt.sink_node.is_sink

    def test_group(self) -> None:
        root = build(
            "{ StepA -> StepB }[retry_attempts = 2] -> StepC"
        )

        rebuilt = through_json(root)

        assert isinstance(rebuilt, PipelineTaskGrouping)
        assert rebuilt.options.retry_attempts == 2
        assert [chain.event for chain in rebuilt.chains] == [StepA]
        assert rebuilt.chains[0].condition_node.on_success_event.event is StepB
        assert rebuilt.condition_node.on_success_event.event is StepC

    def test_rebuilt_graph_serializes_identically(self) -> None:
        root = build(
            "StepA[x = 1] * 2 (failure -> StepB, success -> StepC) -> {StepS}"
        )
        first = json.loads(json.dumps(StateSerializer.serialize_task(root), default=str))

        again = json.loads(
            json.dumps(
                StateSerializer.serialize_task(StateDeserializer.rebuild_task(first)),
                default=str,
            )
        )

        assert again == first
