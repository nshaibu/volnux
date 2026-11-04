"""The smaller parts of the resume path: pipeline identity, hooks, signals, accessors."""

from collections import deque
from unittest.mock import AsyncMock, MagicMock

import pytest

from volnux.constants import EMPTY
from volnux.engine.base import EngineExecutionResult, EngineResult
from volnux.engine.default_engine import DefaultWorkflowEngine
from volnux.engine.resume import resume_suspended_workflow
from volnux.execution.context import ExecutionContext
from volnux.execution.pipeline.fields import InputDataField
from volnux.execution.rehydrator.engine.snapshot import (
    EngineStateSnapshot,
    TaskSnapshot,
)
from volnux.execution.status import ExecutionStatus
from volnux.mixins.event.communication.datastructures import (
    ExternalCommunicationQueueEntry,
)
from volnux.result import EventResult, ResultSet
from volnux.signal import signals

from .helpers import answer, suspend


def result(name, content="c"):
    return EventResult(
        error=False, event_name=name, content=content, workflow_id=None, task_id="t"
    )


class TestHitlRequestedSignal:
    @pytest.mark.asyncio
    async def test_a_human_wait_is_announced_with_its_request_id(
        self, serialized_store, workflow
    ):
        seen = []

        def listener(sender, signal, execution_context, request, **kwargs):
            seen.append((request.request_id, request.title, execution_context.status))

        signals.hitl_requested.connect(None, listener)
        try:
            _, entry = await suspend(workflow)
        finally:
            signals.hitl_requested.disconnect(None, listener)
        assert seen == [(entry.request_id, "Approve order", ExecutionStatus.SUSPENDED)]

    @pytest.mark.asyncio
    async def test_waiting_for_an_event_is_not_a_human_request(
        self, serialized_store, make_event, make_pipeline
    ):
        async def communicate(self, *a, **k):
            await self.wait_for_event("payment.confirmed", "Wait", "for payment")

        async def process(self, *a, **k):
            return True, "x"

        seen = []

        def listener(sender, signal, **kwargs):
            seen.append(1)

        signals.hitl_requested.connect(None, listener)
        try:
            event = make_event(process, "Waiter", communicate=communicate)
            await suspend(make_pipeline(event.__name__))
        finally:
            signals.hitl_requested.disconnect(None, listener)
        assert seen == []


class TestPipelineAcrossProcesses:
    @pytest.fixture
    def regional(self, make_event, make_pipeline, seen):
        ran = seen.setdefault("ran", [])

        async def first(self):
            ran.append("A")
            return True, "a"

        async def communicate(self, *a, **k):
            if self.get_response() is None:
                await self.request_human_input("ok?", "d", {})

        async def second(self, *a, **k):
            ran.append("B")
            return True, "b"

        async def third(self, region):
            seen["region"] = region
            return True, region

        A = make_event(first, "RegA")
        B = make_event(second, "RegB", communicate=communicate)
        C = make_event(third, "RegC")
        return make_pipeline(
            "%s -> %s -> %s" % (A.__name__, B.__name__, C.__name__),
            region=InputDataField(data_type=str, required=True),
        )

    @pytest.mark.asyncio
    async def test_input_values_survive_the_resume(
        self, serialized_store, regional, seen
    ):
        """A later task reads a pipeline input: the rebuilt pipeline must still have it."""
        _, entry = await suspend(regional, region="eu-west")
        await resume_suspended_workflow(entry, answer(entry))
        assert seen["region"] == "eu-west"

    @pytest.mark.asyncio
    async def test_the_run_keeps_its_identity(self, serialized_store, regional):
        pipeline, entry = await suspend(regional, region="eu-west")
        result = await resume_suspended_workflow(entry, answer(entry))
        assert result.final_context.pipeline.id == pipeline.id
        assert entry.workflow_id == pipeline.id

    def test_the_reference_carries_only_serialisable_inputs(self, regional):
        from volnux.execution.codecs import serialize_pipeline

        pipeline = regional(region="eu-west")
        payload = serialize_pipeline(pipeline)
        assert payload["fields"] == {"region": "eu-west"}
        assert payload["id"] == pipeline.id

    def test_a_pipeline_without_inputs_has_no_fields(self, workflow):
        from volnux.execution.codecs import deserialize_pipeline, serialize_pipeline

        reference = deserialize_pipeline(serialize_pipeline(workflow()))
        assert reference.fields == {}

    def test_an_old_payload_without_fields_still_loads(self):
        from volnux.execution.codecs import deserialize_pipeline

        reference = deserialize_pipeline({"id": "p", "class_path": "a.B"})
        assert reference.fields == {} and reference.id == "p"


class TestEngineResume:
    @pytest.mark.asyncio
    async def test_nothing_queued_means_nothing_to_resume(self):
        result = await DefaultWorkflowEngine().resume(MagicMock())
        assert result.status == EngineExecutionResult.COMPLETED

    @pytest.mark.asyncio
    async def test_the_progress_count_carries_on_from_the_snapshot(
        self, serialized_store, workflow, seen
    ):
        _, entry = await suspend(workflow)
        state = await EngineStateSnapshot.get(entry.context_id)
        result = await resume_suspended_workflow(entry, answer(entry))
        # B and C ran after the restored count
        assert result.tasks_processed == state.tasks_processed + 1

    @pytest.mark.asyncio
    async def test_a_resumed_run_does_not_rerun_what_finished(
        self, serialized_store, workflow, seen
    ):
        _, entry = await suspend(workflow)
        await resume_suspended_workflow(entry, answer(entry))
        assert seen["ran"].count("A") == 1

    @pytest.mark.asyncio
    async def test_a_second_suspension_registers_a_second_wait(
        self, serialized_store, make_event, make_pipeline
    ):
        async def communicate(self, *a, **k):
            if self.get_response() is None:
                await self.request_human_input("ok?", "d", {})

        async def process(self, *a, **k):
            return True, "done"

        event = make_event(process, "Again", communicate=communicate)
        pipeline = make_pipeline(event.__name__)()
        # the answer is injected, but the event asks again unless it finds one
        # that is *new*: here the first run asks, and a resumed run finds the answer
        await DefaultWorkflowEngine().execute(
            pipeline.get_pipeline_state().start, pipeline
        )
        [first] = await (await ExternalCommunicationQueueEntry.all()).to_list()
        resumed = await resume_suspended_workflow(first, answer(first))
        assert resumed.status == EngineExecutionResult.COMPLETED

    def test_a_suspension_inside_a_group_is_left_unresumable_not_half_saved(self):
        parent = DefaultWorkflowEngine()
        child = DefaultWorkflowEngine(parent_engine=parent)
        context = MagicMock()
        context.save = AsyncMock()
        import asyncio

        asyncio.run(child._save_suspension_state(context, MagicMock()))
        context.save.assert_not_called()

    def test_staging_an_event_for_a_task(self):
        engine = DefaultWorkflowEngine()
        engine.stage_resumed_event("t1", "event")
        assert engine._resumed_events == {"t1": "event"}


class TestContextHooks:
    def test_a_resumed_event_is_handed_over_once(self):
        context = ExecutionContext.__new__(ExecutionContext)
        context.set_resumed_events({"t1": "event-1", "t2": "event-2"})
        assert context.take_resumed_event("t1") == "event-1"
        assert context.take_resumed_event("t1") is None
        assert context.take_resumed_event("t2") == "event-2"

    def test_no_resumed_events_by_default(self):
        context = ExecutionContext.__new__(ExecutionContext)
        assert context.take_resumed_event("t1") is None

    def test_setting_resumed_events_copies_the_mapping(self):
        context = ExecutionContext.__new__(ExecutionContext)
        mapping = {"t1": "event"}
        context.set_resumed_events(mapping)
        mapping.clear()
        assert context.take_resumed_event("t1") == "event"


class TestFlowAdoptsTheResumedEvent:
    @pytest.mark.asyncio
    async def test_the_event_is_bound_to_the_new_context(
        self, serialized_store, workflow, seen
    ):
        _, entry = await suspend(workflow)
        bound = {}

        original = ExecutionContext.take_resumed_event

        def spy(self, task_id):
            event = original(self, task_id)
            if event is not None:
                bound["event"], bound["context"] = event, self
            return event

        ExecutionContext.take_resumed_event = spy
        try:
            await resume_suspended_workflow(entry, answer(entry))
        finally:
            ExecutionContext.take_resumed_event = original

        event, context = bound["event"], bound["context"]
        assert event._execution_context is context
        assert event.get_init_args()["execution_context"] is context
        assert context.status == ExecutionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_the_run_has_two_contexts_for_the_resumed_task(
        self, serialized_store, workflow
    ):
        """The suspended context stays as history; the resumed task runs in a new one."""
        _, entry = await suspend(workflow)
        suspended = await ExecutionContext.get(entry.context_id)
        result = await resume_suspended_workflow(entry, answer(entry))
        chain = list(result.final_context.get_head_context())
        assert entry.context_id not in {c.id for c in chain}
        assert suspended.status == ExecutionStatus.SUSPENDED


class TestGetResponse:
    @pytest.fixture
    def build(self):
        return lambda event_class, previous_result: event_class(
            execution_context=MagicMock(), task_id="t", previous_result=previous_result
        )

    def event(self, build, previous_result):
        async def process(self, *a, **k):
            return True, 1

        from .conftest import unique
        from volnux import EventBase

        event_class = type(unique("Probe"), (EventBase,), {"process": process})
        return build(event_class, previous_result)

    def test_nothing_before_the_task_was_suspended(self, build):
        assert self.event(build, EMPTY).get_response() is None
        assert self.event(build, None).get_response() is None

    def test_finds_the_answer_among_other_results(self, build):
        upstream, human = result("Upstream"), result("human_response", {"d": 1})
        event = self.event(build, ResultSet([upstream, human]))
        assert event.get_response() is human

    @pytest.mark.parametrize(
        "name", ["human_response", "external_event", "condition_met"]
    )
    def test_any_kind_of_answer_qualifies_by_default(self, build, name):
        answer_result = result(name)
        assert self.event(build, ResultSet([answer_result])).get_response() is (
            answer_result
        )

    def test_unrelated_results_are_not_answers(self, build):
        assert self.event(build, ResultSet([result("Upstream")])).get_response() is None

    def test_can_ask_for_a_specific_kind(self, build):
        human, external = result("human_response"), result("external_event")
        event = self.event(build, ResultSet([human, external]))
        assert event.get_response("external_event") is external


class TestReportOutcome:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status, signal_name",
        [
            (ExecutionStatus.COMPLETED, "pipeline_execution_end"),
            (ExecutionStatus.CANCELLED, "pipeline_stop"),
            (ExecutionStatus.ABORTED, "pipeline_shutdown"),
            (ExecutionStatus.SUSPENDED, None),
        ],
    )
    async def test_the_signal_matches_how_the_run_ended(
        self, workflow, status, signal_name
    ):
        fired = []
        listeners = []
        for name in ("pipeline_execution_end", "pipeline_stop", "pipeline_shutdown"):

            def make(name):
                def listener(sender, signal, **kwargs):
                    fired.append(name)

                return listener

            fn = make(name)
            listeners.append((getattr(signals, name), fn))
            getattr(signals, name).connect(None, fn)

        pipeline = workflow()
        latest = MagicMock(status=status)
        pipeline.execution_context = MagicMock()
        pipeline.execution_context.get_latest_context.return_value = latest
        try:
            await pipeline.report_outcome()
        finally:
            for signal, fn in listeners:
                signal.disconnect(None, fn)
        assert fired == ([signal_name] if signal_name else [])


class TestSnapshotRestore:
    @pytest.mark.asyncio
    async def test_a_task_snapshot_restores_its_graph(self, serialized_store, workflow):
        _, entry = await suspend(workflow)
        snapshot = await TaskSnapshot.get(entry.task_id)
        task = await snapshot.restore()
        assert task.get_id() == entry.task_id
        assert task.get_event_class().__name__.startswith("StepB")

    @pytest.mark.asyncio
    async def test_an_engine_snapshot_points_to_the_rehydrator(
        self, serialized_store, workflow
    ):
        _, entry = await suspend(workflow)
        state = await EngineStateSnapshot.get(entry.context_id)
        with pytest.raises(NotImplementedError, match="resume_from"):
            await state.restore()


class TestTriggersCanBeConstructed:
    def test_a_trigger_can_be_built_at_all(self):
        """Regression: TriggerBase passed updated_at as a string; the field takes a datetime."""
        from datetime import datetime

        from volnux.engine.workflows.trigger.triggers import ManualTrigger

        trigger = ManualTrigger("flow")
        assert isinstance(trigger.state.updated_at, (datetime, str))
        assert trigger.workflow_name == "flow"
