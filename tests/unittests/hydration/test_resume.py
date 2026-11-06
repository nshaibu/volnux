"""A suspended workflow is resumed, with the answer, and carries on to the end."""

import pytest

from volnux.engine.base import EngineExecutionResult
from volnux.engine.default_engine import DefaultWorkflowEngine
from volnux.engine.resume import build_response_result, resume_suspended_workflow
from volnux.execution.context import ExecutionContext
from volnux.execution.status import ExecutionStatus
from volnux.mixins.event.communication.datastructures import (
    ExternalCommunicationQueueEntry,
    ExternalCommunicationResponse,
)

from .helpers import answer, suspend


class TestResume:
    @pytest.mark.asyncio
    async def test_the_workflow_runs_to_the_end_after_the_answer(
        self, serialized_store, workflow, seen
    ):
        _, entry = await suspend(workflow)
        assert seen["ran"] == ["A"]

        result = await resume_suspended_workflow(entry, answer(entry))

        assert result.status == EngineExecutionResult.COMPLETED
        assert seen["ran"] == ["A", "B", "C"]  # A is not run again
        assert result.final_context.status == ExecutionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_the_task_sees_the_answer_in_previous_result(
        self, serialized_store, workflow, seen
    ):
        _, entry = await suspend(workflow)
        await resume_suspended_workflow(entry, answer(entry, "reject"))
        assert seen["decision"] == "reject"
        # first run found nothing and asked; the resumed run found the answer
        assert seen["answers"][0] is None
        assert seen["answers"][1].content["decision"] == "reject"

    @pytest.mark.asyncio
    async def test_communicate_runs_again_but_the_task_is_not_asked_twice(
        self, serialized_store, workflow, seen
    ):
        _, entry = await suspend(workflow)
        await resume_suspended_workflow(entry, answer(entry))
        assert len(seen["answers"]) == 2
        assert len(await (await ExternalCommunicationQueueEntry.all()).to_list()) == 1

    @pytest.mark.asyncio
    async def test_the_answer_reaches_the_task_as_a_named_result(
        self, serialized_store, workflow, seen
    ):
        _, entry = await suspend(workflow)
        await resume_suspended_workflow(entry, answer(entry, "approve", source="human"))
        result = seen["answers"][1]
        assert result.event_name == "human_response"
        assert result.content["type"] == "human_response"
        assert result.content["request_id"] == entry.request_id
        assert result.content["reviewer"] == "ann"
        assert result.task_id == entry.task_id

    @pytest.mark.asyncio
    async def test_a_timeout_is_delivered_like_any_answer(
        self, serialized_store, workflow, seen
    ):
        _, entry = await suspend(workflow)
        await resume_suspended_workflow(
            entry, ExternalCommunicationResponse.timeout(entry.request_id)
        )
        assert seen["decision"] == "timeout"
        assert seen["answers"][1].content["source"] == "timeout"

    @pytest.mark.asyncio
    async def test_the_resumed_run_extends_the_original_chain(
        self, serialized_store, workflow, seen
    ):
        pipeline, entry = await suspend(workflow)
        result = await resume_suspended_workflow(entry, answer(entry))
        names = [
            ctx.get_task_profiles()[0].get_event_class().__name__
            for ctx in result.final_context.get_head_context()
            if ctx.get_task_profiles()
        ]
        assert names[-1].startswith("StepC")
        assert any(n.startswith("StepA") for n in names)

    @pytest.mark.asyncio
    async def test_no_answer_means_the_workflow_stays_suspended(
        self, serialized_store, workflow, seen
    ):
        pipeline, entry = await suspend(workflow)
        context = await ExecutionContext.get(entry.context_id)
        assert context.status == ExecutionStatus.SUSPENDED
        assert seen["ran"] == ["A"]


class TestResumeAgain:
    @pytest.mark.asyncio
    async def test_a_resumed_task_can_suspend_again(
        self, serialized_store, make_event, make_pipeline, seen
    ):
        ran = seen.setdefault("ran", [])

        async def communicate(self, *a, **k):
            responses = [
                r
                for r in (
                    []
                    if self.previous_result is None
                    or isinstance(self.previous_result, type)
                    else self.previous_result
                )
                if r.event_name == "human_response"
            ]
            if len(responses) < 2:
                await self.request_human_input("again", str(len(responses)), {})

        async def process(self, *a, **k):
            ran.append("done")
            return True, "done"

        event = make_event(process, "Twice", communicate=communicate)
        pipeline_class = make_pipeline(event.__name__)
        pipeline = pipeline_class()
        engine = DefaultWorkflowEngine()
        await engine.execute(pipeline.get_pipeline_state().start, pipeline)

        [first] = await (await ExternalCommunicationQueueEntry.all()).to_list()
        result = await resume_suspended_workflow(first, answer(first))
        # one answer is not enough: it asks a second time
        assert result.status == EngineExecutionResult.SUSPENDED
        assert ran == []


class TestBuildResponseResult:
    @pytest.mark.parametrize(
        "request_type, name",
        [
            ("hitl", "human_response"),
            ("event", "external_event"),
            ("condition", "condition_met"),
            ("other", "external_response"),
        ],
    )
    def test_named_by_kind_of_request(self, request_type, name):
        entry = ExternalCommunicationQueueEntry.create(
            request_id="r",
            workflow_name="W",
            workflow_id="w",
            task_id="t",
            request_type=request_type,
            checkpoint_key="ck",
            context_id="cx",
            request_title="x",
        )
        result = build_response_result(entry, answer(entry))
        assert result.event_name == name and result.content["type"] == name
        assert result.task_id == "t" and result.success
