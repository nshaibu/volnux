"""A task that asks the outside world for something is suspended, persisted and released."""

import asyncio

import pytest

from volnux.engine.base import EngineExecutionResult
from volnux.engine.default_engine import DefaultWorkflowEngine
from volnux.exceptions import ExternalCommunicationSuspensionRequest, SuspendTask
from volnux.execution.context import ExecutionContext
from volnux.execution.rehydrator.engine.snapshot import (
    EngineStateSnapshot,
    TaskSnapshot,
)
from volnux.execution.rehydrator.event.snapshot import (
    EventCheckpointSnapshot,
    EventPhase,
)
from volnux.execution.status import ExecutionStatus
from volnux.mixins.event.communication.datastructures import (
    ExternalCommunicationQueueEntry,
    RequestStatus,
)


@pytest.fixture
def ran():
    return []


@pytest.fixture
def three_step(make_event, make_pipeline, ran):
    """A -> B -> C where B asks a human, in communicate()."""

    async def a(self):
        ran.append("A")
        return True, "a"

    async def communicate(self, *args, **kwargs):
        await self.request_human_input(
            "Approve order",
            "Order 7 needs sign-off",
            {"order": 7},
            options=["approve", "reject"],
            timeout_hours=2,
        )

    async def b(self, *args, **kwargs):
        ran.append("B")
        return True, "b"

    async def c(self):
        ran.append("C")
        return True, "c"

    A = make_event(a, "StepA")
    B = make_event(b, "StepB", communicate=communicate)
    C = make_event(c, "StepC")
    pipeline_class = make_pipeline(
        "%s -> %s -> %s" % (A.__name__, B.__name__, C.__name__)
    )
    return pipeline_class, A, B, C


async def run_until_suspended(pipeline_class):
    pipeline = pipeline_class()
    engine = DefaultWorkflowEngine()
    result = await engine.execute(pipeline.get_pipeline_state().start, pipeline)
    return pipeline, engine, result


class TestSuspendedRun:
    @pytest.mark.asyncio
    async def test_the_run_ends_suspended_and_later_tasks_do_not_run(
        self, memory_store, three_step, ran
    ):
        pipeline_class, *_ = three_step
        _, _, result = await run_until_suspended(pipeline_class)
        assert result.status == EngineExecutionResult.SUSPENDED
        assert result.final_context.status == ExecutionStatus.SUSPENDED
        assert ran == ["A"]  # B stopped in communicate(); C never started

    @pytest.mark.asyncio
    async def test_a_suspended_context_is_never_marked_completed(
        self, memory_store, three_step, monkeypatch
    ):
        statuses = []
        original_bulk = ExecutionContext.bulk_update_async
        original_update = ExecutionContext.update_status_async

        async def bulk(self, status=None, errors=None, results=None):
            statuses.append((self.id, status))
            return await original_bulk(self, status, errors, results)

        async def update(self, status):
            statuses.append((self.id, status))
            return await original_update(self, status)

        monkeypatch.setattr(ExecutionContext, "bulk_update_async", bulk)
        monkeypatch.setattr(ExecutionContext, "update_status_async", update)

        pipeline_class, *_ = three_step
        _, _, result = await run_until_suspended(pipeline_class)
        suspended = [s for cid, s in statuses if cid == result.final_context.id]
        assert ExecutionStatus.COMPLETED not in suspended
        assert suspended[-1] == ExecutionStatus.SUSPENDED
        assert ExecutionStatus.PAUSED not in suspended

    @pytest.mark.asyncio
    async def test_a_queue_entry_records_the_wait(self, memory_store, three_step):
        pipeline_class, A, B, C = three_step
        pipeline, _, result = await run_until_suspended(pipeline_class)

        entries = await (await ExternalCommunicationQueueEntry.all()).to_list()
        assert len(entries) == 1
        entry = entries[0]
        assert entry.status == RequestStatus.WAITING.value
        assert entry.request_type == "hitl"
        assert entry.workflow_name == pipeline_class.__name__
        assert entry.workflow_id == pipeline.id
        assert entry.request_title == "Approve order"
        assert entry.request_payload == {"order": 7}
        assert entry.options == ["approve", "reject"]
        assert entry.context_id == result.final_context.state_id
        assert entry.id == entry.request_id
        assert entry.is_expired() is False and entry.timeout_at is not None

    @pytest.mark.asyncio
    async def test_the_event_is_snapshotted_at_the_suspension_point(
        self, memory_store, three_step
    ):
        pipeline_class, A, B, C = three_step
        await run_until_suspended(pipeline_class)
        [entry] = await (await ExternalCommunicationQueueEntry.all()).to_list()

        snapshot = await EventCheckpointSnapshot.get(entry.checkpoint_key)
        assert snapshot.class_path.endswith(B.__name__)
        # Taken while communicating, not after the completion step the event runs
        # on its way out: resuming must re-enter communicate(), not skip it.
        assert snapshot.phase == EventPhase.PRE_PROCESS
        assert snapshot.task_id == entry.task_id

    @pytest.mark.asyncio
    async def test_the_context_and_engine_position_are_saved(
        self, memory_store, three_step
    ):
        pipeline_class, A, B, C = three_step
        _, _, result = await run_until_suspended(pipeline_class)
        [entry] = await (await ExternalCommunicationQueueEntry.all()).to_list()

        context = await ExecutionContext.get(entry.context_id)
        assert context.status == ExecutionStatus.SUSPENDED

        state = await EngineStateSnapshot.get(entry.context_id)
        assert state.context_id == entry.context_id
        assert state.current_task["task_id"] == entry.task_id
        assert state.tasks_processed == result.tasks_processed
        # the suspended task's graph is saved too, so the run can continue after it
        task = await TaskSnapshot.get(entry.task_id)
        assert task.event_class_import_path.endswith(B.__name__)

    @pytest.mark.asyncio
    async def test_suspending_the_first_task_has_no_previous_context(
        self, memory_store, make_event, make_pipeline
    ):
        async def communicate(self, *args, **kwargs):
            await self.request_human_input("t", "d", {})

        async def process(self, *a, **k):
            return True, "x"

        event = make_event(process, "First", communicate=communicate)
        pipeline = make_pipeline(event.__name__)()
        engine = DefaultWorkflowEngine()
        result = await engine.execute(pipeline.get_pipeline_state().start, pipeline)
        assert result.status == EngineExecutionResult.SUSPENDED
        [entry] = await (await ExternalCommunicationQueueEntry.all()).to_list()
        state = await EngineStateSnapshot.get(entry.context_id)
        assert state.current_task["previous_context_id"] is None

    @pytest.mark.asyncio
    async def test_request_types_are_recorded(
        self, memory_store, make_event, make_pipeline
    ):
        async def communicate(self, *args, **kwargs):
            await self.wait_for_event("payment.confirmed", "Wait", "for payment")

        async def process(self, *a, **k):
            return True, "x"

        event = make_event(process, "Waiter", communicate=communicate)
        pipeline = make_pipeline(event.__name__)()
        await DefaultWorkflowEngine().execute(
            pipeline.get_pipeline_state().start, pipeline
        )
        [entry] = await (await ExternalCommunicationQueueEntry.all()).to_list()
        assert entry.request_type == "event"
        assert entry.timeout_at is None  # waits forever by default


class TestSuspensionDetection:
    def test_a_subclass_of_suspend_task_is_found(self):
        context = ExecutionContext.__new__(ExecutionContext)
        context.errors = [ValueError("x")]
        assert context.get_suspension_request() is None

        class Suspension(SuspendTask):
            def __init__(self):
                Exception.__init__(self, "s")

        suspension = Suspension()
        context.errors = [ValueError("x"), suspension]
        assert context.get_suspension_request() is suspension

    def test_other_sentinels_are_still_matched_exactly(self):
        from volnux.exceptions import StopProcessingError

        class Special(StopProcessingError):
            pass

        context = ExecutionContext.__new__(ExecutionContext)
        context.errors = [Special(message="x", exception=None)]
        assert context.get_stop_processing_request() is None


class TestPipelineLevel:
    @pytest.mark.asyncio
    async def test_a_suspended_pipeline_reports_no_end(self, memory_store, three_step):
        from volnux.signal import signals

        ends = []

        def on_end(sender, signal, **kwargs):
            ends.append(1)

        signals.pipeline_execution_end.connect(None, on_end)
        try:
            pipeline_class, *_ = three_step
            pipeline = pipeline_class()
            await pipeline.start()
            latest = pipeline.execution_context.get_latest_context()
            assert latest.status == ExecutionStatus.SUSPENDED
            assert ends == []
        finally:
            signals.pipeline_execution_end.disconnect(None, on_end)

    @pytest.mark.asyncio
    async def test_the_suspension_signal_is_emitted(self, memory_store, three_step):
        from volnux.signal import signals

        seen = []

        def on_suspended(sender, signal, execution_context, **kwargs):
            seen.append(execution_context.status)

        signals.event_execution_suspended.connect(None, on_suspended)
        try:
            pipeline_class, *_ = three_step
            await run_until_suspended(pipeline_class)
            assert seen == [ExecutionStatus.SUSPENDED]
        finally:
            signals.event_execution_suspended.disconnect(None, on_suspended)


class TestOtherSuspensions:
    @pytest.mark.asyncio
    async def test_a_bare_suspension_registers_nothing(self, memory_store, caplog):
        """Only a real external request has what it takes to be registered and resumed."""
        from unittest.mock import MagicMock

        from volnux.execution.coordinator import ExecutionCoordinator

        coordinator = ExecutionCoordinator(MagicMock())
        bare = SuspendTask(MagicMock(), suspension_type=SuspendTask.SuspensionType.HITL)
        await coordinator._process_suspension_request(bare)
        assert await (await ExternalCommunicationQueueEntry.all()).to_list() == []
        assert "cannot be resumed automatically" in caplog.text
