import asyncio
import json
import weakref
from collections import deque
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from volnux import EventBase
from volnux.exceptions import StopProcessingError, SuspendTask, SwitchTask
from volnux.execution import context as context_module
from volnux.execution.context import (
    TERMINAL_STATUSES,
    ExecutionContext,
    ExecutionMetrics,
    preformat_task_profile,
)
from volnux.execution.codecs import PipelineReference
from volnux.execution.pipeline import Pipeline
from volnux.execution.status import ExecutionStatus
from volnux.parser.operator import PipeType
from volnux.parser.options import ResultEvaluationStrategy
from volnux.result import EventResult
from volnux.result_evaluators import EventEvaluator, ResultEvaluationStrategies
from volnux.task import PipelineTask, PipelineTaskGrouping


def make_task(event_name="EventA"):
    task = MagicMock(spec=PipelineTask)
    task.get_event_name.return_value = event_name
    # Instance attributes set in PipelineTask.__init__, invisible to spec=
    task.options = None
    task.condition_node = MagicMock(name="condition_node")
    return task


@pytest.fixture
def task():
    return make_task()


@pytest.fixture
def pipeline():
    return MagicMock(spec=Pipeline)


def make_context(task=None, pipeline=None, **kwargs):
    return ExecutionContext(
        task_profiles=task if task is not None else make_task(),
        pipeline=pipeline if pipeline is not None else MagicMock(spec=Pipeline),
        **kwargs,
    )


def link(*contexts):
    """Link contexts into a doubly-linked chain, in order."""
    for prev, nxt in zip(contexts, contexts[1:]):
        prev.next_context = nxt
        nxt.previous_context = prev
    return contexts


class _Result(EventResult):
    def should_persist(self):
        return False


def FakeResult(id):
    """A real (non-persisting) EventResult with the given id."""
    result = _Result(
        error=False, event_name="event", content=None, workflow_id=None, task_id=None
    )
    result.change_object_id(id)
    return result


class TestExecutionMetrics:
    def test_duration_while_running(self):
        assert ExecutionMetrics(start_time=10.0).duration == 0.0

    def test_duration_when_finished(self):
        assert ExecutionMetrics(start_time=10.0, end_time=12.5).duration == 2.5

    def test_start_time_defaults_to_now(self):
        assert ExecutionMetrics().start_time > 0


class TestPreformatTaskProfile:
    def test_single_task(self, task):
        assert preformat_task_profile(task) == deque([task])

    def test_task_grouping(self):
        group = MagicMock(spec=PipelineTaskGrouping)
        assert preformat_task_profile(group) == deque([group])

    @pytest.mark.parametrize("container", [list, tuple])
    def test_list_and_tuple(self, task, container):
        result = preformat_task_profile(container([task, task]))
        assert isinstance(result, deque)
        assert list(result) == [task, task]

    def test_deque_returned_as_is(self, task):
        profiles = deque([task])
        assert preformat_task_profile(profiles) is profiles

    def test_invalid(self):
        with pytest.raises(Exception, match="task_profiles must be"):
            preformat_task_profile("not-a-task")


class TestConstruction:
    def test_direct_construction_defaults(self, task, pipeline):
        context = ExecutionContext(task_profiles=task, pipeline=pipeline)
        assert context.task_profiles == deque([task])
        assert context.pipeline is pipeline
        assert isinstance(context.metrics, ExecutionMetrics)
        assert context.previous_context is None
        assert context.next_context is None
        assert context.parent_context is None
        assert context.child_contexts == []
        assert context.workflow_id is None
        assert context.workflow_name is None
        assert context.status == ExecutionStatus.PENDING
        assert context.errors == []
        assert len(context.results) == 0
        assert context.aggregated_result is None
        assert context.persist_state is False
        assert context.get_engine() is None
        assert context.get_task_checkpoint() is None

    def test_mutable_defaults_not_shared(self):
        a, b = make_context(), make_context()
        a.errors.append(ValueError())
        a.child_contexts.append(b)
        assert b.errors == []
        assert b.child_contexts == []
        assert a.results is not b.results
        assert a.metrics is not b.metrics

    def test_rejects_invalid_task_profiles(self, pipeline):
        # formax wraps pre-formatter failures; the cause carries the reason
        with pytest.raises(RuntimeError) as exc_info:
            ExecutionContext(task_profiles=42, pipeline=pipeline)
        assert "task_profiles must be" in str(exc_info.value.__cause__)

    @pytest.mark.asyncio
    async def test_create_context(self, task, pipeline):
        context = await ExecutionContext.create_context(
            workflow_id="wf-1",
            workflow_name="flow",
            task_profiles=[task],
            pipeline=pipeline,
            persist_state=True,
        )
        assert context.workflow_id == "wf-1"
        assert context.workflow_name == "flow"
        assert context.task_profiles == deque([task])
        assert context.metrics.start_time > 0
        assert context.persist_state is True

    @pytest.mark.asyncio
    async def test_create_context_single_task(self, task, pipeline):
        context = await ExecutionContext.create_context("wf", "n", task, pipeline)
        assert context.task_profiles == deque([task])
        assert not context.is_multitask()

    @pytest.mark.asyncio
    async def test_create_context_with_links(self, pipeline):
        parent, prev, child = make_context(), make_context(), make_context()
        metrics = ExecutionMetrics(start_time=1.0)
        context = await ExecutionContext.create_context(
            "wf",
            "n",
            [make_task()],
            pipeline,
            metrics=metrics,
            previous_context=prev,
            parent_context=parent,
            child_contexts=(c for c in [child]),
        )
        assert context.metrics is metrics
        assert context.previous_context is prev
        assert context.parent_context is parent
        assert context.child_contexts == [child]


class TestIdentity:
    def test_equality_is_identity_based(self, task, pipeline):
        a = make_context(task, pipeline)
        b = make_context(task, pipeline)
        assert a == a
        assert a != b

    def test_equal_to_object_with_same_id(self):
        a, b = make_context(), make_context()
        b._id = a.id
        assert a == b
        assert hash(a) == hash(b)

    def test_not_equal_to_other_types(self):
        context = make_context()
        assert context != context.id
        assert context != object()

    def test_usable_in_sets_and_dicts(self):
        a, b = make_context(), make_context()
        assert len({a, b, a}) == 2
        assert {a: 1}[a] == 1

    def test_equality_does_not_recurse_through_graph(self):
        parent, child = make_context(), make_context()
        parent.child_contexts.append(child)
        child.parent_context = parent
        assert parent != child  # would recurse with field-wise equality

    def test_state_id(self):
        context = make_context()
        assert context.state_id == context.id


class TestLinkedList:
    def test_iteration_follows_next_links(self):
        a, b, c = link(make_context(), make_context(), make_context())
        assert list(a) == [a, b, c]
        assert list(b) == [b, c]

    def test_single_context_iterates_itself(self):
        context = make_context()
        assert list(context) == [context]

    def test_head_and_tail(self):
        a, b, c = link(make_context(), make_context(), make_context())
        for context in (a, b, c):
            assert context.get_head_context() is a
            assert context.get_tail_context() is c
            assert context.get_latest_context() is c

    def test_cycles_terminate(self):
        a, b = link(make_context(), make_context())
        b.next_context = a
        a.previous_context = b
        assert list(a) == [a, b]
        assert a.get_head_context() is b
        assert a.get_tail_context() is b

    def test_filter_by_event_case_insensitive(self):
        a = make_context(make_task("Fetch"))
        b = make_context([make_task("Parse"), make_task("FETCH")])
        c = make_context(make_task("Store"))
        link(a, b, c)
        assert c.filter_by_event("fetch") == [a, b]
        assert a.filter_by_event("store") == [c]
        assert a.filter_by_event("missing") == []

    def test_filter_by_event_ignores_tasks_without_names(self):
        nameless = MagicMock(spec=[])
        context = make_context([nameless, make_task("A")])
        assert context.filter_by_event("a") == [context]


class TestTree:
    @pytest.mark.asyncio
    async def test_spawn_child_links_both_ways(self, pipeline):
        parent = await ExecutionContext.create_context(
            "wf", "flow", [make_task()], pipeline, persist_state=True
        )
        child = await parent.spawn_child(make_task("Child"))

        assert child.parent_context is parent
        assert parent.child_contexts == [child]
        assert child.pipeline is pipeline
        assert child.workflow_id == "wf"
        assert child.workflow_name == "flow"
        assert child.persist_state is True
        assert child.task_profiles[0].get_event_name() == "Child"

    @pytest.mark.asyncio
    async def test_concurrent_spawns_all_registered(self, pipeline):
        parent = await ExecutionContext.create_context(
            "wf", "n", [make_task()], pipeline
        )
        children = await asyncio.gather(
            *(parent.spawn_child([make_task()]) for _ in range(10))
        )
        assert set(parent.child_contexts) == set(children)
        assert len(parent.child_contexts) == 10

    @pytest.mark.asyncio
    async def test_spawn_child_preserves_subclass(self, pipeline):
        class CustomContext(ExecutionContext):
            pass

        parent = CustomContext(task_profiles=make_task(), pipeline=pipeline)
        assert isinstance(await parent.spawn_child(make_task()), CustomContext)

    @pytest.mark.asyncio
    async def test_root_leaf_depth(self, pipeline):
        root = make_context(pipeline=pipeline)
        child = await root.spawn_child(make_task())
        grandchild = await child.spawn_child(make_task())

        assert root.is_root and not child.is_root
        assert grandchild.is_leaf and not root.is_leaf
        assert [root.get_depth(), child.get_depth(), grandchild.get_depth()] == [
            0,
            1,
            2,
        ]
        assert grandchild.get_root_context() is root
        assert root.get_root_context() is root

    def test_parent_cycle_terminates(self):
        a, b = make_context(), make_context()
        a.parent_context, b.parent_context = b, a
        assert a.get_depth() == 1
        assert a.get_root_context() is b


class TestGroupFinality:
    @pytest.mark.asyncio
    async def test_parent_completes_when_all_children_done(self, pipeline):
        parent = make_context(pipeline=pipeline)
        first = await parent.spawn_child(make_task())
        second = await parent.spawn_child(make_task())

        await first.update_status_async(ExecutionStatus.COMPLETED)
        assert parent.status == ExecutionStatus.PENDING

        await second.update_status_async(ExecutionStatus.COMPLETED)
        assert parent.status == ExecutionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_failed_last_child_still_finalizes_group(self, pipeline):
        parent = make_context(pipeline=pipeline)
        first = await parent.spawn_child(make_task())
        second = await parent.spawn_child(make_task())

        await first.update_status_async(ExecutionStatus.COMPLETED)
        await second.failed_async()
        assert parent.status == ExecutionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_running_child_blocks_group(self, pipeline):
        parent = make_context(pipeline=pipeline)
        done = await parent.spawn_child(make_task())
        running = await parent.spawn_child(make_task())
        running.status = ExecutionStatus.RUNNING

        await done.update_status_async(ExecutionStatus.COMPLETED)
        assert parent.status == ExecutionStatus.PENDING

    @pytest.mark.asyncio
    async def test_non_terminal_status_does_not_notify(self, pipeline):
        parent = make_context(pipeline=pipeline)
        child = await parent.spawn_child(make_task())
        with patch.object(
            ExecutionContext, "_evaluate_group_finality", new=AsyncMock()
        ) as evaluate:
            await child.update_status_async(ExecutionStatus.RUNNING)
            await child.paused_async()
        evaluate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_terminal_parent_is_not_overwritten(self, pipeline):
        parent = make_context(pipeline=pipeline)
        child = await parent.spawn_child(make_task())
        parent.status = ExecutionStatus.FAILED

        await child.update_status_async(ExecutionStatus.COMPLETED)
        assert parent.status == ExecutionStatus.FAILED

    @pytest.mark.asyncio
    async def test_completion_propagates_up_the_tree(self, pipeline):
        root = make_context(pipeline=pipeline)
        middle = await root.spawn_child(make_task())
        leaf = await middle.spawn_child(make_task())

        await leaf.update_status_async(ExecutionStatus.COMPLETED)
        assert middle.status == ExecutionStatus.COMPLETED
        assert root.status == ExecutionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_bulk_update_async_notifies_parent(self, pipeline):
        parent = make_context(pipeline=pipeline)
        child = await parent.spawn_child(make_task())
        await child.bulk_update_async(ExecutionStatus.COMPLETED, [], [])
        assert parent.status == ExecutionStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_bulk_update_async_without_status_does_not_notify(self, pipeline):
        parent = make_context(pipeline=pipeline)
        child = await parent.spawn_child(make_task())
        child.status = ExecutionStatus.COMPLETED
        await child.bulk_update_async(errors=[ValueError()])
        assert parent.status == ExecutionStatus.PENDING

    def test_sync_update_does_not_evaluate_parent(self):
        parent, child = make_context(), make_context()
        child.parent_context = parent
        parent.child_contexts.append(child)
        child.update_status(ExecutionStatus.COMPLETED)
        assert parent.status == ExecutionStatus.PENDING

    def test_terminal_statuses(self):
        assert TERMINAL_STATUSES == {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.ABORTED,
        }


class TestMutators:
    def test_add_error(self):
        context = make_context()
        error = ValueError("x")
        context.add_error(error)
        assert context.errors == [error]

    @pytest.mark.asyncio
    async def test_add_error_async(self):
        context = make_context()
        await context.add_error_async(KeyError("k"))
        assert len(context.errors) == 1

    def test_add_result(self):
        context = make_context()
        result = FakeResult("r1")
        context.add_result(result)
        assert list(context.results) == [result]

    @pytest.mark.asyncio
    async def test_add_result_async(self):
        context = make_context()
        await context.add_result_async(FakeResult("r1"))
        assert len(context.results) == 1

    def test_bulk_update(self):
        context = make_context()
        context.add_error(ValueError("first"))
        context.bulk_update(
            status=ExecutionStatus.RUNNING,
            errors=[KeyError("second")],
            results=[FakeResult("a"), FakeResult("b")],
        )
        assert context.status == ExecutionStatus.RUNNING
        assert len(context.errors) == 2
        assert len(context.results) == 2

    def test_bulk_update_nothing(self):
        context = make_context()
        context.bulk_update()
        assert context.status == ExecutionStatus.PENDING
        assert context.errors == []

    @pytest.mark.asyncio
    async def test_bulk_update_async_positional(self):
        context = make_context()
        await context.bulk_update_async(
            ExecutionStatus.COMPLETED, [ValueError()], [FakeResult("a")]
        )
        assert context.status == ExecutionStatus.COMPLETED
        assert len(context.errors) == 1
        assert len(context.results) == 1

    @pytest.mark.asyncio
    async def test_update_aggregated_result(self):
        context = make_context()
        result = FakeResult("agg")
        await context.update_aggregated_result(result)
        assert context.aggregated_result is result

    def test_task_checkpoint(self):
        context = make_context()
        context.set_task_checkpoint({"step": 3})
        assert context.get_task_checkpoint() == {"step": 3}


class TestPersistence:
    @pytest.fixture
    def save(self):
        with patch.object(ExecutionContext, "save", new=AsyncMock()) as save:
            yield save

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "call",
        [
            lambda c: c.update_status_async(ExecutionStatus.RUNNING),
            lambda c: c.add_error_async(ValueError()),
            lambda c: c.add_result_async(FakeResult("r")),
            lambda c: c.bulk_update_async(status=ExecutionStatus.RUNNING),
            lambda c: c.update_aggregated_result(FakeResult("r")),
        ],
    )
    async def test_async_mutators_save_when_enabled(self, save, call):
        await call(make_context(persist_state=True))
        save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_async_mutators_skip_save_by_default(self, save):
        context = make_context()
        await context.update_status_async(ExecutionStatus.RUNNING)
        await context.add_error_async(ValueError())
        save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sync_mutator_schedules_save_in_running_loop(self, save):
        context = make_context(persist_state=True)
        context.update_status(ExecutionStatus.RUNNING)
        assert len(context._pending_saves) == 1
        await asyncio.gather(*context._pending_saves)
        save.assert_awaited_once()
        await asyncio.sleep(0)
        assert context._pending_saves == set()

    def test_sync_mutator_saves_without_running_loop(self, save):
        context = make_context(persist_state=True)
        context.add_error(ValueError())
        save.assert_awaited_once()

    def test_sync_mutators_skip_save_by_default(self, save):
        context = make_context()
        context.update_status(ExecutionStatus.RUNNING)
        context.bulk_update(status=ExecutionStatus.COMPLETED)
        save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_background_save_failure_is_logged(self, save, caplog):
        save.side_effect = RuntimeError("backend down")
        context = make_context(persist_state=True)
        context.update_status(ExecutionStatus.RUNNING)
        await asyncio.gather(*context._pending_saves, return_exceptions=True)
        await asyncio.sleep(0)
        assert "Background save of context" in caplog.text
        assert "backend down" in caplog.text

    def test_snapshot_api_removed(self):
        # Contexts are KeyValueStoreIntegrationMixin models: save() persists them
        assert not hasattr(ExecutionContext, "create_snapshot")
        assert not hasattr(ExecutionContext, "persist")


class StateEvent(EventBase):
    def process(self, *args, **kwargs):
        return True, None


class StatePipeline(Pipeline):
    class Meta:
        pointy = "A"


def roundtrip(context):
    """Persist and reload through the JSON path the key-value stores use."""
    state = json.loads(json.dumps(context.__getstate__(), default=str))
    record_id = state.pop("id")
    restored = ExecutionContext.hydrate_formax_model(state)
    restored.change_object_id(record_id)
    return restored


class TestState:
    """The context is persisted through its fields; there is no hand-written
    serialization on the model."""

    @pytest.fixture
    def populated(self):
        parent, prev, nxt, child = (make_context() for _ in range(4))
        context = make_context(
            task=PipelineTask(event=StateEvent),
            pipeline=StatePipeline(),
            workflow_id="wf",
            workflow_name="flow",
            previous_context=prev,
            next_context=nxt,
            parent_context=parent,
            child_contexts=[child],
            metrics=ExecutionMetrics(start_time=1.0, end_time=2.0),
            persist_state=True,
        )
        context.status = ExecutionStatus.RUNNING
        context.errors.append(ValueError("bad"))
        context.results.append(FakeResult("r1"))
        context.aggregated_result = FakeResult("agg")
        context.set_task_checkpoint({"step": 1})
        return context, parent, prev, nxt, child

    def test_state_is_json_serializable(self, populated):
        json.dumps(populated[0].get_state())

    def test_state_contents(self, populated):
        context, parent, prev, nxt, child = populated
        state = context.get_state()

        assert context.__getstate__()["id"] == context.id
        assert state["status"] == "running"
        assert state["errors"][0]["type"] == "ValueError"
        assert state["errors"][0]["message"] == "bad"
        assert [r["object_id"] for r in state["results"]] == ["r1"]
        assert state["aggregated_result"] == {
            "type": "foreignkey",
            "object_id": "agg",
        }
        assert state["workflow_id"] == "wf"
        assert state["workflow_name"] == "flow"
        assert state["persist_state"] is True
        assert state["metrics"] == {"start_time": 1.0, "end_time": 2.0}
        assert state["task_checkpoint"] == {"step": 1}
        assert state["parent_context"] == {"type": "foreignkey", "object_id": parent.id}
        assert state["previous_context"]["object_id"] == prev.id
        assert state["next_context"]["object_id"] == nxt.id
        assert [c["object_id"] for c in state["child_contexts"]] == [child.id]
        assert state["pipeline"] == {
            "id": context.pipeline.id,
            "class_path": f"{StatePipeline.__module__}.StatePipeline",
            "fields": {},
        }
        assert state["task_profiles"][0]["event_name"] == "StateEvent"

    def test_state_excludes_runtime_attributes(self, populated):
        state = populated[0].get_state()
        # runtime-only (underscore) fields are never persisted
        assert not [key for key in state if key.startswith("_")]

    def test_state_is_idempotent(self, populated):
        context = populated[0]
        assert context.get_state() == context.get_state()

    def test_flattened_errors_survive_a_second_save(self, populated):
        context = populated[0]
        first = context.get_state()
        context.errors = first["errors"]  # already-flattened errors
        assert context.get_state()["errors"] == first["errors"]

    def test_round_trip(self, populated):
        context, parent, prev, nxt, child = populated
        restored = roundtrip(context)

        assert restored.id == context.id
        assert restored.status == ExecutionStatus.RUNNING
        assert restored.errors[0]["message"] == "bad"
        assert restored.workflow_id == "wf"
        assert restored.workflow_name == "flow"
        assert restored.persist_state is True
        assert restored.metrics == ExecutionMetrics(start_time=1.0, end_time=2.0)
        assert restored.get_task_checkpoint() == {"step": 1}

    def test_round_trip_links_are_lazy_references(self, populated):
        context, parent, prev, nxt, child = populated
        restored = roundtrip(context)

        assert not restored.parent_context.is_resolved
        assert restored.parent_context.id == parent.id
        assert restored.previous_context.id == prev.id
        assert restored.next_context.id == nxt.id
        assert [c.id for c in restored.child_contexts] == [child.id]
        assert [r.id for r in restored.results] == ["r1"]
        assert restored.aggregated_result.id == "agg"

    def test_round_trip_task_graph_and_pipeline_reference(self, populated):
        restored = roundtrip(populated[0])

        task = restored.get_task_profiles()[0]
        assert isinstance(task, PipelineTask)
        assert task.event is StateEvent
        assert task.id == populated[0].get_task_profiles()[0].id
        assert restored.pipeline == PipelineReference(
            id=populated[0].pipeline.id,
            class_path=f"{StatePipeline.__module__}.StatePipeline",
        )

    @pytest.mark.asyncio
    async def test_restored_context_is_usable(self, populated):
        context = populated[0]
        restored = roundtrip(context)

        assert restored.get_engine() is None
        restored.add_result(FakeResult("new"))
        await restored.add_error_async(ValueError())
        assert len(restored.results) == 2
        assert restored == context

    def test_state_defaults(self):
        restored = ExecutionContext.hydrate_formax_model({})
        assert restored.status == ExecutionStatus.PENDING
        assert restored.errors == []
        assert restored.results == []
        assert restored.child_contexts == []
        assert restored.parent_context is None
        assert restored.metrics.end_time == 0.0

    @pytest.mark.asyncio
    async def test_save_persists_state_and_result_records(self, populated):
        context = populated[0]
        saved = AsyncMock()
        record = MagicMock(spec=EventResult, id="r2", save=saved)
        context.results.append(record)

        with patch.object(
            context_module.KeyValueStoreIntegrationMixin, "save", AsyncMock()
        ) as persist:
            await context.save()

        saved.assert_awaited_once()
        # the class-level save also serves the other result records
        # (r1 and the aggregate) and, last, the context itself
        assert persist.await_count == 3
        assert persist.await_args_list[-1].args == ()


class TestControlFlowRequests:
    @pytest.mark.parametrize(
        "error_cls, getter",
        [
            (StopProcessingError, "get_stop_processing_request"),
            (SwitchTask, "get_switch_request"),
            (SuspendTask, "get_suspension_request"),
        ],
    )
    def test_found(self, error_cls, getter):
        context = make_context()
        error = error_cls.__new__(error_cls)
        context.errors.extend([ValueError("noise"), error])
        assert getattr(context, getter)() is error

    @pytest.mark.parametrize(
        "getter",
        [
            "get_stop_processing_request",
            "get_switch_request",
            "get_suspension_request",
        ],
    )
    def test_not_found(self, getter):
        context = make_context()
        context.errors.append(ValueError("noise"))
        assert getattr(context, getter)() is None

    def test_exact_type_match_only(self):
        class CustomSwitch(SwitchTask):
            pass

        context = make_context()
        context.errors.append(CustomSwitch.__new__(CustomSwitch))
        assert context.get_switch_request() is None

    def test_serialized_errors_ignored(self):
        context = make_context()
        context.errors.append({"type": "SwitchTask", "message": "x"})
        assert context.get_switch_request() is None

    def test_returns_first_match(self):
        context = make_context()
        first, second = (SwitchTask.__new__(SwitchTask) for _ in range(2))
        context.errors.extend([first, second])
        assert context.get_switch_request() is first


SYNC_TRANSITIONS = [
    ("cancel", ExecutionStatus.CANCELLED, "event_execution_cancelled"),
    ("abort", ExecutionStatus.ABORTED, "event_execution_aborted"),
    ("failed", ExecutionStatus.FAILED, "event_execution_failed"),
]
ASYNC_TRANSITIONS = [
    ("cancel_async", ExecutionStatus.CANCELLED, "event_execution_cancelled"),
    ("abort_async", ExecutionStatus.ABORTED, "event_execution_aborted"),
    ("failed_async", ExecutionStatus.FAILED, "event_execution_failed"),
    ("paused_async", ExecutionStatus.PAUSED, "event_execution_paused"),
]


class TestTransitions:
    @pytest.mark.parametrize("method, status, signal_name", SYNC_TRANSITIONS)
    def test_sync(self, method, status, signal_name):
        task = make_task()
        context = make_context(task)
        with patch.object(getattr(context_module, signal_name), "emit") as emit:
            getattr(context, method)()

        assert context.status == status
        emit.assert_called_once()
        kwargs = emit.call_args.kwargs
        assert kwargs["sender"] is ExecutionContext
        assert kwargs["execution_context"] is context
        assert kwargs["state"] == status
        assert list(kwargs["task_profiles"]) == [task]
        assert kwargs["task_profiles"] is not context.task_profiles

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method, status, signal_name", ASYNC_TRANSITIONS)
    async def test_async(self, method, status, signal_name):
        context = make_context()
        with patch.object(
            getattr(context_module, signal_name), "emit_async", new=AsyncMock()
        ) as emit:
            await getattr(context, method)()

        assert context.status == status
        emit.assert_awaited_once()
        assert emit.call_args.kwargs["state"] == status
        assert emit.call_args.kwargs["execution_context"] is context

    @pytest.mark.asyncio
    async def test_status_set_before_signal(self):
        context = make_context()
        seen = []

        async def listener(**kwargs):
            seen.append(kwargs["execution_context"].status)

        with patch.object(
            context_module.event_execution_failed, "emit_async", new=listener
        ):
            await context.failed_async()
        assert seen == [ExecutionStatus.FAILED]


class TestTaskProfiles:
    def test_is_multitask(self):
        assert not make_context(make_task()).is_multitask()
        assert make_context([make_task(), make_task()]).is_multitask()

    def test_get_task_profiles(self, task):
        context = make_context([task])
        assert context.get_task_profiles() is context.task_profiles

    def test_decision_profile_single_task(self, task):
        assert make_context(task).get_decision_task_profile() is task

    def test_decision_profile_end_of_parallel_chain(self):
        first, last = make_task(), make_task()
        first.get_pointer_to_task.return_value = PipeType.PARALLELISM
        first.condition_node.on_success_pipe = PipeType.PARALLELISM
        last.get_pointer_to_task.return_value = PipeType.PARALLELISM
        last.condition_node.on_success_pipe = PipeType.POINTER
        assert make_context([first, last]).get_decision_task_profile() is last

    def test_decision_profile_none_found(self):
        tasks = [make_task(), make_task()]
        for t in tasks:
            t.get_pointer_to_task.return_value = PipeType.POINTER
        assert make_context(tasks).get_decision_task_profile() is None


class TestResultEvaluator:
    def test_none_without_decision_profile(self):
        tasks = [make_task(), make_task()]
        for t in tasks:
            t.get_pointer_to_task.return_value = PipeType.POINTER
        assert make_context(tasks).get_result_evaluator() is None

    def test_event_class_evaluator_by_default(self, task):
        evaluator = object()
        task.get_event_class.return_value.evaluator.return_value = evaluator
        assert make_context(task).get_result_evaluator() is evaluator

    def test_strategy_from_options(self, task):
        task.options = MagicMock()
        task.options.is_configured.return_value = True
        task.options.result_evaluation_strategy = (
            ResultEvaluationStrategy.ANY_MUST_SUCCEED
        )
        evaluator = make_context(task).get_result_evaluator()
        assert isinstance(evaluator, EventEvaluator)
        assert evaluator.strategy is ResultEvaluationStrategies.ANY_MUST_SUCCEED
        task.options.is_configured.assert_called_once_with("result_evaluation_strategy")
        task.get_event_class.assert_not_called()

    def test_unresolvable_strategy_falls_back(self, task, caplog):
        task.options = MagicMock()
        task.options.is_configured.return_value = True
        task.options.result_evaluation_strategy = MagicMock()
        task.options.result_evaluation_strategy.name = "NOT_A_STRATEGY"
        fallback = object()
        task.get_event_class.return_value.evaluator.return_value = fallback
        assert make_context(task).get_result_evaluator() is fallback
        assert "Error resolving result evaluation strategy" in caplog.text

    def test_unconfigured_options_fall_back(self, task):
        task.options = MagicMock()
        task.options.is_configured.return_value = False
        fallback = object()
        task.get_event_class.return_value.evaluator.return_value = fallback
        assert make_context(task).get_result_evaluator() is fallback


class TestEngineAssociation:
    def test_set_and_get_engine(self):
        class Engine:
            pass

        engine = Engine()
        context = make_context()
        context.set_engine(engine)
        assert context.get_engine() is engine
        assert isinstance(context._engine_ref, weakref.ReferenceType)

    def test_engine_reference_is_weak(self):
        class Engine:
            pass

        engine = Engine()
        context = make_context()
        context.set_engine(engine)
        del engine
        assert context.get_engine() is None


class TestDispatch:
    @pytest.mark.asyncio
    async def test_dispatch_uses_coordinator(self):
        context = make_context()
        with patch("volnux.execution.coordinator.ExecutionCoordinator") as coordinator:
            coordinator.return_value.execute_async = AsyncMock(
                return_value=("ok", None)
            )
            assert await context.dispatch(timeout=5) == ("ok", None)
        coordinator.assert_called_once_with(execution_context=context, timeout=5)

    @pytest.mark.asyncio
    async def test_dispatch_logs_and_reraises(self, caplog):
        context = make_context()
        with patch("volnux.execution.coordinator.ExecutionCoordinator") as coordinator:
            coordinator.return_value.execute_async = AsyncMock(
                side_effect=RuntimeError("boom")
            )
            with pytest.raises(RuntimeError, match="boom"):
                await context.dispatch()
        assert "boom" in caplog.text
