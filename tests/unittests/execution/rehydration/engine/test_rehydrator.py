"""Tests for rehydrator/engine/rehydrator — LazyRehydrator.

An ExecutionContext is persisted as itself, so rehydrating one is loading it
with ``ExecutionContext.get`` and re-attaching what cannot be data.

Covers:
  - __init__: instance-level registry isolation, cache type.
  - rehydrate_node: load, caching, pipeline re-attachment, missing context.
  - resolve_links / resolve_results: lazy references become live objects.
  - _reconstruct_pipeline: registry lookup, import fallback, kwargs, caching.
  - _rebuild_task: queued tasks restored from TaskSnapshot records.
  - _load_engine_state / _rehydrate_engine / _rebuild_current_task_node.
  - resume_from: with and without an engine class.
"""

from __future__ import annotations

import json
import weakref
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from volnux import EventBase
from volnux.backends.fields.foreign_key import ForeignKey
from volnux.exceptions import ObjectDoesNotExist
from volnux.execution.context import ExecutionContext
from volnux.execution.pipeline import Pipeline
from volnux.execution.rehydrator.engine.deserializer import StateDeserializer
from volnux.execution.rehydrator.engine.rehydrator import LazyRehydrator
from volnux.result import EventResult
from volnux.execution.rehydrator.engine.snapshot import (
    EngineStateSnapshot,
    TaskSnapshot,
)
from volnux.task import PipelineTask

MODULE = "volnux.execution.rehydrator.engine.rehydrator"


class RehydratedEvent(EventBase):
    def process(self, *args, **kwargs):
        return True, None


class RehydratedPipeline(Pipeline):
    class Meta:
        pointy = "A"


def make_context(**kwargs: Any) -> ExecutionContext:
    return ExecutionContext(
        task_profiles=PipelineTask(event=RehydratedEvent),
        pipeline=kwargs.pop("pipeline", RehydratedPipeline()),
        **kwargs,
    )


def stored(context: ExecutionContext) -> ExecutionContext:
    """What the store hands back: the context rebuilt from its JSON state."""
    state = json.loads(json.dumps(context.__getstate__(), default=str))
    record_id = state.pop("id")
    loaded = ExecutionContext.hydrate_formax_model(state)
    loaded.change_object_id(record_id)
    return loaded


def engine_state(**overrides: Any) -> EngineStateSnapshot:
    data = {
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
    data.update(overrides)
    return EngineStateSnapshot(**data)


@pytest.fixture()
def rehydrator() -> LazyRehydrator:
    return LazyRehydrator(deserializer=StateDeserializer)


class TestInit:

    def test_default_init(self) -> None:
        rehydrator = LazyRehydrator()
        assert rehydrator.deserializer is StateDeserializer
        assert rehydrator.pipeline_registry == {}

    def test_custom_deserializer(self) -> None:
        custom = MagicMock()
        assert LazyRehydrator(deserializer=custom).deserializer is custom

    def test_instance_registry_isolation(self) -> None:
        first, second = LazyRehydrator(), LazyRehydrator()
        first.pipeline_registry["p"] = object()
        assert "p" not in second.pipeline_registry

    def test_registry_with_initial_data_is_copied(self) -> None:
        initial = {"p1": object()}
        rehydrator = LazyRehydrator(pipeline_registry=initial)
        assert rehydrator.pipeline_registry == initial
        assert rehydrator.pipeline_registry is not initial

    def test_cache_is_weak_value_dict(self) -> None:
        assert isinstance(LazyRehydrator()._cache, weakref.WeakValueDictionary)


class TestRehydrateNode:

    @pytest.mark.asyncio
    async def test_loads_the_context_itself(self, rehydrator: LazyRehydrator) -> None:
        original = make_context(workflow_id="wf")

        with patch.object(
            ExecutionContext, "get", AsyncMock(return_value=stored(original))
        ) as get:
            context = await rehydrator.rehydrate_node(original.id)

        get.assert_awaited_once_with(original.id)
        assert isinstance(context, ExecutionContext)
        assert context.id == original.id
        assert context.workflow_id == "wf"
        assert context.get_task_profiles()[0].event is RehydratedEvent

    @pytest.mark.asyncio
    async def test_pipeline_is_reattached_from_its_reference(
        self, rehydrator: LazyRehydrator
    ) -> None:
        original = make_context()

        with patch.object(
            ExecutionContext, "get", AsyncMock(return_value=stored(original))
        ):
            context = await rehydrator.rehydrate_node(original.id)

        assert isinstance(context.pipeline, RehydratedPipeline)
        assert rehydrator.pipeline_registry[original.pipeline.id] is context.pipeline

    @pytest.mark.asyncio
    async def test_registered_pipeline_is_reused(
        self, rehydrator: LazyRehydrator
    ) -> None:
        live = RehydratedPipeline()
        rehydrator.pipeline_registry[live.id] = live
        original = make_context(pipeline=live)

        with patch.object(
            ExecutionContext, "get", AsyncMock(return_value=stored(original))
        ):
            context = await rehydrator.rehydrate_node(original.id)

        assert context.pipeline is live

    @pytest.mark.asyncio
    async def test_caching_returns_same_instance(
        self, rehydrator: LazyRehydrator
    ) -> None:
        original = make_context()
        loaded = stored(original)

        with patch.object(ExecutionContext, "get", AsyncMock(return_value=loaded)) as get:
            first = await rehydrator.rehydrate_node(original.id)
            second = await rehydrator.rehydrate_node(original.id)

        assert first is second
        get.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_missing_context_raises_runtime_error(
        self, rehydrator: LazyRehydrator
    ) -> None:
        with patch.object(
            ExecutionContext, "get", AsyncMock(side_effect=ObjectDoesNotExist("x"))
        ):
            with pytest.raises(RuntimeError, match="no context found for state_id='x'"):
                await rehydrator.rehydrate_node("x")


class TestResolveLinks:

    @pytest.fixture()
    def family(self):
        parent, prev, nxt, child = (make_context() for _ in range(4))
        context = make_context(
            parent_context=parent,
            previous_context=prev,
            next_context=nxt,
            child_contexts=[child],
        )
        by_id = {c.id: stored(c) for c in (parent, prev, nxt, child)}
        return context, by_id

    @pytest.mark.asyncio
    async def test_references_become_live_contexts(
        self, rehydrator: LazyRehydrator, family
    ) -> None:
        context, by_id = family
        loaded = stored(context)
        assert isinstance(loaded.parent_context, ForeignKey)

        with patch.object(
            ExecutionContext, "get", AsyncMock(side_effect=lambda i: by_id[i])
        ):
            await rehydrator.resolve_links(loaded)

        for name in ("parent_context", "previous_context", "next_context"):
            link = getattr(loaded, name)
            assert isinstance(link, ExecutionContext)
            assert link is by_id[link.id]
        assert [c.id for c in loaded.child_contexts] == [
            c.id for c in by_id.values() if c.id in {r.id for r in loaded.child_contexts}
        ]
        assert all(isinstance(c, ExecutionContext) for c in loaded.child_contexts)

    @pytest.mark.asyncio
    async def test_each_neighbour_is_loaded_once(
        self, rehydrator: LazyRehydrator, family
    ) -> None:
        context, by_id = family
        loaded = stored(context)
        get = AsyncMock(side_effect=lambda i: by_id[i])

        with patch.object(ExecutionContext, "get", get):
            await rehydrator.resolve_links(loaded)
            await rehydrator.resolve_links(loaded)

        assert get.await_count == 4

    @pytest.mark.asyncio
    async def test_live_links_are_left_alone(self, rehydrator: LazyRehydrator) -> None:
        parent = make_context()
        context = make_context(parent_context=parent)

        with patch.object(ExecutionContext, "get", AsyncMock()) as get:
            await rehydrator.resolve_links(context)

        get.assert_not_awaited()
        assert context.parent_context is parent

    @pytest.mark.asyncio
    async def test_no_links(self, rehydrator: LazyRehydrator) -> None:
        context = stored(make_context())
        with patch.object(ExecutionContext, "get", AsyncMock()) as get:
            await rehydrator.resolve_links(context)
        get.assert_not_awaited()
        assert context.parent_context is None
        assert context.child_contexts == []


class _Result(EventResult):
    def should_persist(self) -> bool:
        return False


def make_result(result_id: str) -> EventResult:
    result = _Result(
        error=False, event_name="event", content=None, workflow_id=None, task_id=None
    )
    result.change_object_id(result_id)
    return result


class TestResolveResults:

    @pytest.mark.asyncio
    async def test_references_are_loaded(self, rehydrator: LazyRehydrator) -> None:
        first, live, aggregate = make_result("r1"), make_result("r2"), make_result("agg")
        context = stored(make_context())
        context.results = [ForeignKey("r1", "m.R", resolved_instance=first), live]
        context.aggregated_result = ForeignKey("agg", "m.R", resolved_instance=aggregate)

        await rehydrator.resolve_results(context)

        assert list(context.results) == [first, live]
        assert context.aggregated_result is aggregate


class TestReconstructPipeline:

    def test_registry_hit(self, rehydrator: LazyRehydrator) -> None:
        pipeline = object()
        rehydrator.pipeline_registry["p1"] = pipeline
        assert rehydrator._reconstruct_pipeline("p1", "x.Y") is pipeline

    def test_registry_miss_imports_and_caches(self, rehydrator: LazyRehydrator) -> None:
        klass = MagicMock()
        with patch(f"{MODULE}.import_class", return_value=klass) as import_class:
            pipeline = rehydrator._reconstruct_pipeline("p2", "x.Y")

        import_class.assert_called_once_with("x.Y")
        assert pipeline is klass.return_value
        assert rehydrator.pipeline_registry["p2"] is pipeline

    def test_with_pipeline_kwargs(self, rehydrator: LazyRehydrator) -> None:
        klass = MagicMock()
        with patch(f"{MODULE}.import_class", return_value=klass):
            rehydrator._reconstruct_pipeline("p3", "x.Y", {"a": 1})
        klass.assert_called_once_with(a=1)

    def test_empty_kwargs_default(self, rehydrator: LazyRehydrator) -> None:
        klass = MagicMock()
        with patch(f"{MODULE}.import_class", return_value=klass):
            rehydrator._reconstruct_pipeline("p4", "x.Y")
        klass.assert_called_once_with()


class TestRebuildTask:
    ENTRY = {"task_id": "t-1", "previous_context_id": "c1", "position_in_queue": 0}

    def snapshot(self, **overrides: Any) -> TaskSnapshot:
        data = {
            "context_id": "ctx",
            "task_id": "t-1",
            "task_type": "normal",
            "task_checkpoint": None,
            "event_name": "RehydratedEvent",
            "event_class_import_path": f"{RehydratedEvent.__module__}.RehydratedEvent",
            "sink_task_id": None,
            "sink_task_pipe": None,
            "options": {},
            "condition_node": {},
            "sequence_number": 0,
            "descriptor": None,
            "descriptor_pipe_type": None,
            "namespace": "billing",
            "descriptors": {"odd": 2},
        }
        data.update(overrides)
        return TaskSnapshot(**data)

    @pytest.mark.asyncio
    async def test_valid_task(self, rehydrator: LazyRehydrator) -> None:
        with patch.object(TaskSnapshot, "get", AsyncMock(return_value=self.snapshot())):
            task = await rehydrator._rebuild_task(self.ENTRY)

        assert isinstance(task, PipelineTask)
        assert task.event is RehydratedEvent
        assert task.namespace == "billing"
        assert task.descriptors == {"odd": 2}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("entry", [{}, {"task_id": ""}])
    async def test_missing_task_id_returns_none(
        self, rehydrator: LazyRehydrator, entry: dict
    ) -> None:
        assert await rehydrator._rebuild_task(entry) is None

    @pytest.mark.asyncio
    async def test_load_failure_returns_none_and_warns(
        self, rehydrator: LazyRehydrator, caplog: pytest.LogCaptureFixture
    ) -> None:
        with patch.object(
            TaskSnapshot, "get", AsyncMock(side_effect=ObjectDoesNotExist("t-1"))
        ), caplog.at_level("WARNING"):
            assert await rehydrator._rebuild_task(self.ENTRY) is None

        assert any("t-1" in record.message for record in caplog.records)

    @pytest.mark.asyncio
    async def test_raw_dict_falls_back_to_deserializer(
        self, rehydrator: LazyRehydrator
    ) -> None:
        raw = {"task_id": "t-1"}
        with patch.object(
            TaskSnapshot, "get", AsyncMock(return_value=raw)
        ), patch.object(
            rehydrator.deserializer, "deserialize_task", return_value=self.snapshot()
        ) as deserialize:
            task = await rehydrator._rebuild_task(self.ENTRY)

        deserialize.assert_called_once_with(raw)
        assert task.event is RehydratedEvent


class TestLoadEngineState:

    @pytest.mark.asyncio
    async def test_returns_snapshot_directly(self, rehydrator: LazyRehydrator) -> None:
        snapshot = engine_state()
        with patch.object(EngineStateSnapshot, "get", AsyncMock(return_value=snapshot)):
            assert await rehydrator._load_engine_state("ctx-1") is snapshot

    @pytest.mark.asyncio
    async def test_raw_dict_delegates_to_deserializer(
        self, rehydrator: LazyRehydrator
    ) -> None:
        raw = {"context_id": "ctx-1"}
        rebuilt = engine_state()
        with patch.object(
            EngineStateSnapshot, "get", AsyncMock(return_value=raw)
        ), patch.object(
            rehydrator.deserializer, "deserialize_engine_state", return_value=rebuilt
        ) as deserialize:
            assert await rehydrator._load_engine_state("ctx-1") is rebuilt

        deserialize.assert_called_once_with(raw)

    @pytest.mark.asyncio
    async def test_missing_raises_runtime_error(self, rehydrator: LazyRehydrator) -> None:
        with patch.object(
            EngineStateSnapshot, "get", AsyncMock(side_effect=ObjectDoesNotExist("x"))
        ):
            with pytest.raises(RuntimeError, match="no engine state"):
                await rehydrator._load_engine_state("x")


class TestRehydrateEngine:

    @pytest.mark.asyncio
    async def test_engine_is_constructed_and_positioned(
        self, rehydrator: LazyRehydrator
    ) -> None:
        engine_class = MagicMock()
        node = MagicMock()
        context = make_context()

        with patch.object(
            rehydrator, "_rebuild_current_task_node", AsyncMock(return_value=node)
        ):
            engine = await rehydrator._rehydrate_engine(
                engine_class, engine_state(tasks_processed=5), context
            )

        engine_class.assert_called_once_with(enable_checkpointing=True)
        assert engine is engine_class.return_value
        assert engine.tasks_processed == 5
        assert engine.current_task_node is node


class TestRebuildCurrentTaskNode:
    CURRENT = {"task_id": "t-1", "previous_context_id": "c1", "position_in_queue": 0}

    @pytest.mark.asyncio
    async def test_no_current_task_returns_none(self, rehydrator: LazyRehydrator) -> None:
        assert (
            await rehydrator._rebuild_current_task_node(engine_state(), make_context())
            is None
        )

    @pytest.mark.asyncio
    async def test_valid_task_creates_task_node(self, rehydrator: LazyRehydrator) -> None:
        task = PipelineTask(event=RehydratedEvent)
        context = make_context()

        with patch.object(rehydrator, "_rebuild_task", AsyncMock(return_value=task)):
            node = await rehydrator._rebuild_current_task_node(
                engine_state(current_task=self.CURRENT), context
            )

        assert node.task is task
        assert node.previous_context is context

    @pytest.mark.asyncio
    async def test_rebuild_failure_returns_none(self, rehydrator: LazyRehydrator) -> None:
        with patch.object(rehydrator, "_rebuild_task", AsyncMock(return_value=None)):
            node = await rehydrator._rebuild_current_task_node(
                engine_state(current_task=self.CURRENT), make_context()
            )
        assert node is None


class TestResumeFrom:

    @pytest.mark.asyncio
    async def test_without_engine_class(self, rehydrator: LazyRehydrator) -> None:
        context = make_context()
        with patch.object(
            rehydrator, "rehydrate_node", AsyncMock(return_value=context)
        ) as rehydrate:
            result_context, engine = await rehydrator.resume_from("ctx-1")

        rehydrate.assert_awaited_once_with("ctx-1")
        assert result_context is context
        assert engine is None

    @pytest.mark.asyncio
    async def test_with_engine_class(self, rehydrator: LazyRehydrator) -> None:
        context = MagicMock()
        snapshot, engine = engine_state(), MagicMock()
        engine_class = MagicMock()

        with patch.object(
            rehydrator, "rehydrate_node", AsyncMock(return_value=context)
        ), patch.object(
            rehydrator, "_load_engine_state", AsyncMock(return_value=snapshot)
        ) as load, patch.object(
            rehydrator, "_rehydrate_engine", AsyncMock(return_value=engine)
        ) as rehydrate_engine:
            result_context, result_engine = await rehydrator.resume_from(
                "ctx-1", engine_class
            )

        load.assert_awaited_once_with("ctx-1")
        rehydrate_engine.assert_awaited_once_with(engine_class, snapshot, context)
        context.set_engine.assert_called_once_with(engine)
        assert result_context is context
        assert result_engine is engine
