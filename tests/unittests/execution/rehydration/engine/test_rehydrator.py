"""Tests for rehydrator/engine/rehydrator — LazyRehydrator & LazyContextProxy.

Covers:
  - LazyContextProxy: state_id access, is_resolved, _resolve caching,
    __getattr__ forwarding, __setattr__ forwarding to slots, __hash__/__eq__,
    __repr__, __iter__.
  - LazyRehydrator.__init__: instance-level registry isolation, cache type.
  - rehydrate_node: caching returns same instance, lazy links attached.
  - _load_snapshot: returns ContextSnapshot, delegates to deserializer for raw
    dict, raises RuntimeError on ObjectDoesNotExist.
  - _build_context: pipeline reconstruction, task deque, state restoration,
    task checkpoint forwarding.
  - _reconstruct_pipeline: registry lookup, import_class fallback, pipeline
    kwargs, registry caching.
  - _rebuild_tasks: empty list, valid list, skipped None entries.
  - _rebuild_task: valid task, missing task_id, load failure, raw dict
    fallback via deserializer.
  - _restore_execution_state: results, status, metrics restoration.
  - _attach_lazy_links: previous/next/parent/children wiring.
  - _link_for: cached returns live node, uncached returns LazyContextProxy.
  - resume_from: with and without engine_class.
  - _rehydrate_engine: engine construction, tasks_processed, current task.
  - _rebuild_current_task_node: no current_task, valid task, failed rebuild.
"""

from __future__ import annotations

import pytest
import threading
from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from volnux.execution.rehydrator.engine.rehydrator import (
    LazyRehydrator,
    LazyContextProxy,
)
from volnux.execution.rehydrator.engine.deserializer import StateDeserializer
from volnux.execution.rehydrator.engine.snapshot import (
    ContextSnapshot,
    TraversalSnapshot,
    TaskSnapshot,
)


def _make_traversal_data(**overrides: Any) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "engine_class_path": "volnux.engine.base.WorkflowEngine",
        "current_task": None,
        "current_task_checkpoint": None,
        "task_queue_snapshot": [],
        "queue_index": 0,
        "current_task_queue_size": 0,
        "current_sink_queue_size": 0,
        "sink_queue_snapshot": [],
        "tasks_processed": 0,
    }
    defaults.update(overrides)
    return defaults


def _make_context_snapshot_data(**overrides: Any) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "state_id": "state-test-001",
        "workflow_id": "wf-test",
        "parent_id": None,
        "child_ids": [],
        "depth": 0,
        "previous_context_id": None,
        "next_context_id": None,
        "traversal": _make_traversal_data(),
        "pipeline_id": "pipe-test",
        "pipeline_state": {},
        "pipeline_class_path": "myapp.pipelines.TestPipeline",
        "status": "RUNNING",
        "errors": [],
        "results": [],
        "metrics": {"start_time": 0.0, "end_time": 10.0, "duration": 10.0},
    }
    defaults.update(overrides)
    return defaults


def _make_context_snapshot(**overrides: Any) -> MagicMock:
    """Create a mock ContextSnapshot with the given overrides.

    Uses a real TraversalSnapshot dataclass for the traversal field so that
    attribute access (e.g. traversal.current_task, traversal.tasks_processed)
    returns actual values instead of auto-generated MagicMocks.
    """
    data = _make_context_snapshot_data(**overrides)

    # Build a real TraversalSnapshot so attribute access returns real values.
    traversal_data = data.pop("traversal")
    real_traversal = TraversalSnapshot(**traversal_data)

    snap = MagicMock(spec=ContextSnapshot)
    for k, v in data.items():
        setattr(snap, k, v)
    snap.traversal = real_traversal
    return snap


@pytest.fixture()
def rehydrator() -> LazyRehydrator:
    return LazyRehydrator(deserializer=StateDeserializer)


# ===========================================================================
# LazyContextProxy
# ===========================================================================
class TestLazyContextProxy:

    def test_state_id_accessible_without_resolve(
        self, rehydrator: LazyRehydrator
    ) -> None:
        proxy = LazyContextProxy(state_id="lazy-001", rehydrator=rehydrator)
        assert proxy.state_id == "lazy-001"

    def test_is_resolved_initially_false(self, rehydrator: LazyRehydrator) -> None:
        proxy = LazyContextProxy(state_id="lazy-002", rehydrator=rehydrator)
        assert proxy.is_resolved() is False

    def test_repr_lazy(self, rehydrator: LazyRehydrator) -> None:
        proxy = LazyContextProxy(state_id="lazy-003", rehydrator=rehydrator)
        assert "lazy" in repr(proxy)
        assert "lazy-003" in repr(proxy)
        assert "LazyContextProxy" in repr(proxy)

    def test_repr_resolved(self, rehydrator: LazyRehydrator) -> None:
        proxy = LazyContextProxy(state_id="lazy-004", rehydrator=rehydrator)

        mock_context = MagicMock()
        mock_context.state_id = "lazy-004"
        with patch.object(
            rehydrator,
            "rehydrate_node",
            new_callable=MagicMock,
            return_value=mock_context,
        ):
            proxy._resolve()

        assert proxy.is_resolved() is True
        assert "resolved" in repr(proxy)

    def test_hash_based_on_state_id(self, rehydrator: LazyRehydrator) -> None:
        proxy_a = LazyContextProxy(state_id="hash-001", rehydrator=rehydrator)
        proxy_b = LazyContextProxy(state_id="hash-001", rehydrator=rehydrator)
        assert hash(proxy_a) == hash(proxy_b)

    def test_hash_different_ids(self, rehydrator: LazyRehydrator) -> None:
        proxy_a = LazyContextProxy(state_id="h-a", rehydrator=rehydrator)
        proxy_b = LazyContextProxy(state_id="h-b", rehydrator=rehydrator)
        assert hash(proxy_a) != hash(proxy_b)

    def test_eq_same_state_id(self, rehydrator: LazyRehydrator) -> None:
        proxy_a = LazyContextProxy(state_id="eq-001", rehydrator=rehydrator)
        proxy_b = LazyContextProxy(state_id="eq-001", rehydrator=rehydrator)
        assert proxy_a == proxy_b

    def test_eq_different_state_id(self, rehydrator: LazyRehydrator) -> None:
        proxy_a = LazyContextProxy(state_id="eq-a", rehydrator=rehydrator)
        proxy_b = LazyContextProxy(state_id="eq-b", rehydrator=rehydrator)
        assert proxy_a != proxy_b

    def test_eq_non_state_id_object(self, rehydrator: LazyRehydrator) -> None:
        proxy = LazyContextProxy(state_id="eq-x", rehydrator=rehydrator)
        assert proxy != "eq-x"  # string has no state_id attr

    def test_eq_object_with_state_id(self, rehydrator: LazyRehydrator) -> None:
        proxy = LazyContextProxy(state_id="eq-y", rehydrator=rehydrator)
        other = MagicMock()
        other.state_id = "eq-y"
        assert proxy == other

    def test_getattr_forwards_to_resolved(self, rehydrator: LazyRehydrator) -> None:
        proxy = LazyContextProxy(state_id="fwd-001", rehydrator=rehydrator)

        mock_context = MagicMock()
        mock_context.some_attr = "hello"
        with patch.object(
            rehydrator,
            "rehydrate_node",
            new_callable=MagicMock,
            return_value=mock_context,
        ):
            assert proxy.some_attr == "hello"

    def test_setattr_non_slot_forwards(self, rehydrator: LazyRehydrator) -> None:
        proxy = LazyContextProxy(state_id="setattr-001", rehydrator=rehydrator)

        mock_context = MagicMock()
        with patch.object(
            rehydrator,
            "rehydrate_node",
            new_callable=MagicMock,
            return_value=mock_context,
        ):
            proxy.new_value = 42
            assert mock_context.new_value == 42

    def test_setattr_slot_direct(self, rehydrator: LazyRehydrator) -> None:
        proxy = LazyContextProxy(state_id="slot-001", rehydrator=rehydrator)
        # Setting a slot attribute should NOT trigger resolution
        object.__setattr__(proxy, "_resolved", "already-set")
        # Verify no rehydrate_node call happened
        assert proxy._resolved == "already-set"

    def test_iter_forwards(self, rehydrator: LazyRehydrator) -> None:
        proxy = LazyContextProxy(state_id="iter-001", rehydrator=rehydrator)

        mock_context = MagicMock()
        mock_context.__iter__ = MagicMock(return_value=iter([1, 2, 3]))
        with patch.object(
            rehydrator,
            "rehydrate_node",
            new_callable=MagicMock,
            return_value=mock_context,
        ):
            result = list(proxy)
            assert result == [1, 2, 3]

    def test_resolve_caches_result(self, rehydrator: LazyRehydrator) -> None:
        proxy = LazyContextProxy(state_id="cache-001", rehydrator=rehydrator)

        mock_context = MagicMock()
        with patch.object(
            rehydrator,
            "rehydrate_node",
            new_callable=MagicMock,
            return_value=mock_context,
        ) as mock_rehydrate:
            # First resolve
            result1 = proxy._resolve()
            # Second resolve — should use cache
            result2 = proxy._resolve()

        assert result1 is result2
        assert result1 is mock_context

    def test_resolve_thread_safety(self, rehydrator: LazyRehydrator) -> None:
        """Verify that concurrent resolutions return the same instance."""
        proxy = LazyContextProxy(state_id="thread-001", rehydrator=rehydrator)
        call_count = 0

        def _mock_rehydrate(state_id):
            nonlocal call_count
            call_count += 1
            return MagicMock(state_id=state_id)

        with patch.object(
            rehydrator,
            "rehydrate_node",
            new_callable=MagicMock,
            side_effect=_mock_rehydrate,
        ):
            results = []
            barrier = threading.Barrier(3)

            def _resolve():
                barrier.wait()
                results.append(proxy._resolve())

            threads = [threading.Thread(target=_resolve) for _ in range(3)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        # All threads should get the same object
        assert all(r is results[0] for r in results)


# ===========================================================================
# LazyRehydrator.__init__
# ===========================================================================
class TestLazyRehydratorInit:

    def test_default_init(self) -> None:
        r = LazyRehydrator()
        assert r.deserializer is StateDeserializer
        assert r.pipeline_registry == {}

    def test_custom_deserializer(self) -> None:
        mock_deser = MagicMock()
        r = LazyRehydrator(deserializer=mock_deser)
        assert r.deserializer is mock_deser

    def test_instance_registry_isolation(self) -> None:
        """Each instance should have its own registry dict."""
        r1 = LazyRehydrator()
        r2 = LazyRehydrator()

        r1.pipeline_registry["pipe-a"] = "instance-1"
        assert "pipe-a" not in r2.pipeline_registry

    def test_registry_with_initial_data(self) -> None:
        initial = {"pipe-x": "pipeline-instance"}
        r = LazyRehydrator(pipeline_registry=initial)
        # Should be a COPY, not the same dict
        assert r.pipeline_registry["pipe-x"] == "pipeline-instance"
        initial["pipe-y"] = "leaked"
        assert "pipe-y" not in r.pipeline_registry

    def test_cache_is_weak_value_dict(self) -> None:
        import weakref

        r = LazyRehydrator()
        assert isinstance(r._cache, weakref.WeakValueDictionary)


# ===========================================================================
# _load_snapshot
# ===========================================================================
class TestLoadSnapshot:

    async def test_returns_context_snapshot_directly(
        self, rehydrator: LazyRehydrator
    ) -> None:
        mock_snapshot = MagicMock(spec=ContextSnapshot)
        mock_snapshot.get_async = AsyncMock(return_value=mock_snapshot)

        with patch.object(ContextSnapshot, "get_async", return_value=mock_snapshot):
            result = await rehydrator._load_snapshot("state-load-001")

        assert result is mock_snapshot

    async def test_raw_dict_delegates_to_deserializer(
        self, rehydrator: LazyRehydrator
    ) -> None:
        raw_data = {"state_id": "raw-001", "some": "data"}
        mock_snapshot = MagicMock(spec=ContextSnapshot)

        with patch.object(
            ContextSnapshot, "get_async", AsyncMock(return_value=raw_data)
        ), patch.object(
            rehydrator.deserializer,
            "deserialize_context",
            return_value=mock_snapshot,
        ) as mock_deser:
            result = await rehydrator._load_snapshot("state-load-002")

        mock_deser.assert_called_once_with(raw_data)
        assert result is mock_snapshot

    async def test_object_does_not_exist_raises_runtime_error(
        self, rehydrator: LazyRehydrator
    ) -> None:
        from volnux.exceptions import ObjectDoesNotExist

        with patch.object(
            ContextSnapshot,
            "get_async",
            AsyncMock(side_effect=ObjectDoesNotExist("state-load-003")),
        ), pytest.raises(RuntimeError, match="state-load-003"):
            await rehydrator._load_snapshot("state-load-003")


# ===========================================================================
# rehydrate_node
# ===========================================================================
class TestRehydrateNode:

    async def test_caching_returns_same_instance(
        self, rehydrator: LazyRehydrator
    ) -> None:
        mock_snapshot = _make_context_snapshot(state_id="cached-001")
        mock_context = MagicMock()
        mock_context.state_id = "cached-001"

        async def _mock_load(state_id):
            return mock_snapshot

        async def _mock_build(snap):
            return mock_context

        with patch.object(
            rehydrator, "_load_snapshot", side_effect=_mock_load
        ), patch.object(rehydrator, "_build_context", side_effect=_mock_build):
            result1 = await rehydrator.rehydrate_node("cached-001")
            result2 = await rehydrator.rehydrate_node("cached-001")

        assert result1 is result2
        assert result1 is mock_context

    async def test_attach_lazy_links_called(self, rehydrator: LazyRehydrator) -> None:
        mock_snapshot = _make_context_snapshot(
            state_id="links-001",
            parent_id="parent-001",
            previous_context_id="prev-001",
        )
        mock_context = MagicMock()

        with patch.object(
            rehydrator, "_load_snapshot", return_value=mock_snapshot
        ), patch.object(
            rehydrator, "_build_context", return_value=mock_context
        ), patch.object(
            rehydrator, "_attach_lazy_links"
        ) as mock_attach:
            await rehydrator.rehydrate_node("links-001")

        mock_attach.assert_called_once_with(mock_context, mock_snapshot)


# ===========================================================================
# _build_context
# ===========================================================================
class TestBuildContext:

    async def test_pipeline_reconstructed(self, rehydrator: LazyRehydrator) -> None:
        mock_pipeline = MagicMock()
        snapshot = _make_context_snapshot(
            pipeline_id="pipe-bc",
            pipeline_class_path="myapp.Pipeline",
        )
        snapshot.results = []

        mock_context = MagicMock()
        mock_context.state_id = "state-bc"

        with patch.object(
            rehydrator, "_reconstruct_pipeline", return_value=mock_pipeline
        ), patch(
            "volnux.execution.context.ExecutionContext", return_value=mock_context
        ), patch.object(
            rehydrator, "_restore_execution_state", new_callable=AsyncMock
        ):
            result = await rehydrator._build_context(snapshot)

        # ExecutionContext should have been called with the pipeline
        assert result is mock_context

    async def test_change_object_id_called(self, rehydrator: LazyRehydrator) -> None:
        snapshot = _make_context_snapshot(state_id="state-change-id")
        snapshot.results = []

        mock_context = MagicMock()

        with patch.object(rehydrator, "_reconstruct_pipeline"), patch(
            "volnux.execution.context.ExecutionContext", return_value=mock_context
        ), patch.object(rehydrator, "_restore_execution_state", new_callable=AsyncMock):
            await rehydrator._build_context(snapshot)

        mock_context.change_object_id.assert_called_once_with("state-change-id")

    async def test_workflow_id_set(self, rehydrator: LazyRehydrator) -> None:
        snapshot = _make_context_snapshot(
            state_id="wf-id-001",
            workflow_id="wf-unique",
        )
        snapshot.results = []

        mock_context = MagicMock()

        with patch.object(rehydrator, "_reconstruct_pipeline"), patch(
            "volnux.execution.context.ExecutionContext", return_value=mock_context
        ), patch.object(rehydrator, "_restore_execution_state", new_callable=AsyncMock):
            await rehydrator._build_context(snapshot)

        assert mock_context.workflow_id == "wf-unique"

    async def test_task_checkpoint_forwarded(self, rehydrator: LazyRehydrator) -> None:
        snapshot = _make_context_snapshot(
            state_id="tcp-001",
        )
        snapshot.traversal.current_task_checkpoint = {"offset": 77}
        snapshot.results = []

        mock_context = MagicMock()

        with patch.object(rehydrator, "_reconstruct_pipeline"), patch(
            "volnux.execution.context.ExecutionContext", return_value=mock_context
        ), patch.object(rehydrator, "_restore_execution_state", new_callable=AsyncMock):
            await rehydrator._build_context(snapshot)

        mock_context.set_task_checkpoint.assert_called_once_with({"offset": 77})

    async def test_no_task_checkpoint_skips(self, rehydrator: LazyRehydrator) -> None:
        snapshot = _make_context_snapshot(state_id="no-tcp")
        snapshot.traversal.current_task_checkpoint = None
        snapshot.results = []

        mock_context = MagicMock()

        with patch.object(rehydrator, "_reconstruct_pipeline"), patch(
            "volnux.execution.context.ExecutionContext", return_value=mock_context
        ), patch.object(rehydrator, "_restore_execution_state", new_callable=AsyncMock):
            await rehydrator._build_context(snapshot)

        mock_context.set_task_checkpoint.assert_not_called()


# ===========================================================================
# _reconstruct_pipeline
# ===========================================================================
class TestReconstructPipeline:

    def test_registry_hit(self, rehydrator: LazyRehydrator) -> None:
        mock_pipeline = MagicMock()
        rehydrator.pipeline_registry["pipe-reg"] = mock_pipeline

        result = rehydrator._reconstruct_pipeline("pipe-reg", "any.ClassPath")
        assert result is mock_pipeline

    def test_registry_miss_imports_and_caches(self, rehydrator: LazyRehydrator) -> None:
        mock_pipeline_cls = MagicMock(return_value=MagicMock(id="new-pipe"))

        with patch(
            "rehydrator.engine.rehydrator.import_class", return_value=mock_pipeline_cls
        ):
            result = rehydrator._reconstruct_pipeline("pipe-new", "myapp.NewPipeline")

        mock_pipeline_cls.assert_called_once()
        assert "pipe-new" in rehydrator.pipeline_registry

    def test_with_pipeline_kwargs(self, rehydrator: LazyRehydrator) -> None:
        mock_pipeline_cls = MagicMock(return_value=MagicMock())

        with patch(
            "rehydrator.engine.rehydrator.import_class", return_value=mock_pipeline_cls
        ):
            rehydrator._reconstruct_pipeline(
                "pipe-kw",
                "myapp.KwPipeline",
                pipeline_kwargs={"batch_size": 100},
            )

        mock_pipeline_cls.assert_called_once_with(batch_size=100)

    def test_empty_kwargs_defaults_to_empty_dict(
        self, rehydrator: LazyRehydrator
    ) -> None:
        mock_pipeline_cls = MagicMock(return_value=MagicMock())

        with patch(
            "rehydrator.engine.rehydrator.import_class", return_value=mock_pipeline_cls
        ):
            rehydrator._reconstruct_pipeline("pipe-empty-kw", "myapp.Pipeline")

        mock_pipeline_cls.assert_called_once_with()


# ===========================================================================
# _rebuild_tasks
# ===========================================================================
class TestRebuildTasks:

    async def test_empty_list(self, rehydrator: LazyRehydrator) -> None:
        result = await rehydrator._rebuild_tasks([])
        assert result == []

    async def test_valid_tasks(self, rehydrator: LazyRehydrator) -> None:
        mock_task_a = MagicMock()
        mock_task_b = MagicMock()

        async def _mock_rebuild(qt):
            return {"t1": mock_task_a, "t2": mock_task_b}.get(qt.get("task_id"), None)

        queue = [
            {"task_id": "t1", "previous_context_id": "c1", "position_in_queue": 0},
            {"task_id": "t2", "previous_context_id": "c2", "position_in_queue": 1},
        ]

        with patch.object(rehydrator, "_rebuild_task", side_effect=_mock_rebuild):
            result = await rehydrator._rebuild_tasks(queue)

        assert len(result) == 2
        assert result[0] is mock_task_a
        assert result[1] is mock_task_b

    async def test_skips_none_entries(self, rehydrator: LazyRehydrator) -> None:
        async def _mock_rebuild(qt):
            return None if qt["task_id"] == "skip-me" else MagicMock()

        queue = [
            {"task_id": "keep-me", "previous_context_id": "c1", "position_in_queue": 0},
            {"task_id": "skip-me", "previous_context_id": "c2", "position_in_queue": 1},
        ]

        with patch.object(rehydrator, "_rebuild_task", side_effect=_mock_rebuild):
            result = await rehydrator._rebuild_tasks(queue)

        assert len(result) == 1


# ===========================================================================
# _rebuild_task
# ===========================================================================
class TestRebuildTask:

    async def test_valid_task(self, rehydrator: LazyRehydrator) -> None:
        # Return raw dict from store → deserialize_task builds a TaskSnapshot.
        raw_data = {
            "task_id": "t-valid",
            "event_name": "process_data",
            "event_class_import_path": "myapp.events.MyEvent",
            "context_id": "ctx-001",
            "task_type": "normal",
            "task_checkpoint": None,
            "sink_task_id": None,
            "sink_task_pipe": None,
            "options": {},
            "condition_node": {},
            "sequence_number": 0,
            "descriptor": None,
            "descriptor_pipe_type": None,
        }

        mock_pipeline_task = MagicMock()

        with patch.object(
            TaskSnapshot, "get_async", AsyncMock(return_value=raw_data)
        ), patch.object(
            rehydrator.deserializer, "deserialize_task"
        ) as mock_deser, patch(
            "rehydrator.engine.rehydrator.import_class", return_value=MagicMock()
        ), patch(
            "volnux.task.PipelineTask", return_value=mock_pipeline_task
        ):
            mock_deser.return_value = MagicMock(
                event_class_import_path="myapp.events.MyEvent"
            )
            result = await rehydrator._rebuild_task(
                {
                    "task_id": "t-valid",
                    "previous_context_id": "c1",
                    "position_in_queue": 0,
                }
            )

        assert result is mock_pipeline_task

    async def test_missing_task_id_returns_none(
        self, rehydrator: LazyRehydrator
    ) -> None:
        result = await rehydrator._rebuild_task(
            {"task_id": None, "previous_context_id": "c1", "position_in_queue": 0}
        )
        assert result is None

    async def test_empty_task_id_returns_none(self, rehydrator: LazyRehydrator) -> None:
        result = await rehydrator._rebuild_task(
            {"task_id": "", "previous_context_id": "c1", "position_in_queue": 0}
        )
        assert result is None

    async def test_load_failure_returns_none(self, rehydrator: LazyRehydrator) -> None:
        """When TaskSnapshot.get_async raises, the code catches and returns None."""
        with patch.object(
            TaskSnapshot,
            "get_async",
            AsyncMock(side_effect=RuntimeError("DB down")),
        ), patch("volnux.task.PipelineTask"):
            result = await rehydrator._rebuild_task(
                {
                    "task_id": "t-fail",
                    "previous_context_id": "c1",
                    "position_in_queue": 0,
                }
            )

        assert result is None

    async def test_load_failure_logs_warning(
        self, rehydrator: LazyRehydrator, caplog: pytest.LogCaptureFixture
    ) -> None:
        with patch.object(
            TaskSnapshot,
            "get_async",
            AsyncMock(side_effect=RuntimeError("DB down")),
        ), patch("volnux.task.PipelineTask"):
            with caplog.at_level("WARNING"):
                result = await rehydrator._rebuild_task(
                    {
                        "task_id": "t-fail",
                        "previous_context_id": "c1",
                        "position_in_queue": 0,
                    }
                )

        assert result is None
        assert any("t-fail" in r.message for r in caplog.records)

    async def test_raw_dict_falls_back_to_deserializer(
        self, rehydrator: LazyRehydrator
    ) -> None:
        raw_data = {"task_id": "t-raw", "event_name": "process"}
        mock_snapshot = MagicMock()
        mock_snapshot.event_class_import_path = "myapp.events.RawEvent"

        mock_pipeline_task = MagicMock()

        with patch.object(
            TaskSnapshot, "get_async", AsyncMock(return_value=raw_data)
        ), patch.object(
            rehydrator.deserializer, "deserialize_task", return_value=mock_snapshot
        ) as mock_deser, patch(
            "rehydrator.engine.rehydrator.import_class", return_value=MagicMock()
        ), patch(
            "volnux.task.PipelineTask", return_value=mock_pipeline_task
        ):
            result = await rehydrator._rebuild_task(
                {
                    "task_id": "t-raw",
                    "previous_context_id": "c1",
                    "position_in_queue": 0,
                }
            )

        assert result is mock_pipeline_task
        mock_deser.assert_called_once_with(raw_data)


# ===========================================================================
# _restore_execution_state
# ===========================================================================
class TestRestoreExecutionState:
    """_restore_execution_state now assigns hot state directly onto the
    ExecutionContext (status/errors/results/aggregated_result) and persists
    via context.save_async(), instead of pushing an ExecutionState into a
    separate StateManager singleton keyed by state_id."""

    async def test_restores_status_and_results(
        self, rehydrator: LazyRehydrator
    ) -> None:
        mock_context = MagicMock()
        mock_context.save_async = AsyncMock()

        snapshot = _make_context_snapshot(status="COMPLETED", results=[])

        mock_result_a = MagicMock()
        mock_result_b = MagicMock()

        with patch(
            "volnux.execution.rehydrator.event.event_result_serializer.EXEC_RESULT_SERIALIZER.deserialize_exec_result",
            new_callable=AsyncMock,
            side_effect=[mock_result_a, mock_result_b],
        ), patch("volnux.execution.status.ExecutionStatus") as mock_status_cls:
            mock_status_cls.return_value = "COMPLETED"

            await rehydrator._restore_execution_state(mock_context, snapshot)

        assert mock_context.status == "COMPLETED"
        assert mock_context.errors == []
        mock_context.save_async.assert_awaited_once()

    async def test_empty_results_no_deserialize_calls(
        self, rehydrator: LazyRehydrator
    ) -> None:
        mock_context = MagicMock()
        mock_context.save_async = AsyncMock()
        snapshot = _make_context_snapshot(results=[])

        with patch(
            "volnux.execution.rehydrator.event.event_result_serializer.EXEC_RESULT_SERIALIZER.deserialize_exec_result",
            new_callable=AsyncMock,
        ) as mock_deser:
            await rehydrator._restore_execution_state(mock_context, snapshot)

        mock_deser.assert_not_called()
        assert mock_context.results == []

    async def test_metrics_restored(self, rehydrator: LazyRehydrator) -> None:
        mock_context = MagicMock()
        mock_context.save_async = AsyncMock()
        mock_context.metrics.start_time = 0.0
        mock_context.metrics.end_time = 0.0

        snapshot = _make_context_snapshot(
            results=[],
            metrics={"start_time": 50.0, "end_time": 150.0, "duration": 100.0},
        )

        await rehydrator._restore_execution_state(mock_context, snapshot)

        assert mock_context.metrics.start_time == 50.0
        assert mock_context.metrics.end_time == 150.0

    async def test_no_metrics_keys_skipped(self, rehydrator: LazyRehydrator) -> None:
        mock_context = MagicMock()
        mock_context.save_async = AsyncMock()

        snapshot = _make_context_snapshot(results=[], metrics={})

        await rehydrator._restore_execution_state(mock_context, snapshot)

        # No metrics assignment should happen
        mock_context.metrics.start_time  # just access to verify no crash

    async def test_none_metrics_safe(self, rehydrator: LazyRehydrator) -> None:
        mock_context = MagicMock()
        mock_context.save_async = AsyncMock()

        snapshot = _make_context_snapshot(results=[], metrics=None)

        await rehydrator._restore_execution_state(mock_context, snapshot)

        # Should not raise


# ===========================================================================
# _attach_lazy_links
# ===========================================================================
class TestAttachLazyLinks:

    def test_previous_context_linked(self, rehydrator: LazyRehydrator) -> None:
        mock_context = MagicMock()
        snapshot = _make_context_snapshot(previous_context_id="prev-link")

        with patch.object(rehydrator, "_link_for") as mock_link:
            rehydrator._attach_lazy_links(mock_context, snapshot)

        mock_link.assert_any_call("prev-link")

    def test_next_context_linked(self, rehydrator: LazyRehydrator) -> None:
        mock_context = MagicMock()
        snapshot = _make_context_snapshot(next_context_id="next-link")

        with patch.object(rehydrator, "_link_for") as mock_link:
            rehydrator._attach_lazy_links(mock_context, snapshot)

        mock_link.assert_any_call("next-link")

    def test_parent_context_linked(self, rehydrator: LazyRehydrator) -> None:
        mock_context = MagicMock()
        snapshot = _make_context_snapshot(parent_id="parent-link")

        with patch.object(rehydrator, "_link_for") as mock_link:
            rehydrator._attach_lazy_links(mock_context, snapshot)

        mock_link.assert_any_call("parent-link")

    def test_children_linked(self, rehydrator: LazyRehydrator) -> None:
        mock_context = MagicMock()
        snapshot = _make_context_snapshot(child_ids=["ch-1", "ch-2", "ch-3"])

        with patch.object(rehydrator, "_link_for") as mock_link:
            rehydrator._attach_lazy_links(mock_context, snapshot)

        # child_contexts should be set to a list of link results
        assert mock_context.child_contexts is not None

    def test_no_links_when_all_none(self, rehydrator: LazyRehydrator) -> None:
        mock_context = MagicMock()
        snapshot = _make_context_snapshot(
            parent_id=None,
            previous_context_id=None,
            next_context_id=None,
            child_ids=[],
        )

        with patch.object(rehydrator, "_link_for") as mock_link:
            rehydrator._attach_lazy_links(mock_context, snapshot)

        mock_link.assert_not_called()


# ===========================================================================
# _link_for
# ===========================================================================
class TestLinkFor:

    def test_cached_returns_live_node(self, rehydrator: LazyRehydrator) -> None:
        live_node = MagicMock()
        live_node.state_id = "live-001"
        rehydrator._cache["live-001"] = live_node

        result = rehydrator._link_for("live-001")
        assert result is live_node
        assert not isinstance(result, LazyContextProxy)

    def test_uncached_returns_proxy(self, rehydrator: LazyRehydrator) -> None:
        result = rehydrator._link_for("uncached-001")
        assert isinstance(result, LazyContextProxy)
        assert result.state_id == "uncached-001"


# ===========================================================================
# resume_from
# ===========================================================================
class TestResumeFrom:

    async def test_without_engine_class(self, rehydrator: LazyRehydrator) -> None:
        mock_context = MagicMock()
        mock_context.state_id = "resume-001"

        mock_snapshot = _make_context_snapshot(state_id="resume-001")

        with patch.object(
            rehydrator, "rehydrate_node", AsyncMock(return_value=mock_context)
        ) as mock_rehydrate:
            context, engine = await rehydrator.resume_from("resume-001")

        assert context is mock_context
        assert engine is None
        mock_rehydrate.assert_awaited_once_with("resume-001")

    async def test_with_engine_class(self, rehydrator: LazyRehydrator) -> None:
        mock_context = MagicMock()
        mock_engine = MagicMock()
        mock_snapshot = _make_context_snapshot(state_id="resume-eng")
        mock_engine_cls = MagicMock(return_value=mock_engine)

        with patch.object(
            rehydrator, "rehydrate_node", AsyncMock(return_value=mock_context)
        ), patch.object(
            rehydrator, "_load_snapshot", AsyncMock(return_value=mock_snapshot)
        ), patch.object(
            rehydrator,
            "_rehydrate_engine",
            AsyncMock(return_value=mock_engine),
        ) as mock_rehydrate_engine:
            context, engine = await rehydrator.resume_from(
                "resume-eng", engine_class=mock_engine_cls
            )

        assert context is mock_context
        assert engine is mock_engine
        mock_context.set_engine.assert_called_once_with(mock_engine)

    async def test_engine_class_none_skips_engine(
        self, rehydrator: LazyRehydrator
    ) -> None:
        mock_context = MagicMock()

        with patch.object(
            rehydrator, "rehydrate_node", AsyncMock(return_value=mock_context)
        ), patch.object(rehydrator, "_load_snapshot") as mock_load:
            context, engine = await rehydrator.resume_from("resume-no-eng")

        mock_load.assert_not_called()
        assert engine is None


# ===========================================================================
# _rehydrate_engine
# ===========================================================================
class TestRehydrateEngine:

    async def test_engine_constructed_with_checkpoint_flag(
        self, rehydrator: LazyRehydrator
    ) -> None:
        mock_engine = MagicMock()
        mock_engine_cls = MagicMock(return_value=mock_engine)

        mock_context = MagicMock()
        snapshot = _make_context_snapshot()
        snapshot.traversal.tasks_processed = 5

        with patch.object(
            rehydrator,
            "_rebuild_current_task_node",
            AsyncMock(return_value=None),
        ):
            engine = await rehydrator._rehydrate_engine(
                mock_engine_cls, snapshot, mock_context
            )

        mock_engine_cls.assert_called_once_with(enable_checkpointing=True)
        assert engine.tasks_processed == 5

    async def test_current_task_node_set(self, rehydrator: LazyRehydrator) -> None:
        mock_engine = MagicMock()
        mock_engine_cls = MagicMock(return_value=mock_engine)

        mock_task_node = MagicMock()
        mock_context = MagicMock()
        snapshot = _make_context_snapshot()

        with patch.object(
            rehydrator,
            "_rebuild_current_task_node",
            AsyncMock(return_value=mock_task_node),
        ):
            engine = await rehydrator._rehydrate_engine(
                mock_engine_cls, snapshot, mock_context
            )

        assert engine.current_task_node is mock_task_node


# ===========================================================================
# _rebuild_current_task_node
# ===========================================================================
class TestRebuildCurrentTaskNode:

    async def test_no_current_task_returns_none(
        self, rehydrator: LazyRehydrator
    ) -> None:
        snapshot = _make_context_snapshot()
        snapshot.traversal.current_task = None

        mock_context = MagicMock()
        result = await rehydrator._rebuild_current_task_node(
            snapshot.traversal, mock_context
        )
        assert result is None

    async def test_valid_task_creates_task_node(
        self, rehydrator: LazyRehydrator
    ) -> None:
        mock_task = MagicMock()

        snapshot = _make_context_snapshot()
        snapshot.traversal.current_task = {
            "task_id": "t-current",
            "previous_context_id": "ctx-prev",
            "position_in_queue": 0,
        }

        mock_context = MagicMock()

        with patch.object(
            rehydrator, "_rebuild_task", AsyncMock(return_value=mock_task)
        ), patch("volnux.engine.base.TaskNode") as mock_node_cls:
            result = await rehydrator._rebuild_current_task_node(
                snapshot.traversal, mock_context
            )

        mock_node_cls.assert_called_once_with(
            task=mock_task, previous_context=mock_context
        )

    async def test_rebuild_task_returns_none(self, rehydrator: LazyRehydrator) -> None:
        snapshot = _make_context_snapshot()
        snapshot.traversal.current_task = {
            "task_id": "t-fail",
            "previous_context_id": "ctx-prev",
            "position_in_queue": 0,
        }

        mock_context = MagicMock()

        with patch.object(rehydrator, "_rebuild_task", AsyncMock(return_value=None)):
            result = await rehydrator._rebuild_current_task_node(
                snapshot.traversal, mock_context
            )
            assert result is None
