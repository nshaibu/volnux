"""
Tests for `EventCheckpointingMixin`.

Scope:
  * _process_result_tuple — tuple/EventResult validation.
  * _get_steps — ordered list of phase methods, each wrapped by @phase_step.
  * _process_wrapper — sync/async dispatch, EmptyResultError on None,
    _process_result_tuple validation.
  * enqueue_checkpoint — no-op when manager is None (unless
    CHECKPOINT_REQUIRED), delegates to checkpoint_manager.enqueue(snapshot).
  * create_snapshot — delegates to SnapshotBuilder.build(self).
  * _process — happy path (no retry), retry path with success, MaxRetryError
    handling, generic Exception handling, SwitchTask propagation.
  * _pre_process — bypass() returning None → EmptyResultError; bypass() raising
    NotImplementedError → silent return; bypass() returning valid tuple →
    SkipExecutionError; bypass() returning invalid → ValueError.
  * _post_process — on_success when exec_status, on_failure otherwise.
  * _completed — iterates external resources, cleans up each, calls
    self.cleanup() in a try/except.
  * acquire_resource — already-managed path (restore), init_func path,
    provider-string path; starts resource_monitor on first acquisition.
  * sub_phase — cache hit returns cached value; first call executes
    init_func; suspend_on=True raises SubPhaseSuspension and caches; serialise
    /deserialise transform pair; timeout raises asyncio.TimeoutError.
  * _register_resource — TypeError when provider is not ResourceProvider;
    ValueError when missing save_state/restore_state; success path stores
    ResourceState dict + resource instance.
  * _restore_resource — missing provider_path returns None; ImportError
    swallowed; success path binds resource to attribute when bind=True.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Any
from unittest.mock import MagicMock, AsyncMock, patch

import pytest


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------


def _import_mixin():
    """Try common module paths for the checkpointing mixin."""
    candidates = [
        "volnux.mixins._checkpointing",  # likely path given the relative imports
        "volnux.mixins.checkpointing",
        "volnux.mixins.event_checkpointing",
    ]
    import importlib

    last_err = None
    for path in candidates:
        try:
            mod = importlib.import_module(path)
            if hasattr(mod, "EventCheckpointingMixin"):
                return mod
        except ImportError as e:
            last_err = e
    raise ImportError(
        f"Could not import EventCheckpointingMixin from any of {candidates}. "
        f"Last error: {last_err!r}. Adjust the import in this test file to "
        f"match your repo layout."
    )


# ---------------------------------------------------------------------------
# _process_result_tuple
# ---------------------------------------------------------------------------


class TestProcessResultTuple:
    def test_valid_tuple_returns_unchanged(self):
        mod = _import_mixin()
        result = mod.EventCheckpointingMixin._process_result_tuple((True, {"x": 1}))
        assert result == (True, {"x": 1})

    def test_event_result_is_unwrapped_to_success_and_self(self):
        mod = _import_mixin()
        from volnux.result import EventResult

        er = EventResult(error=False, content={"k": "v"}, task_id="t1", event_name="E")
        success, returned = mod.EventCheckpointingMixin._process_result_tuple(er)
        assert success is False  # EventResult.success property
        assert returned is er

    def test_non_tuple_non_event_result_raises_value_error(self):
        mod = _import_mixin()
        with pytest.raises(ValueError, match="must be a tuple"):
            mod.EventCheckpointingMixin._process_result_tuple("not-a-tuple")

    def test_tuple_with_non_bool_first_element_raises(self):
        mod = _import_mixin()
        with pytest.raises(ValueError, match="First element.*must be a boolean"):
            mod.EventCheckpointingMixin._process_result_tuple(("yes", "data"))  # type: ignore[arg-type]

    def test_none_raises_value_error(self):
        mod = _import_mixin()
        with pytest.raises(ValueError):
            mod.EventCheckpointingMixin._process_result_tuple(None)

    def test_custom_method_name_appears_in_error(self):
        mod = _import_mixin()
        with pytest.raises(ValueError, match="bypass"):
            mod.EventCheckpointingMixin._process_result_tuple("x", method_name="bypass")


# ---------------------------------------------------------------------------
# _get_steps
# ---------------------------------------------------------------------------


class TestGetSteps:
    def test_get_steps_returns_six_lifecycle_methods(self, event):
        steps = event._get_steps()
        # Expected order: _setup_event, _pre_process, _communicate, _process,
        # _post_process, _completed
        assert len(steps) == 6
        names = [getattr(s, "__name__", None) for s in steps]
        assert names == [
            "_setup_event",
            "_pre_process",
            "_communicate",
            "_process",
            "_post_process",
            "_completed",
        ]

    def test_each_step_is_marked_with_is_step(self, event):
        """All steps wrapped by @phase_step should have _is_step = True."""
        steps = event._get_steps()
        for step in steps:
            # _setup_event is invoked from __init__ directly, not via steps_runner
            # in the current source. But it's still in the list. It should still
            # be decorated.
            assert getattr(step, "_is_step", False) is True

    def test_each_step_has_phase_attribute(self, event):
        from volnux.execution.rehydrator.event.snapshot import EventPhase

        steps = event._get_steps()
        expected_phases = [
            EventPhase.INITIALIZED,
            EventPhase.PRE_PROCESS,
            EventPhase.COMMUNICATING,
            EventPhase.PROCESSING,
            EventPhase.POST_PROCESS,
            EventPhase.COMPLETED,
        ]
        for step, expected_phase in zip(steps, expected_phases):
            assert getattr(step, "_phase", None) == expected_phase


# ---------------------------------------------------------------------------
# _process_wrapper
# ---------------------------------------------------------------------------


class TestProcessWrapper:
    @pytest.mark.asyncio
    async def test_async_process_returned_directly(self, event):
        async def _proc(*args, **kwargs):
            return True, {"ok": True}

        event.process = _proc
        result = await event._process_wrapper()
        assert result == (True, {"ok": True})

    @pytest.mark.asyncio
    async def test_sync_process_offloaded_to_thread(self, event):
        from volnux.concurrency.async_utils import to_thread

        def _proc(*args, **kwargs):
            return True, {"sync": True}

        event.process = _proc
        # Just verify it runs without raising — to_thread handles the dispatch.
        result = await event._process_wrapper()
        assert result == (True, {"sync": True})

    @pytest.mark.asyncio
    async def test_none_result_raises_empty_result_error(self, event):
        from volnux.exceptions import EmptyResultError

        async def _proc(*args, **kwargs):
            return None

        event.process = _proc
        with pytest.raises(EmptyResultError):
            await event._process_wrapper()

    @pytest.mark.asyncio
    async def test_invalid_result_format_raises_value_error(self, event):
        async def _proc(*args, **kwargs):
            return "not-a-tuple"

        event.process = _proc
        with pytest.raises(ValueError):
            await event._process_wrapper()

    @pytest.mark.asyncio
    async def test_event_result_returned_unchanged(self, event):
        from volnux.result import EventResult

        er = EventResult(error=False, content={"k": "v"}, task_id="t1", event_name="E")

        async def _proc(*args, **kwargs):
            return er

        event.process = _proc
        success, returned = await event._process_wrapper()
        assert returned is er

    @pytest.mark.asyncio
    async def test_passes_args_kwargs_to_process(self, event):
        received = {}

        async def _proc(*args, **kwargs):
            received["args"] = args
            received["kwargs"] = kwargs
            return True, None

        event.process = _proc
        await event._process_wrapper("pos1", key="v")
        assert received["args"] == ("pos1",)
        assert received["kwargs"] == {"key": "v"}


# ---------------------------------------------------------------------------
# enqueue_checkpoint
# ---------------------------------------------------------------------------


class TestEnqueueCheckpoint:

    @pytest.mark.asyncio
    async def test_no_manager_no_required_returns_silently(self, event):
        event.checkpoint_manager = None
        # CHECKPOINT_REQUIRED not set on stub config → returns False
        await event.enqueue_checkpoint()  # should not raise

    @pytest.mark.asyncio
    async def test_no_manager_with_required_raises_runtime_error(
        self, event, volnux_config_stub
    ):
        event.checkpoint_manager = None
        volnux_config_stub.set("CHECKPOINT_REQUIRED", True)
        with pytest.raises(RuntimeError, match="Checkpointing is required"):
            await event.enqueue_checkpoint()

    @pytest.mark.asyncio
    async def test_calls_manager_enqueue_with_snapshot(self, event, checkpoint_manager):
        event.checkpoint_manager = checkpoint_manager
        sentinel_snapshot = MagicMock(name="snapshot")
        with patch.object(
            event, "create_snapshot", new=AsyncMock(return_value=sentinel_snapshot)
        ):
            await event.enqueue_checkpoint()
            checkpoint_manager.enqueue.assert_called_once_with(sentinel_snapshot)

    @pytest.mark.asyncio
    async def test_swallows_snapshot_creation_error_when_not_required(
        self, event, checkpoint_manager, volnux_config_stub
    ):
        event.checkpoint_manager = checkpoint_manager
        # Make sure CHECKPOINT_REQUIRED is off
        volnux_config_stub.set("CHECKPOINT_REQUIRED", False)
        with patch.object(
            event, "create_snapshot", new=AsyncMock(side_effect=RuntimeError("boom"))
        ):
            # Should not raise — error is logged, not propagated
            await event.enqueue_checkpoint()

    @pytest.mark.asyncio
    async def test_reraises_snapshot_error_when_required(
        self, event, checkpoint_manager, volnux_config_stub
    ):
        event.checkpoint_manager = checkpoint_manager
        volnux_config_stub.set("CHECKPOINT_REQUIRED", True)
        with patch.object(
            event, "create_snapshot", new=AsyncMock(side_effect=RuntimeError("boom"))
        ):
            with pytest.raises(RuntimeError, match="boom"):
                await event.enqueue_checkpoint()


# ---------------------------------------------------------------------------
# create_snapshot
# ---------------------------------------------------------------------------


class TestCreateSnapshot:
    @pytest.mark.asyncio
    async def test_delegates_to_snapshot_builder(self, event):
        sentinel = MagicMock(name="snapshot")
        builder_instance = MagicMock()
        builder_instance.build = AsyncMock(return_value=sentinel)
        with patch(
            "volnux.mixins._checkpointing.SnapshotBuilder",
            return_value=builder_instance,
        ):
            result = await event.create_snapshot()
            assert result is sentinel
            builder_instance.build.assert_awaited_once_with(event)


# ---------------------------------------------------------------------------
# _process (phase-stepped)
# ---------------------------------------------------------------------------


class TestProcess:

    @pytest.mark.asyncio
    async def test_no_retry_policy_calls_wrapper_directly(self, event):
        """When retry_policy is None, _process calls _process_wrapper without
        going through _retry, sets exec_status/exec_result."""
        event.retry_policy = None
        with patch.object(
            event,
            "_process_wrapper",
            new=AsyncMock(return_value=(True, {"ok": True})),
        ) as mock_pw:
            await event._process()
            mock_pw.assert_awaited_once()
            assert event.exec_status is True
            assert event.exec_result == {"ok": True}

    @pytest.mark.asyncio
    async def test_retry_policy_with_success(self, event):
        from volnux.mixins.event import RetryPolicy

        event.retry_policy = RetryPolicy(max_attempts=3)
        with patch.object(
            event,
            "_process_wrapper",
            new=AsyncMock(return_value=(True, {"ok": True})),
        ):
            with patch.object(
                event, "_retry", new=AsyncMock(return_value=(True, {"ok": True}))
            ) as mock_retry:
                await event._process()
                mock_retry.assert_awaited_once()
                assert event.exec_status is True
                assert event.exec_result == {"ok": True}

    @pytest.mark.asyncio
    async def test_retry_policy_with_max_retry_error_sets_failure(self, event):
        from volnux.mixins.event import RetryPolicy
        from volnux.exceptions import MaxRetryError

        event.retry_policy = RetryPolicy(max_attempts=2)
        inner = ValueError("the cause")
        max_retry = MaxRetryError(attempt=2, exception=inner, reason="exhausted")
        with patch.object(event, "_retry", new=AsyncMock(side_effect=max_retry)):
            await event._process()
            assert event.exec_status is False
            assert event.exec_result is inner

    @pytest.mark.asyncio
    async def test_retry_policy_with_generic_exception_sets_failure(self, event):
        from volnux.mixins.event import RetryPolicy

        event.retry_policy = RetryPolicy(max_attempts=2)
        with patch.object(
            event, "_retry", new=AsyncMock(side_effect=RuntimeError("boom"))
        ):
            await event._process()
            assert event.exec_status is False
            assert isinstance(event.exec_result, RuntimeError)

    @pytest.mark.asyncio
    async def test_switch_task_propagates_without_logging_error(self, event, caplog):
        """SwitchTask is a control-flow signal — _process must NOT log it as
        an error (only isinstance check), and it should propagate."""
        from volnux.mixins.event import RetryPolicy
        from volnux.exceptions import SwitchTask

        event.retry_policy = RetryPolicy(max_attempts=2)
        sentinel_exc = SwitchTask(
            current_task_id="t1", next_task_descriptor=2, result=None, reason="manual"
        )
        with patch.object(event, "_retry", new=AsyncMock(side_effect=sentinel_exc)):
            with caplog.at_level("ERROR"):
                with pytest.raises(SwitchTask):
                    await event._process()
            # No ERROR-level log should mention SwitchTask
            assert not any(
                "SwitchTask" in r.getMessage() and r.levelname == "ERROR"
                for r in caplog.records
            )


# ---------------------------------------------------------------------------
# _pre_process (phase-stepped)
# ---------------------------------------------------------------------------


class TestPreProcess:
    @pytest.mark.asyncio
    async def test_bypass_not_implemented_returns_silently(self, event):
        """When bypass() raises NotImplementedError (default), _pre_process
        returns without raising."""
        with patch.object(event, "bypass", side_effect=NotImplementedError):
            result = await event._pre_process()
            # Returns None implicitly
            assert result is None

    @pytest.mark.asyncio
    async def test_bypass_returning_none_raises_empty_result_error(self, event):
        from volnux.exceptions import EmptyResultError

        with patch.object(event, "bypass", new=AsyncMock(return_value=None)):
            with pytest.raises(EmptyResultError):
                await event._pre_process()

    @pytest.mark.asyncio
    async def test_bypass_returning_valid_tuple_raises_skip_execution(self, event):
        from volnux.exceptions import SkipExecutionError

        with patch.object(
            event, "bypass", new=AsyncMock(return_value=(True, {"skipped": True}))
        ):
            with pytest.raises(SkipExecutionError):
                await event._pre_process()
            # exec_status / exec_result should have been set BEFORE the raise
            assert event.exec_status is True
            assert event.exec_result == {"skipped": True}

    @pytest.mark.asyncio
    async def test_bypass_with_invalid_result_raises_value_error(self, event):
        with patch.object(event, "bypass", new=AsyncMock(return_value="not-a-tuple")):
            with pytest.raises(ValueError):
                await event._pre_process()

    @pytest.mark.asyncio
    async def test_bypass_returning_coroutine_is_awaited(self, event):
        from volnux.exceptions import SkipExecutionError

        async def _bypass():
            return (False, "skipped due to rule")

        # bypass() returns a coroutine — _pre_process should await it.
        # Note: the source uses `result = self.bypass()` then `if asyncio.iscoroutine(result)`.
        # For an async def, calling it returns a coroutine, so this branch works.
        with patch.object(event, "bypass", side_effect=_bypass):
            with pytest.raises(SkipExecutionError):
                await event._pre_process()
            assert event.exec_status is False
            assert event.exec_result == "skipped due to rule"

    async def test_bypass_with_unhandled_exception_propagates(self, event):
        with patch.object(
            event, "bypass", new=AsyncMock(side_effect=RuntimeError("boom"))
        ):
            with pytest.raises(RuntimeError, match="boom"):
                await event._pre_process()


# ---------------------------------------------------------------------------
# _post_process (phase-stepped)
# ---------------------------------------------------------------------------


class TestPostProcess:
    def test_calls_on_success_when_exec_status_true(self, event):
        event.exec_status = True
        event.exec_result = {"done": True}
        sentinel = MagicMock(name="result")
        with patch.object(event, "on_success", return_value=sentinel) as mock_os:
            with patch.object(event, "on_failure") as mock_of:
                result = event._post_process()
                mock_os.assert_called_once_with({"done": True})
                mock_of.assert_not_called()
                assert result is sentinel

    def test_calls_on_failure_when_exec_status_false(self, event):
        event.exec_status = False
        event.exec_result = ValueError("boom")
        sentinel = MagicMock(name="result")
        with patch.object(event, "on_failure", return_value=sentinel) as mock_of:
            with patch.object(event, "on_success") as mock_os:
                result = event._post_process()
                mock_of.assert_called_once_with(ValueError("boom"))
                mock_os.assert_not_called()
                assert result is sentinel


# ---------------------------------------------------------------------------
# _completed (phase-stepped)
# ---------------------------------------------------------------------------


class TestCompleted:

    @pytest.mark.asyncio
    async def test_calls_cleanup_hook(self, event):
        event._external_resources = {}
        with patch.object(event, "cleanup", new=AsyncMock()) as mock_cleanup:
            result = await event._completed()
            mock_cleanup.assert_awaited_once()
            assert result is None

    @pytest.mark.asyncio
    async def test_swallows_cleanup_exception(self, event, caplog):
        import logging

        event._external_resources = {}
        with patch.object(
            event, "cleanup", new=AsyncMock(side_effect=RuntimeError("cleanup boom"))
        ):
            with caplog.at_level(logging.ERROR):
                # Should not raise
                result = await event._completed()
            assert result is None
            assert any("cleanup" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_iterates_external_resources_for_provider_cleanup(self, event):
        """For each registered external resource with a provider_path, the
        provider class's cleanup() is invoked on the resource instance."""
        # Build a fake provider class and patch `import_string` to return it.
        cleanup_calls: list = []

        class _FakeProvider:
            @classmethod
            def save_state(cls, resource):
                return {"data": "stub"}

            @classmethod
            def restore_state(cls, data):
                return {"restored": data}

            @classmethod
            async def cleanup(cls, resource):
                cleanup_calls.append(resource)

        event._external_resources = {
            "my_resource": {
                "resource_name": "my_resource",
                "provider_path": "some.module._FakeProvider",
                "data": {"x": 1},
            }
        }
        event._resource_instances = {"my_resource": {"the_resource": True}}

        with patch(
            "volnux.mixins._checkpointing.import_string",
            return_value=_FakeProvider,
        ):
            await event._completed()
            assert cleanup_calls == [{"the_resource": True}]

    @pytest.mark.asyncio
    async def test_swallows_provider_cleanup_failure(self, event, caplog):
        """If a provider's cleanup raises, _completed should log a warning
        but continue to the next resource and to the cleanup() hook."""
        import logging

        class _BadProvider:
            @classmethod
            def save_state(cls, resource):
                return {}

            @classmethod
            def restore_state(cls, data):
                return None

            @classmethod
            async def cleanup(cls, resource):
                raise RuntimeError("provider cleanup boom")

        event._external_resources = {
            "bad": {
                "resource_name": "bad",
                "provider_path": "some.module._BadProvider",
                "data": {},
            }
        }
        event._resource_instances = {"bad": object()}

        with patch.object(event, "cleanup", new=AsyncMock()):
            with patch(
                "volnux.mixins._checkpointing.import_string",
                return_value=_BadProvider,
            ):
                with caplog.at_level(logging.WARNING):
                    # Should not raise
                    await event._completed()
                # A warning should mention the failed resource
                assert any("bad" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# acquire_resource
# ---------------------------------------------------------------------------


class TestAcquireResource:

    @pytest.mark.asyncio
    async def test_existing_resource_returns_restored_instance(self, event):
        """When the resource name is already in _external_resources, the
        method restores it instead of re-initialising."""
        from volnux.execution.rehydrator.event.resources import ResourceProvider

        class _FakeProvider(ResourceProvider):
            @classmethod
            def save_state(cls, resource):
                return {"data": "stub"}

            @classmethod
            def restore_state(cls, data):
                return {"restored_from": data}

        event._external_resources = {
            "my_resource": {
                "resource_name": "my_resource",
                "provider_path": f"{_FakeProvider.__module__}.{_FakeProvider.__name__}",
                "data": {"k": "v"},
            }
        }
        # Patch _restore_resource to verify it's called with bind=False
        sentinel = MagicMock(name="restored")
        with patch.object(
            event, "_restore_resource", new=AsyncMock(return_value=sentinel)
        ) as mock_restore:
            result = await event.acquire_resource("my_resource", _FakeProvider, {})
            assert result is sentinel
            mock_restore.assert_awaited_once()
            args, kwargs = mock_restore.call_args
            assert args[0] == "my_resource"

    @pytest.mark.asyncio
    async def test_init_func_path_registers_and_starts_monitor(self, event):
        """When init_func is provided, the resource is created via init_func,
        registered via _register_resource, and the monitor is started if not
        already running."""
        sentinel_resource = MagicMock(name="resource")
        event._resource_monitor = MagicMock()
        event._resource_monitor.is_started = MagicMock(return_value=False)
        event._resource_monitor.start = AsyncMock()

        async def _init_func(args_dict):
            return sentinel_resource

        with patch.object(
            event, "_register_resource", new=AsyncMock()
        ) as mock_register:
            result = await event.acquire_resource(
                "new_resource", "some.path", {"k": "v"}, init_func=_init_func
            )
            assert result is sentinel_resource
            mock_register.assert_awaited_once_with(
                "new_resource", sentinel_resource, "some.path"
            )
            event._resource_monitor.start.assert_awaited_once_with(event)

    @pytest.mark.asyncio
    async def test_init_func_returning_coroutine_is_awaited(self, event):
        """If init_func returns a coroutine, acquire_resource awaits it."""
        event._resource_monitor = MagicMock()
        event._resource_monitor.is_started = MagicMock(return_value=True)
        sentinel = MagicMock(name="resource")

        def _init_func(args_dict):
            async def _inner():
                return sentinel

            return _inner()

        with patch.object(event, "_register_resource", new=AsyncMock()):
            result = await event.acquire_resource("x", "p", {}, init_func=_init_func)
            assert result is sentinel

    @pytest.mark.asyncio
    async def test_provider_path_path_uses_restore_resource(self, event):
        """When init_func is None and provider is a class, acquire_resource
        builds a ResourceState and calls _restore_resource(bind=False)."""
        from volnux.execution.rehydrator.event.resources import ResourceProvider

        class _FakeProvider(ResourceProvider):
            @classmethod
            def save_state(cls, resource):
                return {}

            @classmethod
            def restore_state(cls, data):
                return {"restored": True}

        sentinel = MagicMock(name="restored")
        event._resource_monitor = MagicMock()
        event._resource_monitor.is_started = MagicMock(return_value=True)

        with patch.object(
            event, "_restore_resource", new=AsyncMock(return_value=sentinel)
        ) as mock_restore:
            result = await event.acquire_resource("x", _FakeProvider, {"k": "v"})
            assert result is sentinel
            mock_restore.assert_awaited_once()
            # The ResourceState should have been saved on _external_resources
            assert "x" in event._external_resources


# ---------------------------------------------------------------------------
# sub_phase
# ---------------------------------------------------------------------------


class TestSubPhase:

    @pytest.mark.asyncio
    async def test_first_call_executes_init_func(self, event):
        init_called = False

        def _init():
            nonlocal init_called
            init_called = True
            return {"computed": True}

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            result = await event.sub_phase("compute_block", _init)
            assert init_called is True
            assert result == {"computed": True}

    @pytest.mark.asyncio
    async def test_second_call_returns_cached_value(self, event):
        call_count = 0

        def _init():
            nonlocal call_count
            call_count += 1
            return {"v": call_count}

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            first = await event.sub_phase("block", _init)
            second = await event.sub_phase("block", _init)
            assert first == {"v": 1}
            assert second == {"v": 1}  # cached, not re-executed
            assert call_count == 1

    @pytest.mark.asyncio
    async def test_async_init_func_supported(self, event):
        async def _init():
            return {"async": True}

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            result = await event.sub_phase("async_block", _init)
            assert result == {"async": True}

    @pytest.mark.asyncio
    async def test_suspend_on_true_raises_sub_phase_suspension(self, event):
        from volnux.mixins._checkpointing import SubPhaseSuspension

        def _init():
            return {"pending": True}

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(SubPhaseSuspension) as exc_info:
                await event.sub_phase(
                    "suspend_block", _init, suspend_on=lambda r: r["pending"]
                )
            assert exc_info.value.sub_phase_name == "suspend_block"
            assert exc_info.value.pending_value == {"pending": True}

    @pytest.mark.asyncio
    async def test_suspend_on_true_caches_value(self, event):
        """When suspend_on returns True, the value is cached (serialised if
        serialise provided) BEFORE the suspension is raised."""
        from volnux.mixins._checkpointing import SubPhaseSuspension

        def _init():
            return {"raw": True}

        serialised = {"serialised": True}

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(SubPhaseSuspension):
                await event.sub_phase(
                    "block",
                    _init,
                    suspend_on=lambda r: True,
                    serialise=lambda r: serialised,
                )
            # Cached value should be the serialised form
            assert event._sub_phase_cache.get("block") == serialised

    @pytest.mark.asyncio
    async def test_cached_value_re_suspends_when_suspend_on_still_true(self, event):
        """On resume, if suspend_on(cached) is still True, SubPhaseSuspension
        is re-raised WITHOUT re-executing init_func."""
        from volnux.mixins._checkpointing import SubPhaseSuspension

        # Pre-populate the cache as if a previous run suspended
        event._sub_phase_cache["block"] = {"pending": True}

        init_called = False

        def _init():
            nonlocal init_called
            init_called = True
            return {"should_not_reach": True}

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(SubPhaseSuspension):
                await event.sub_phase("block", _init, suspend_on=lambda r: r["pending"])
            assert init_called is False  # init_func not re-executed

    @pytest.mark.asyncio
    async def test_cached_value_returns_when_suspend_on_false(self, event):
        """On resume, if suspend_on(cached) is False, the cached value is
        returned (deserialised if deserialise provided) without re-executing."""
        event._sub_phase_cache["block"] = {"resolved": True}

        init_called = False

        def _init():
            nonlocal init_called
            init_called = True
            return None

        result = await event.sub_phase(
            "block", _init, suspend_on=lambda r: not r.get("resolved")
        )
        assert result == {"resolved": True}
        assert init_called is False

    @pytest.mark.asyncio
    async def test_serialise_deserialise_pair(self, event):
        """When both serialise and deserialise are provided, the returned
        value passes through deserialise."""

        def _init():
            return "raw-value"

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            result = await event.sub_phase(
                "block",
                _init,
                serialise=lambda r: f"serialised:{r}",
                deserialise=lambda s: s.replace("serialised:", "deserialised:"),
            )
            assert result == "deserialised:raw-value"
            # And the cache stores the serialised form
            assert event._sub_phase_cache["block"] == "serialised:raw-value"

    @pytest.mark.asyncio
    async def test_timeout_raises_asyncio_timeout_error(self, event):
        """When init_func exceeds the timeout, asyncio.TimeoutError is raised
        and the result is NOT cached."""

        async def _slow_init():
            await asyncio.sleep(1.0)
            return {"never": "reached"}

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(asyncio.TimeoutError):
                await event.sub_phase("slow_block", _slow_init, timeout=0.05)
            # Not cached
            assert "slow_block" not in event._sub_phase_cache

    @pytest.mark.asyncio
    async def test_exception_not_cached(self, event):
        """When init_func raises, the result is NOT cached — the next call
        re-executes init_func."""
        attempt = 0

        def _init():
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise RuntimeError("first attempt fails")
            return {"second": True}

        with patch.object(event, "enqueue_checkpoint", new=AsyncMock()):
            with pytest.raises(RuntimeError):
                await event.sub_phase("block", _init)
            # Second attempt should re-execute and succeed
            result = await event.sub_phase("block", _init)
            assert result == {"second": True}
            assert attempt == 2


# ---------------------------------------------------------------------------
# _register_resource
# ---------------------------------------------------------------------------


class TestRegisterResource:

    @pytest.mark.asyncio
    async def test_string_provider_path_imports_and_registers(self, event):
        from volnux.execution.rehydrator.event.resources import ResourceProvider

        class _FakeProvider(ResourceProvider):
            @classmethod
            def save_state(cls, resource):
                return {"data": "stub"}

            @classmethod
            def restore_state(cls, data):
                return None

        # Register via string path
        path = f"{_FakeProvider.__module__}.{_FakeProvider.__name__}"
        sentinel_resource = MagicMock(name="resource")
        await event._register_resource("my_resource", sentinel_resource, path)

        assert "my_resource" in event._external_resources
        state = event._external_resources["my_resource"]
        assert state["resource_name"] == "my_resource"
        assert state["data"] == {"data": "stub"}
        assert state["provider_path"] == path
        assert event._resource_instances["my_resource"] is sentinel_resource

    @pytest.mark.asyncio
    async def test_class_provider_registers_with_module_path(self, event):
        from volnux.execution.rehydrator.event.resources import ResourceProvider

        class _FakeProvider(ResourceProvider):
            @classmethod
            def save_state(cls, resource):
                return {}

            @classmethod
            def restore_state(cls, data):
                return None

        await event._register_resource("r", MagicMock(), _FakeProvider)
        state = event._external_resources["r"]
        assert (
            state["provider_path"]
            == f"{_FakeProvider.__module__}.{_FakeProvider.__name__}"
        )

    @pytest.mark.asyncio
    async def test_non_resource_provider_raises_type_error(self, event):
        class _NotAProvider:
            @classmethod
            def save_state(cls, resource):
                return {}

            @classmethod
            def restore_state(cls, data):
                return None

        with pytest.raises(TypeError, match="ResourceProvider"):
            await event._register_resource("r", MagicMock(), _NotAProvider)

    @pytest.mark.asyncio
    async def test_missing_save_state_raises_value_error(self, event):
        from volnux.execution.rehydrator.event.resources import ResourceProvider

        class _NoSave(ResourceProvider):
            # No save_state
            @classmethod
            def restore_state(cls, data):
                return None

        with pytest.raises(ValueError, match="save_state"):
            await event._register_resource("r", MagicMock(), _NoSave)

    @pytest.mark.asyncio
    async def test_missing_restore_state_raises_value_error(self, event):
        from volnux.execution.rehydrator.event.resources import ResourceProvider

        class _NoRestore(ResourceProvider):
            @classmethod
            def save_state(cls, resource):
                return {}

        with pytest.raises(ValueError, match="restore_state"):
            await event._register_resource("r", MagicMock(), _NoRestore)

    @pytest.mark.asyncio
    async def test_save_state_returning_non_dict_raises_value_error(self, event):
        from volnux.execution.rehydrator.event.resources import ResourceProvider

        class _BadSave(ResourceProvider):
            @classmethod
            def save_state(cls, resource):
                return "not-a-dict"  # type: ignore[return-value]

            @classmethod
            def restore_state(cls, data):
                return None

        with pytest.raises(ValueError, match="must return a dict"):
            await event._register_resource("r", MagicMock(), _BadSave)

    @pytest.mark.asyncio
    async def test_async_save_state_supported(self, event):
        from volnux.execution.rehydrator.event.resources import ResourceProvider

        class _AsyncSave(ResourceProvider):
            @classmethod
            async def save_state(cls, resource):
                return {"async": True}

            @classmethod
            def restore_state(cls, data):
                return None

        await event._register_resource("r", MagicMock(), _AsyncSave)
        assert event._external_resources["r"]["data"] == {"async": True}


# ---------------------------------------------------------------------------
# _restore_resource
# ---------------------------------------------------------------------------


class TestRestoreResource:
    @pytest.mark.asyncio
    async def test_missing_provider_path_returns_none_with_warning(self, event, caplog):
        import logging

        result = await event._restore_resource(
            "r", {"resource_name": "r", "data": {}}, bind=False
        )
        assert result is None
        assert any(
            "r" in r.getMessage()
            for r in caplog.records
            if r.levelno >= logging.WARNING
        )

    @pytest.mark.asyncio
    async def test_successful_restore_returns_resource(self, event):
        from volnux.execution.rehydrator.event.resources import ResourceProvider

        class _FakeProvider(ResourceProvider):
            @classmethod
            def save_state(cls, resource):
                return {}

            @classmethod
            def restore_state(cls, data):
                return {"restored_from": data}

        path = f"{_FakeProvider.__module__}.{_FakeProvider.__name__}"
        sentinel_resource = {"restored_from": {"k": "v"}}

        with patch(
            "volnux.mixins._checkpointing.import_string",
            return_value=_FakeProvider,
        ):
            result = await event._restore_resource(
                "r",
                {"resource_name": "r", "provider_path": path, "data": {"k": "v"}},
                bind=False,
            )
            assert result == sentinel_resource
            assert event._resource_instances["r"] == sentinel_resource

    @pytest.mark.asyncio
    async def test_bind_true_sets_attribute_on_self(self, event):
        from volnux.execution.rehydrator.event.resources import ResourceProvider

        class _FakeProvider(ResourceProvider):
            @classmethod
            def save_state(cls, resource):
                return {}

            @classmethod
            def restore_state(cls, data):
                return {"the_resource": True}

        path = f"{_FakeProvider.__module__}.{_FakeProvider.__name__}"
        with patch(
            "volnux.mixins._checkpointing.import_string",
            return_value=_FakeProvider,
        ):
            await event._restore_resource(
                "my_resource",
                {"resource_name": "my_resource", "provider_path": path, "data": {}},
                bind=True,
            )
            assert getattr(event, "my_resource") == {"the_resource": True}

    @pytest.mark.asyncio
    async def test_async_restore_state_awaited(self, event):
        from volnux.execution.rehydrator.event.resources import ResourceProvider

        class _AsyncRestore(ResourceProvider):
            @classmethod
            def save_state(cls, resource):
                return {}

            @classmethod
            async def restore_state(cls, data):
                return {"async": True}

        path = f"{_AsyncRestore.__module__}.{_AsyncRestore.__name__}"
        with patch(
            "volnux.mixins._checkpointing.import_string",
            return_value=_AsyncRestore,
        ):
            result = await event._restore_resource(
                "r", {"provider_path": path, "data": {}}, bind=False
            )
            assert result == {"async": True}

    @pytest.mark.asyncio
    async def test_import_error_is_swallowed(self, event, caplog):
        """ImportError on the provider path is logged but does NOT raise —
        the event continues without the resource."""
        import logging

        with patch(
            "volnux.mixins._checkpointing.import_string",
            side_effect=ImportError("no such module"),
        ):
            with caplog.at_level(logging.ERROR):
                result = await event._restore_resource(
                    "r", {"provider_path": "no.such.module", "data": {}}, bind=False
                )
                assert result is None
            assert any(
                "r" in r.getMessage()
                for r in caplog.records
                if r.levelno >= logging.ERROR
            )


# ---------------------------------------------------------------------------
# get_phase
# ---------------------------------------------------------------------------


class TestGetPhase:
    def test_returns_current_phase(self, event):
        from volnux.execution.rehydrator.event.snapshot import EventPhase

        # event fixture's _phase was set to INITIALIZED by _setup_event
        assert event.get_phase() == EventPhase.INITIALIZED
