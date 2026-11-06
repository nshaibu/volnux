import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from volnux.exceptions import (
    EmptyResultError,
    MaxRetryError,
    SkipExecutionError,
    StopProcessingError,
    SwitchTask,
)
from volnux.execution.rehydrator.event.resources import ResourceMonitor
from volnux.execution.rehydrator.event.snapshot import EventPhase
from volnux.mixins.event.checkpointer import SubPhaseSuspension
from volnux.mixins.event.retry import RetryPolicy
from volnux.parser.options import StopCondition
from volnux.result import EventResult

from .. import providers
from ..providers import Counter

PROVIDER = "tests.unittests.event.providers.CounterProvider"


@pytest.fixture(autouse=True)
def _clean_providers():
    providers.CLEANED.clear()
    yield
    providers.CLEANED.clear()


async def stop(event):
    if event._resource_monitor.is_started():
        await event._resource_monitor.stop()


class TestSetupEvent:
    def test_state_after_construction(self, build, ctx, checkpoint_manager, channel):
        event = build(
            checkpoint_manager=checkpoint_manager,
            command_channel=channel,
            sequence_number=7,
            run_bypass_event_checks=True,
        )
        assert event._execution_context is ctx
        assert event._task_id == "task-1" and event._sequence_number == 7
        assert event.checkpoint_manager is checkpoint_manager
        assert event._command_channel is channel
        assert event.run_bypass_event_checks is True
        assert event.get_phase() == EventPhase.INITIALIZED
        assert event._active_phase is None and event._bypassed is False
        assert (event.exec_status, event.exec_result) == (False, None)
        assert event._retry_count == 0 and event._preempted is False
        assert event._pause_gate.is_set()
        assert event._external_resources == {} and event._resource_instances == {}
        assert event._sub_phase_cache == {}
        assert event._event_result is None

    def test_init_args_hold_only_init_arguments(
        self, build, checkpoint_manager, channel
    ):
        event = build(checkpoint_manager=checkpoint_manager, command_channel=channel)
        args = event.get_init_args()
        assert args["task_id"] == "task-1"
        assert "checkpoint_manager" not in args and "command_channel" not in args

    def test_call_args_start_empty(self, build):
        from volnux.constants import EMPTY

        assert build().get_call_args() is EMPTY

    def test_event_init_signal_is_emitted_with_init_args(self, build, recorded):
        event = build()
        [emission] = [e for e in recorded["event_init"] if e["event"] is event]
        assert emission["init_kwargs"] is event.get_init_args()

    def test_stop_condition_is_wrapped_in_a_processor(self, build):
        event = build(stop_condition=StopCondition.ON_ERROR)
        assert event.stop_condition.stop_condition == StopCondition.ON_ERROR

    def test_each_event_gets_its_own_state(self, build):
        a, b = build(), build()
        a._sub_phase_cache["x"] = 1
        assert b._sub_phase_cache == {} and a._pause_gate is not b._pause_gate


class TestProcessResultTuple:
    def test_tuple_is_returned_unchanged(self, build):
        assert build()._process_result_tuple((True, "x")) == (True, "x")

    def test_event_result_becomes_success_and_itself(self, build):
        result = EventResult(
            error=False, event_name="E", content=1, workflow_id=None, task_id="t"
        )
        assert build()._process_result_tuple(result) == (True, result)

    def test_failed_event_result(self, build):
        result = EventResult(
            error=True, event_name="E", content=1, workflow_id=None, task_id="t"
        )
        assert build()._process_result_tuple(result)[0] is False

    @pytest.mark.parametrize("bad", ["text", 5, [True, 1], {"a": 1}])
    def test_other_types_are_rejected(self, build, bad):
        with pytest.raises(ValueError, match="tuple"):
            build()._process_result_tuple(bad)

    def test_first_element_must_be_bool(self, build):
        with pytest.raises(ValueError, match="boolean"):
            build()._process_result_tuple(("yes", 1))

    def test_method_name_appears_in_the_message(self, build):
        with pytest.raises(ValueError, match="bypass"):
            build()._process_result_tuple("x", method_name="bypass")


class TestPreProcess:
    @pytest.mark.asyncio
    async def test_default_bypass_means_not_bypassed(self, build):
        event = build()
        assert await event._pre_process() is None
        assert event.get_phase() == EventPhase.PRE_PROCESS
        assert event.exec_status is False

    @pytest.mark.asyncio
    async def test_bypass_result_skips_execution(self, build):
        async def bypass(self):
            return True, {"why": "cached"}

        event = build({"bypass": bypass})
        with pytest.raises(SkipExecutionError):
            await event._pre_process()
        assert (event.exec_status, event.exec_result) == (True, {"why": "cached"})

    @pytest.mark.asyncio
    async def test_sync_bypass_is_supported(self, build):
        event = build({"bypass": lambda self: (False, "skip")})
        with pytest.raises(SkipExecutionError):
            await event._pre_process()
        assert (event.exec_status, event.exec_result) == (False, "skip")

    @pytest.mark.asyncio
    async def test_bypass_is_consulted_regardless_of_the_flag(self, build):
        """run_bypass_event_checks is recorded but does not gate bypass()."""

        async def bypass(self):
            return True, "x"

        event = build({"bypass": bypass}, run_bypass_event_checks=False)
        with pytest.raises(SkipExecutionError):
            await event._pre_process()

    @pytest.mark.asyncio
    async def test_none_result_is_an_error(self, build):
        async def bypass(self):
            return None

        with pytest.raises(EmptyResultError):
            await build({"bypass": bypass})._pre_process()

    @pytest.mark.asyncio
    async def test_invalid_result_is_rejected(self, build):
        async def bypass(self):
            return "yes"

        with pytest.raises(ValueError):
            await build({"bypass": bypass})._pre_process()

    @pytest.mark.asyncio
    async def test_errors_in_bypass_propagate(self, build):
        async def bypass(self):
            raise KeyError("boom")

        with pytest.raises(KeyError):
            await build({"bypass": bypass})._pre_process()

    @pytest.mark.asyncio
    async def test_skipped_pre_process_does_not_advance_the_phase_itself(self, build):
        async def bypass(self):
            return True, 1

        event = build({"bypass": bypass})
        with pytest.raises(SkipExecutionError):
            await event._pre_process()
        # steps_runner sets the phase when it handles SkipExecutionError
        assert event.get_phase() == EventPhase.INITIALIZED


class TestProcess:
    @pytest.mark.asyncio
    async def test_async_process(self, build):
        async def process(self, x):
            return True, x * 2

        event = build({"process": process})
        await event._process(4)
        assert (event.exec_status, event.exec_result) == (True, 8)
        assert event.get_phase() == EventPhase.PROCESSING

    @pytest.mark.asyncio
    async def test_sync_process_runs_off_the_event_loop(self, build):
        import threading

        main = threading.current_thread()

        def process(self):
            return True, threading.current_thread() is main

        event = build({"process": process})
        await event._process()
        assert event.exec_result is False

    @pytest.mark.asyncio
    async def test_failure_tuple(self, build):
        async def process(self):
            return False, "nope"

        event = build({"process": process})
        await event._process()
        assert (event.exec_status, event.exec_result) == (False, "nope")

    @pytest.mark.asyncio
    async def test_event_result_return_value(self, build):
        async def process(self):
            return EventResult(
                error=False, event_name="E", content=1, workflow_id=None, task_id="t"
            )

        event = build({"process": process})
        await event._process()
        assert event.exec_status is True and isinstance(event.exec_result, EventResult)

    @pytest.mark.asyncio
    async def test_exception_becomes_a_failure_result(self, build):
        async def process(self):
            raise ValueError("bad input")

        event = build({"process": process})
        await event._process()
        assert event.exec_status is False
        assert isinstance(event.exec_result, ValueError)

    @pytest.mark.asyncio
    async def test_none_result_is_a_failure(self, build):
        async def process(self):
            return None

        event = build({"process": process})
        await event._process()
        assert event.exec_status is False
        assert isinstance(event.exec_result, EmptyResultError)

    @pytest.mark.asyncio
    async def test_invalid_return_shape_is_a_failure(self, build):
        async def process(self):
            return "not a tuple"

        event = build({"process": process})
        await event._process()
        assert event.exec_status is False and isinstance(event.exec_result, ValueError)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "make_exc",
        [
            lambda: SwitchTask(
                current_task_id="t", next_task_descriptor=1, result=None
            ),
            lambda: StopProcessingError(message="stop", exception=None),
        ],
    )
    async def test_control_flow_propagates(self, build, make_exc):
        async def process(self):
            raise make_exc()

        event = build({"process": process})
        with pytest.raises((SwitchTask, StopProcessingError)):
            await event._process()
        assert event.get_phase() == EventPhase.INITIALIZED  # phase not advanced

    @pytest.mark.asyncio
    async def test_retry_path_recovers(self, build, monkeypatch):
        async def nosleep(*a, **k):
            return None

        monkeypatch.setattr("volnux.mixins.event.retry.asyncio.sleep", nosleep)
        calls = []

        async def process(self):
            calls.append(1)
            if len(calls) < 3:
                raise ValueError("flaky")
            return True, "finally"

        event = build({"process": process, "retry_policy": RetryPolicy(max_attempts=5)})
        await event._process()
        assert (event.exec_status, event.exec_result) == (True, "finally")
        assert len(calls) == 3

    @pytest.mark.asyncio
    async def test_retry_exhaustion_records_the_last_error(self, build, monkeypatch):
        async def nosleep(*a, **k):
            return None

        monkeypatch.setattr("volnux.mixins.event.retry.asyncio.sleep", nosleep)

        async def process(self):
            raise ValueError("always")

        event = build({"process": process, "retry_policy": RetryPolicy(max_attempts=2)})
        await event._process()
        assert event.exec_status is False
        assert isinstance(event.exec_result, ValueError)

    @pytest.mark.asyncio
    async def test_retry_path_propagates_control_flow(self, build):
        async def process(self):
            raise SwitchTask(current_task_id="t", next_task_descriptor=0, result=None)

        event = build({"process": process, "retry_policy": RetryPolicy(max_attempts=3)})
        with pytest.raises(SwitchTask):
            await event._process()
        assert event._retry_count == 1


class TestPostProcess:
    @pytest.mark.asyncio
    async def test_success_builds_a_success_result(self, build):
        event = build(task_id="t9", sequence_number=3)
        event.exec_status, event.exec_result = True, {"k": 1}
        result = event._post_process()
        assert isinstance(result, EventResult) and result.success
        assert result.content == {"k": 1} and result.task_id == "t9"
        assert result.order == 3
        assert event._event_result is result
        assert event.get_phase() == EventPhase.POST_PROCESS

    @pytest.mark.asyncio
    async def test_failure_builds_a_failed_result(self, build):
        event = build()
        event.exec_status, event.exec_result = False, ValueError("x")
        result = event._post_process()
        assert not result.success and isinstance(result.content, ValueError)

    @pytest.mark.asyncio
    async def test_stop_condition_can_abort_post_process(self, build):
        event = build(stop_condition=StopCondition.ON_ERROR)
        event.exec_status, event.exec_result = False, ValueError("x")
        with pytest.raises(StopProcessingError):
            event._post_process()


class TestCompleted:
    @pytest.mark.asyncio
    async def test_runs_the_cleanup_hook(self, build):
        hook = AsyncMock()
        event = build({"cleanup": hook})
        await event._completed(1, a=2)
        hook.assert_awaited_once_with(1, a=2)
        assert event.get_phase() == EventPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_cleanup_hook_error_is_swallowed(self, build):
        async def cleanup(self):
            raise RuntimeError("hook failed")

        event = build({"cleanup": cleanup})
        await event._completed()
        assert event.get_phase() == EventPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_resources_are_cleaned_before_the_hook(self, build):
        order = []

        async def cleanup(self):
            order.append("hook")

        event = build({"cleanup": cleanup})
        counter = Counter(1)
        await event._register_resource("c", counter, PROVIDER)
        original = providers.CounterProvider.cleanup.__func__

        def tracking(cls, resource):
            order.append("resource")
            original(cls, resource)

        providers.CounterProvider.cleanup = classmethod(tracking)
        try:
            await event._completed()
        finally:
            providers.CounterProvider.cleanup = classmethod(original)
        assert order == ["resource", "hook"]
        assert providers.CLEANED == [("sync", counter)]

    @pytest.mark.asyncio
    async def test_async_provider_cleanup_is_awaited(self, build):
        event = build()
        counter = Counter(2)
        await event._register_resource(
            "c", counter, "tests.unittests.event.providers.AsyncCounterProvider"
        )
        await event._completed()
        assert providers.CLEANED == [("async", counter)]

    @pytest.mark.asyncio
    async def test_failing_resource_cleanup_does_not_stop_the_rest(self, build):
        hook = AsyncMock()
        event = build({"cleanup": hook})
        await event._register_resource(
            "bad",
            Counter(),
            "tests.unittests.event.providers.ExplodingCleanupProvider",
        )
        good = Counter(3)
        await event._register_resource("good", good, PROVIDER)
        await event._completed()
        assert ("sync", good) in providers.CLEANED
        hook.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_resource_without_a_live_instance_is_skipped(self, build):
        event = build()
        event._external_resources["ghost"] = {
            "resource_name": "ghost",
            "data": {},
            "provider_path": PROVIDER,
        }
        await event._completed()
        assert providers.CLEANED == []


class TestResources:
    @pytest.mark.asyncio
    async def test_register_stores_state_and_instance(self, build):
        event = build()
        counter = Counter(5)
        await event._register_resource("c", counter, providers.CounterProvider)
        assert event._external_resources["c"] == {
            "resource_name": "c",
            "data": {"value": 5},
            "provider_path": PROVIDER,
        }
        assert event._resource_instances["c"] is counter

    @pytest.mark.asyncio
    async def test_register_with_async_provider(self, build):
        event = build()
        await event._register_resource(
            "c", Counter(2), "tests.unittests.event.providers.AsyncCounterProvider"
        )
        assert event._external_resources["c"]["data"] == {"value": 2}

    @pytest.mark.asyncio
    async def test_register_rejects_non_providers(self, build):
        event = build()
        with pytest.raises(TypeError):
            await event._register_resource(
                "c", Counter(), "tests.unittests.event.providers.NotAProvider"
            )
        assert event._external_resources == {}

    @pytest.mark.asyncio
    async def test_register_rejects_non_dict_state(self, build):
        event = build()
        with pytest.raises(ValueError, match="dict"):
            await event._register_resource(
                "c", Counter(), "tests.unittests.event.providers.BadSaveProvider"
            )

    @pytest.mark.asyncio
    async def test_register_unknown_import_path(self, build):
        with pytest.raises(ImportError):
            await build()._register_resource("c", Counter(), "no.such.Provider")

    @pytest.mark.asyncio
    async def test_restore_creates_and_binds(self, build):
        event = build()
        restored = await event._restore_resource(
            "c",
            {"resource_name": "c", "data": {"value": 9}, "provider_path": PROVIDER},
        )
        assert restored.value == 9
        assert event.c is restored and event._resource_instances["c"] is restored

    @pytest.mark.asyncio
    async def test_restore_without_binding(self, build):
        event = build()
        restored = await event._restore_resource(
            "c",
            {"resource_name": "c", "data": {}, "provider_path": PROVIDER},
            bind=False,
        )
        assert not hasattr(event, "c") and restored is event._resource_instances["c"]

    @pytest.mark.asyncio
    async def test_restore_failures_return_none(self, build):
        event = build()
        assert await event._restore_resource("c", {"data": {}}) is None
        assert (
            await event._restore_resource("c", {"provider_path": "no.such.Provider"})
            is None
        )
        assert (
            await event._restore_resource(
                "c",
                {"provider_path": "tests.unittests.event.providers.NotAProvider"},
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_acquire_with_init_func_registers_and_starts_monitor(self, build):
        event = build()
        try:
            resource = await event.acquire_resource(
                "c",
                providers.CounterProvider,
                {"value": 4},
                init_func=lambda a: Counter(a["value"]),
            )
            assert resource.value == 4
            assert event._external_resources["c"]["data"] == {"value": 4}
            assert event._resource_monitor.is_started()
        finally:
            await stop(event)

    @pytest.mark.asyncio
    async def test_acquire_with_async_init_func(self, build):
        event = build()

        async def init(args):
            return Counter(args["value"])

        try:
            resource = await event.acquire_resource(
                "c", PROVIDER, {"value": 6}, init_func=init
            )
            assert resource.value == 6
        finally:
            await stop(event)

    @pytest.mark.asyncio
    async def test_acquire_without_init_func_restores_from_args(self, build):
        event = build()
        try:
            resource = await event.acquire_resource("c", PROVIDER, {"value": 11})
            assert resource.value == 11
            assert event._external_resources["c"]["provider_path"] == PROVIDER
        finally:
            await stop(event)

    @pytest.mark.asyncio
    async def test_acquire_known_resource_restores_from_saved_state(self, build):
        event = build()
        event._external_resources["c"] = {
            "resource_name": "c",
            "data": {"value": 21},
            "provider_path": PROVIDER,
        }
        init = MagicMock()
        try:
            resource = await event.acquire_resource("c", PROVIDER, {}, init_func=init)
            assert resource.value == 21
            init.assert_not_called()  # restored, not re-created
        finally:
            await stop(event)

    @pytest.mark.asyncio
    async def test_monitor_started_once(self, build):
        event = build()
        try:
            await event.acquire_resource("a", PROVIDER, {"value": 1})
            first = event._resource_monitor._task
            await event.acquire_resource("b", PROVIDER, {"value": 2})
            assert event._resource_monitor._task is first
        finally:
            await stop(event)


class TestResourceMonitor:
    @pytest.mark.asyncio
    async def test_refreshes_saved_state_from_live_resources(self, build):
        event = build()
        counter = Counter(1)
        await event._register_resource("c", counter, PROVIDER)
        monitor = ResourceMonitor(interval=0.01)
        await monitor.start(event)
        try:
            counter.value = 42
            for _ in range(100):
                if event._external_resources["c"]["data"] == {"value": 42}:
                    break
                await asyncio.sleep(0.01)
            assert event._external_resources["c"]["data"] == {"value": 42}
        finally:
            await monitor.stop()
        assert not monitor.is_started()

    @pytest.mark.asyncio
    async def test_skips_resources_without_instances(self, build):
        event = build()
        event._external_resources["ghost"] = {
            "resource_name": "ghost",
            "data": {"value": 1},
            "provider_path": PROVIDER,
        }
        monitor = ResourceMonitor(interval=0.01)
        await monitor.start(event)
        await asyncio.sleep(0.03)
        await monitor.stop()
        assert event._external_resources["ghost"]["data"] == {"value": 1}

    @pytest.mark.asyncio
    async def test_stop_without_start_is_a_noop(self):
        monitor = ResourceMonitor()
        await monitor.stop()
        assert not monitor.is_started()


class TestSubPhase:
    @pytest.mark.asyncio
    async def test_runs_once_and_caches(self, build, checkpoint_manager):
        event = build(checkpoint_manager=checkpoint_manager)
        calls = []

        def work():
            calls.append(1)
            return {"n": 1}

        assert await event.sub_phase("load", work) == {"n": 1}
        assert await event.sub_phase("load", work) == {"n": 1}
        assert len(calls) == 1
        assert event._sub_phase_cache["load"] == {"n": 1}
        checkpoint_manager.enqueue.assert_called()

    @pytest.mark.asyncio
    async def test_async_init_func(self, build):
        async def work():
            return "async"

        assert await build().sub_phase("a", work) == "async"

    @pytest.mark.asyncio
    async def test_distinct_names_are_independent(self, build):
        event = build()
        assert await event.sub_phase("a", lambda: 1) == 1
        assert await event.sub_phase("b", lambda: 2) == 2

    @pytest.mark.asyncio
    async def test_serialise_and_deserialise_round_trip(self, build):
        event = build()
        value = await event.sub_phase(
            "s",
            lambda: {1, 2},
            serialise=lambda v: sorted(v),
            deserialise=lambda v: set(v),
        )
        assert value == {1, 2}
        assert event._sub_phase_cache["s"] == [1, 2]
        again = await event.sub_phase(
            "s", lambda: pytest.fail("cached"), deserialise=lambda v: set(v)
        )
        assert again == {1, 2}

    @pytest.mark.asyncio
    async def test_suspend_on_first_execution(self, build, checkpoint_manager):
        event = build(checkpoint_manager=checkpoint_manager)
        with pytest.raises(SubPhaseSuspension) as info:
            await event.sub_phase(
                "approve",
                lambda: {"status": "pending"},
                suspend_on=lambda v: v["status"] == "pending",
            )
        assert info.value.sub_phase_name == "approve"
        assert info.value.pending_value == {"status": "pending"}
        assert event._sub_phase_cache["approve"] == {"status": "pending"}
        checkpoint_manager.enqueue.assert_called()  # suspended state is checkpointed

    @pytest.mark.asyncio
    async def test_resuspend_from_cache_without_rerunning(self, build):
        event = build()
        event._sub_phase_cache["approve"] = {"status": "pending"}
        with pytest.raises(SubPhaseSuspension):
            await event.sub_phase(
                "approve",
                lambda: pytest.fail("must not re-run"),
                suspend_on=lambda v: v["status"] == "pending",
            )

    @pytest.mark.asyncio
    async def test_resume_after_external_resolution(self, build):
        event = build()
        event._sub_phase_cache["approve"] = {"status": "approved"}
        result = await event.sub_phase(
            "approve",
            lambda: pytest.fail("must not re-run"),
            suspend_on=lambda v: v["status"] == "pending",
        )
        assert result == {"status": "approved"}

    @pytest.mark.asyncio
    async def test_failures_are_not_cached(self, build):
        event = build()
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) == 1:
                raise ValueError("first")
            return "ok"

        with pytest.raises(ValueError):
            await event.sub_phase("f", flaky)
        assert "f" not in event._sub_phase_cache
        assert await event.sub_phase("f", flaky) == "ok"

    @pytest.mark.asyncio
    async def test_async_timeout(self, build):
        async def slow():
            await asyncio.sleep(5)

        with pytest.raises(asyncio.TimeoutError):
            await build().sub_phase("slow", slow, timeout=0.01)

    @pytest.mark.asyncio
    async def test_sync_function_with_timeout_runs_in_the_executor(self, build):
        import threading

        main = threading.current_thread()
        result = await build().sub_phase(
            "t", lambda: threading.current_thread() is main, timeout=5
        )
        assert result is False

    @pytest.mark.asyncio
    async def test_sync_function_exceeding_timeout(self, build):
        import time

        with pytest.raises(asyncio.TimeoutError):
            await build().sub_phase("slow", lambda: time.sleep(0.3), timeout=0.01)

    @pytest.mark.asyncio
    async def test_timeout_log_survives_a_user_timeouterror_without_timeout(
        self, build
    ):
        """A TimeoutError from user code with no timeout configured must not break logging."""

        def work():
            raise asyncio.TimeoutError()

        with pytest.raises(asyncio.TimeoutError):
            await build().sub_phase("u", work)


class TestEnqueueCheckpoint:
    @pytest.mark.asyncio
    async def test_enqueues_a_snapshot(self, build, checkpoint_manager):
        event = build(checkpoint_manager=checkpoint_manager)
        await event.enqueue_checkpoint()
        [(snapshot,), _] = checkpoint_manager.enqueue.call_args
        assert snapshot.task_id == "task-1"
        assert snapshot.phase == EventPhase.INITIALIZED

    @pytest.mark.asyncio
    async def test_snapshot_builds_for_a_default_event(self, build):
        """Regression: previous_result=EMPTY became None and broke serialisation."""
        snapshot = await build().create_snapshot()
        assert snapshot.init_args["previous_result"] == []
        assert snapshot.call_args == {"args": [], "kwargs": {}}

    @pytest.mark.asyncio
    async def test_snapshot_excludes_collaborators(
        self, build, checkpoint_manager, channel
    ):
        event = build(checkpoint_manager=checkpoint_manager, command_channel=channel)
        snapshot = await event.create_snapshot()
        assert "checkpoint_manager" not in snapshot.attribs
        assert "command_channel" not in snapshot.attribs
        assert "command_channel" not in snapshot.init_args

    @pytest.mark.asyncio
    async def test_snapshot_reflects_progress(self, build):
        event = build()
        event._retry_count = 2
        event.exec_status, event.exec_result = True, {"a": 1}
        await event._register_resource("c", Counter(3), PROVIDER)
        snapshot = await event.create_snapshot()
        assert snapshot.retry_count == 2 and snapshot.exec_status is True
        assert snapshot.external_resources["c"]["data"] == {"value": 3}

    @pytest.mark.asyncio
    async def test_snapshot_uses_policy_attempts(self, build):
        event = build({"retry_policy": RetryPolicy(max_attempts=7)})
        assert (await event.create_snapshot()).max_retry_attempts == 7

    @pytest.mark.asyncio
    async def test_missing_manager_is_ignored_by_default(self, build):
        await build().enqueue_checkpoint()  # no error

    @pytest.mark.asyncio
    async def test_missing_manager_raises_when_required(self, build, monkeypatch):
        monkeypatch.setattr(
            "volnux.mixins.event.checkpointer.conf",
            MagicMock(get=lambda key, default=None: key == "CHECKPOINT_REQUIRED"),
        )
        with pytest.raises(RuntimeError, match="required"):
            await build().enqueue_checkpoint()

    @pytest.mark.asyncio
    async def test_manager_errors_are_logged_by_default(
        self, build, checkpoint_manager
    ):
        checkpoint_manager.enqueue.side_effect = ConnectionError("down")
        await build(checkpoint_manager=checkpoint_manager).enqueue_checkpoint()

    @pytest.mark.asyncio
    async def test_manager_errors_raise_when_required(
        self, build, checkpoint_manager, monkeypatch
    ):
        checkpoint_manager.enqueue.side_effect = ConnectionError("down")
        monkeypatch.setattr(
            "volnux.mixins.event.checkpointer.conf",
            MagicMock(get=lambda key, default=None: key == "CHECKPOINT_REQUIRED"),
        )
        with pytest.raises(ConnectionError):
            await build(checkpoint_manager=checkpoint_manager).enqueue_checkpoint()


class TestPhaseStep:
    @pytest.mark.asyncio
    async def test_phase_advances_only_on_success_and_signals(self, build, recorded):
        event = build()
        await event._process()
        phases = [
            e["phase"] for e in recorded["event_phase_changed"] if e["event"] is event
        ]
        assert phases == [EventPhase.INITIALIZED, EventPhase.PROCESSING]

    @pytest.mark.asyncio
    async def test_failed_step_does_not_advance(self, build, recorded):
        async def bypass(self):
            raise RuntimeError("bad")

        event = build({"bypass": bypass})
        with pytest.raises(RuntimeError):
            await event._pre_process()
        assert event.get_phase() == EventPhase.INITIALIZED
        assert event._active_phase is None  # restored even on failure

    @pytest.mark.asyncio
    async def test_active_phase_is_visible_inside_the_step(self, build):
        seen = []

        async def process(self):
            seen.append(self._active_phase)
            return True, 1

        event = build({"process": process})
        await event._process()
        assert seen == [EventPhase.PROCESSING] and event._active_phase is None

    def test_steps_are_tagged_and_ordered(self, build):
        steps = build()._get_steps()
        phases = [s._phase for s in steps]
        assert phases == sorted(phases) and len(phases) == 6
        assert phases[0] == EventPhase.INITIALIZED
        assert phases[-1] == EventPhase.COMPLETED
        assert all(s._is_step for s in steps)

    def test_sync_step_emits_signal_synchronously(self, build, recorded):
        event = build()
        event.exec_status, event.exec_result = True, 1
        event._post_process()
        assert any(
            e["phase"] == EventPhase.POST_PROCESS
            for e in recorded["event_phase_changed"]
            if e["event"] is event
        )
