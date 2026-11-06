"""EventBase: construction, hooks, goto/stop semantics and the full __call__ lifecycle."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from volnux import EventBase
from volnux.constants import EMPTY
from volnux.event.base import EventCategory, EventType
from volnux.exceptions import (
    ImproperlyConfigured,
    MaxRetryError,
    StopProcessingError,
    SwitchTask,
)
from volnux.execution.rehydrator.event.snapshot import EventPhase
from volnux.mixins.event.retry import RetryPolicy
from volnux.parser.options import StopCondition
from volnux.result import EventResult
from volnux.versioning.semantic import SemanticVersioning
from volnux.result_evaluators import (
    EventEvaluator,
    ExecutionResultEvaluationStrategyBase,
    ResultEvaluationStrategies,
)

from .conftest import make_event_class


class TestConstruction:
    def test_defaults(self, build):
        event = build()
        assert event.previous_result is EMPTY
        assert event.options is None
        assert event.stop_condition.stop_condition == StopCondition.NEVER
        assert event.run_bypass_event_checks is False

    def test_class_defaults(self):
        event_class = make_event_class()
        assert event_class.version == "1.0.0"
        # no namespace of its own: the DEFAULT_EVENT_NAMESPACE setting applies
        assert event_class.namespace is None
        assert event_class.get_version_handler().namespace == "local"
        assert event_class.event_type is EventType.OTHER
        assert event_class.categories == frozenset({EventCategory.OTHER})
        assert event_class.executor == "default"
        assert event_class.deprecated is False
        assert event_class.result_evaluation_strategy is (
            ResultEvaluationStrategies.ALL_MUST_SUCCEED
        )

    def test_overriding_init_is_forbidden(self):
        with pytest.raises(ImproperlyConfigured, match="__init__"):

            class Bad(EventBase):
                def __init__(self):
                    pass

                async def process(self):
                    return True, 1

    def test_process_is_required(self, ctx):
        class NoProcess(EventBase):
            pass

        with pytest.raises(TypeError):
            NoProcess(ctx, "t")

    def test_required_arguments(self, ctx):
        with pytest.raises(TypeError):
            make_event_class()(ctx)  # task_id missing

    def test_repr_shows_executor_label_or_class(self, build):
        assert repr(build()).endswith("executor=default>")
        from volnux.executors import ThreadPoolExecutor

        event = build(make_event_class(executor=ThreadPoolExecutor))
        assert "executor=ThreadPoolExecutor" in repr(event)

    def test_init_args_round_trip(self, build, ctx):
        event = build(
            sequence_number=3, previous_result=[1], run_bypass_event_checks=True
        )
        args = event.get_init_args()
        assert args["task_id"] == "task-1" and args["sequence_number"] == 3
        assert args["run_bypass_event_checks"] is True
        assert args["execution_context"] is ctx and args["previous_result"] == [1]

    def test_extra_init_params_come_from_the_schema(self, build):
        event_class = make_event_class(INIT_PARAMS_SCHEMA={"limit": {"default": 10}})
        assert build(event_class).limit == 10


class TestVersioning:
    def test_version_info(self):
        event_class = make_event_class(version="2.3.4", changelog="notes")
        info = event_class.get_version_info()
        assert info["version"] == "2.3.4" and info["changelog"] == "notes"
        assert info["namespace"] == "local" and info["deprecated"] is False

    def test_registry_backed_queries(self):
        event_class = make_event_class(version="1.2.0")
        assert event_class.get_all_versions() == ["1.2.0"]
        assert event_class.get_latest_version() == "1.2.0"
        assert event_class.is_deprecated() is False

    def test_the_version_attribute_is_the_version(self):
        assert (
            make_event_class(version="9.9.9").get_version_info()["version"] == "9.9.9"
        )

    def test_the_default_version_is_1_0_0(self):
        assert make_event_class().get_version_info()["version"] == "1.0.0"

    @pytest.mark.parametrize("version", ["not-a-version", "1", "1.0", "v2", "1.0.0.0"])
    def test_a_version_that_is_not_semantic_is_rejected_at_class_creation(
        self, version
    ):
        with pytest.raises(ValueError, match="not a semantic version"):
            make_event_class(version=version)

    def test_versioning_class_was_removed(self):
        from volnux.exceptions import ImproperlyConfigured as Improper
        from volnux.event.base import ImproperlyConfigured

        with pytest.raises((Improper, ImproperlyConfigured), match="versioning_class"):
            make_event_class(versioning_class=SemanticVersioning)

    def test_deprecation_flag(self):
        assert make_event_class(deprecated=True).is_deprecated() is True

    def test_subclass_gets_its_own_version_handler(self):
        parent = make_event_class(version="1.0.0")
        child = type("Child%d" % id(parent), (parent,), {"version": "2.0.0"})
        assert parent.get_version_info()["version"] == "1.0.0"
        assert child.get_version_info()["version"] == "2.0.0"
        assert child.get_version_handler() is not parent.get_version_handler()

    def test_handler_is_cached_per_class(self):
        event_class = make_event_class()
        assert event_class.get_version_handler() is event_class.get_version_handler()

    def test_all_classes_include_new_events(self):
        event_class = make_event_class()
        assert event_class in EventBase.get_all_event_classes()


class TestGoto:
    def test_switches_with_a_success_result(self, build):
        event = build(sequence_number=2)
        with pytest.raises(SwitchTask) as info:
            event.goto(3, True, {"v": 1})
        exc = info.value
        assert exc.current_task_id == "task-1" and exc.next_task_descriptor == 3
        assert exc.reason == "manual"
        assert isinstance(exc.result, EventResult) and exc.result.success
        assert exc.result.content == {"v": 1} and exc.result.order == 2

    def test_switches_with_a_failure_result(self, build):
        with pytest.raises(SwitchTask) as info:
            build().goto(0, False, "why")
        assert not info.value.result.success

    def test_raw_result_skips_the_hooks(self, build):
        event = build()
        event.on_success = AsyncMock(side_effect=AssertionError("hook ran"))
        with pytest.raises(SwitchTask) as info:
            event.goto(1, True, "raw", execute_on_event_method=False)
        assert info.value.result.content == "raw"
        assert info.value.result.event_name == type(event).__name__

    def test_runs_on_success_hook_by_default(self, build, recorded):
        event = build()
        with pytest.raises(SwitchTask):
            event.goto(1, True, "r")
        assert [e["hook_type"] for e in recorded["event_called"]] == ["on_success"]

    @pytest.mark.parametrize("bad", [-1, 10, 99, "1", 1.5, None])
    def test_descriptor_must_be_an_int_between_0_and_9(self, build, bad):
        with pytest.raises(ValueError, match="between 0 to 9"):
            build().goto(bad, True, "r")

    @pytest.mark.parametrize("descriptor", [0, 1, 9])
    def test_boundary_descriptors(self, build, descriptor):
        with pytest.raises(SwitchTask) as info:
            build().goto(descriptor, True, "r")
        assert info.value.next_task_descriptor == descriptor

    def test_stop_condition_can_override_a_goto(self, build):
        event = build(stop_condition=StopCondition.ON_SUCCESS)
        with pytest.raises(StopProcessingError):
            event.goto(1, True, "r")


class TestOnSuccess:
    def test_returns_a_success_result(self, build):
        result = build(sequence_number=5).on_success({"k": 1})
        assert isinstance(result, EventResult) and result.success
        assert result.content == {"k": 1} and result.order == 5

    def test_emits_event_called(self, build, recorded):
        event = build()
        event._call_args = {"a": 1}
        event.on_success("done")
        [emission] = recorded["event_called"]
        assert emission["hook_type"] == "on_success" and emission["result"] == "done"
        assert emission["event"] is event and emission["call_kwargs"] == {"a": 1}

    @pytest.mark.parametrize(
        "value, message", [("text", "text"), (None, None), (5, "5")]
    )
    def test_message_is_the_stringified_result(self, build, value, message):
        event = build()
        event.on_success(value)
        assert event.stop_condition.message == message

    def test_on_success_stop_condition_raises_with_context(self, build):
        event = build(stop_condition=StopCondition.ON_SUCCESS, sequence_number=1)
        event._call_args = {"x": 1}
        with pytest.raises(StopProcessingError) as info:
            event.on_success("fin")
        exc = info.value
        assert exc.stop_condition is event.stop_condition
        assert exc.params["task_id"] == "task-1"
        assert exc.params["event_name"] == type(event).__name__
        assert exc.params["sequence_number"] == 1 and exc.params["call_args"] == {
            "x": 1
        }

    @pytest.mark.parametrize("condition", [StopCondition.NEVER, StopCondition.ON_ERROR])
    def test_other_conditions_do_not_stop_on_success(self, build, condition):
        assert build(stop_condition=condition).on_success("ok").success


class TestOnFailure:
    def test_returns_a_failed_result(self, build):
        error = ValueError("x")
        result = build().on_failure(error)
        assert not result.success and result.content is error

    def test_emits_event_called(self, build, recorded):
        build().on_failure("bad")
        assert [e["hook_type"] for e in recorded["event_called"]] == ["on_failure"]

    def test_max_retry_error_is_unwrapped(self, build):
        inner = KeyError("root cause")
        result = build().on_failure(MaxRetryError(attempt=3, exception=inner))
        assert result.content is inner

    def test_subclasses_of_max_retry_error_are_not_unwrapped(self, build):
        class Special(MaxRetryError):
            pass

        error = Special(attempt=1, exception=ValueError())
        assert build().on_failure(error).content is error

    def test_non_exceptions_pass_through(self, build):
        assert build().on_failure({"reason": "x"}).content == {"reason": "x"}

    def test_on_error_stop_condition_raises(self, build):
        event = build(stop_condition=StopCondition.ON_ERROR)
        error = RuntimeError("fatal")
        with pytest.raises(StopProcessingError) as info:
            event.on_failure(error)
        assert info.value.exception is error
        assert info.value.params["task_id"] == "task-1"
        assert "Error occurred" in event.stop_condition.message

    def test_on_any_stops_on_failure_too(self, build):
        with pytest.raises(StopProcessingError):
            build(stop_condition=StopCondition.ON_ANY).on_failure("x")

    def test_never_does_not_stop(self, build):
        assert not build(stop_condition=StopCondition.NEVER).on_failure("x").success

    def test_max_retry_error_is_unwrapped_before_stop_handling(self, build):
        inner = ValueError("v")
        event = build(stop_condition=StopCondition.ON_ERROR)
        with pytest.raises(StopProcessingError) as info:
            event.on_failure(MaxRetryError(attempt=2, exception=inner))
        assert info.value.exception is inner


class TestEventResult:
    def test_event_result_fields(self, build):
        result = build(task_id="t7", sequence_number=9).event_result(True, {"a": 1})
        assert result.error is True and result.content == {"a": 1}
        assert result.task_id == "t7" and result.order == 9


class TestEvaluator:
    def test_returns_an_evaluator_for_the_configured_strategy(self):
        evaluator = make_event_class(
            result_evaluation_strategy=ResultEvaluationStrategies.ANY_MUST_SUCCEED
        ).evaluator()
        assert isinstance(evaluator, EventEvaluator)
        assert evaluator.strategy is ResultEvaluationStrategies.ANY_MUST_SUCCEED

    def test_missing_strategy(self):
        with pytest.raises(ImproperlyConfigured, match="No result evaluation"):
            make_event_class(result_evaluation_strategy=None).evaluator()

    def test_invalid_strategy(self):
        with pytest.raises(ImproperlyConfigured, match="not a valid"):
            make_event_class(result_evaluation_strategy="all").evaluator()

    def test_evaluates_results(self):
        results = [
            EventResult(
                error=e, event_name="E", content=1, workflow_id=None, task_id="t"
            )
            for e in (False, True, False)
        ]
        any_ok = (
            make_event_class(
                result_evaluation_strategy=ResultEvaluationStrategies.ANY_MUST_SUCCEED
            )
            .evaluator()
            .evaluate(results)
        )
        all_ok = make_event_class().evaluator().evaluate(results)
        assert any_ok.success and not all_ok.success
        assert (all_ok.successful_tasks, all_ok.failed_tasks) == (2, 1)


class TestBypassAndCleanupDefaults:
    @pytest.mark.asyncio
    async def test_bypass_is_not_supported_by_default(self, build):
        with pytest.raises(NotImplementedError, match="does not support bypass"):
            await build().bypass()

    @pytest.mark.asyncio
    async def test_cleanup_default_is_a_noop(self, build):
        assert await build().cleanup(1, a=2) is None


class TestCall:
    @pytest.mark.asyncio
    async def test_successful_run(self, build, recorded):
        event = build({"process": _double})
        result = await event(21)
        assert isinstance(result, EventResult) and result.success
        assert result.content == 42 and result.task_id == "task-1"
        assert event.get_phase() == EventPhase.COMPLETED
        phases = [
            e["phase"] for e in recorded["event_phase_changed"] if e["event"] is event
        ]
        assert phases == list(EventPhase)
        assert [e["hook_type"] for e in recorded["event_called"]] == ["on_success"]

    @pytest.mark.asyncio
    async def test_failure_is_returned_not_raised(self, build):
        result = await build({"process": _fails})()
        assert not result.success and isinstance(result.content, ValueError)

    @pytest.mark.asyncio
    async def test_false_status_is_a_failed_result(self, build):
        result = await build({"process": _returns_false})()
        assert not result.success and result.content == "declined"

    @pytest.mark.asyncio
    async def test_arguments_reach_process(self, build):
        seen = {}

        async def process(self, a, b=0, **rest):
            seen.update(a=a, b=b, rest=rest)
            return True, a + b

        await build({"process": process})(1, b=2, extra=3)
        assert seen == {"a": 1, "b": 2, "rest": {"extra": 3}}

    @pytest.mark.asyncio
    async def test_records_the_worker_task(self, build):
        event = build()
        await event()
        assert event._main_worker_task is not None

    @pytest.mark.asyncio
    async def test_cleanup_hook_runs_on_success(self, build):
        cleaned = []

        async def cleanup(self, *a, **k):
            cleaned.append((a, k))

        await build({"cleanup": cleanup})(1, x=2)
        assert cleaned == [((1,), {"x": 2})]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", ["goto", "stop"])
    async def test_cleanup_runs_even_when_control_flow_leaves_the_event(
        self, build, kind
    ):
        cleaned = []

        async def cleanup(self, *a, **k):
            cleaned.append(1)

        async def process(self):
            if kind == "goto":
                self.goto(1, True, "r")
            raise StopProcessingError(message="stop", exception=None)

        event = build({"process": process, "cleanup": cleanup})
        with pytest.raises((SwitchTask, StopProcessingError)):
            await event()
        assert cleaned == [1] and event.get_phase() == EventPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_cleanup_failure_does_not_mask_the_result(self, build):
        async def cleanup(self, *a, **k):
            raise RuntimeError("cleanup broke")

        result = await build({"cleanup": cleanup})()
        assert result.success

    @pytest.mark.asyncio
    async def test_command_listener_is_started_and_cancelled(self, build, channel):
        seen = {}

        async def process(self):
            seen["task"] = self._command_listener_task
            seen["running"] = not self._command_listener_task.done()
            return True, 1

        event = build({"process": process}, command_channel=channel)
        await event()
        assert seen["running"] is True
        assert seen["task"].cancelled() or seen["task"].done()
        assert channel.receive_command.await_count >= 1

    @pytest.mark.asyncio
    async def test_no_listener_without_a_channel(self, build):
        event = build()
        await event()
        assert event._command_listener_task is None

    @pytest.mark.asyncio
    async def test_listener_is_cancelled_when_the_event_fails(self, build, channel):
        async def bypass(self):
            raise KeyError("boom")

        event = build({"bypass": bypass}, command_channel=channel)
        with pytest.raises(KeyError):
            await event()
        assert event._command_listener_task.done()

    @pytest.mark.asyncio
    async def test_resource_monitor_is_stopped_at_the_end(self, build):
        from ..event import providers  # noqa: F401  (ensures importable provider)

        event = build()

        async def process(self):
            await self.acquire_resource(
                "c", "tests.unittests.event.providers.CounterProvider", {"value": 1}
            )
            return True, 1

        event = build({"process": process})
        await event()
        assert event._resource_monitor.is_started() is False

    @pytest.mark.asyncio
    async def test_retry_policy_is_applied_through_the_full_stack(
        self, build, monkeypatch
    ):
        async def nosleep(*a, **k):
            return None

        monkeypatch.setattr("volnux.mixins.event.retry.asyncio.sleep", nosleep)
        calls = []

        async def process(self):
            calls.append(1)
            if len(calls) < 3:
                raise ConnectionError("flaky")
            return True, "recovered"

        event = build({"process": process, "retry_policy": RetryPolicy(max_attempts=5)})
        result = await event()
        assert result.success and result.content == "recovered" and len(calls) == 3

    @pytest.mark.asyncio
    async def test_exhausted_retries_become_a_failed_result_with_the_root_cause(
        self, build, monkeypatch
    ):
        async def nosleep(*a, **k):
            return None

        monkeypatch.setattr("volnux.mixins.event.retry.asyncio.sleep", nosleep)

        async def process(self):
            raise ConnectionError("still down")

        event = build({"process": process, "retry_policy": RetryPolicy(max_attempts=2)})
        result = await event()
        assert not result.success
        assert isinstance(result.content, ConnectionError)

    @pytest.mark.asyncio
    async def test_a_second_call_reuses_the_finished_state(self, build):
        """An event instance runs once: later calls skip every phase and return the first result."""
        calls = []

        async def process(self):
            calls.append(1)
            return True, len(calls)

        event = build({"process": process})
        first = await event()
        second = await event()
        assert calls == [1] and second is first

    @pytest.mark.asyncio
    async def test_concurrent_events_do_not_share_state(self, build):
        async def process(self, n):
            await asyncio.sleep(0.01)
            return True, n

        events = [build({"process": process}, task_id="t%d" % i) for i in range(5)]
        results = await asyncio.gather(*(e(i) for i, e in enumerate(events)))
        assert [r.content for r in results] == list(range(5))
        assert [r.task_id for r in results] == ["t%d" % i for i in range(5)]


async def _double(self, x):
    return True, x * 2


async def _fails(self):
    raise ValueError("bad")


async def _returns_false(self):
    return False, "declined"
