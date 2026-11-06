"""ExecutionContext and ExecutionCoordinator instrumentation."""

import asyncio
from types import SimpleNamespace

import pytest
from opentelemetry.trace import StatusCode

from volnux.execution.context import ExecutionContext
from volnux.execution.coordinator import ExecutionCoordinator, ExecutionTimeoutError
from volnux.execution.status import ExecutionStatus
from volnux.otel.context_manager import OTelContextManager
from volnux.otel.instrumentations import context_coordinator as cc
from volnux.otel.instrumentations._patching import is_patched, unpatch_all

pytestmark = pytest.mark.asyncio


def make_context_class():
    """A stand-in with ExecutionContext's method shapes, every one instrumented."""

    class FakeContext:
        def __init__(self, id="c1", previous=None):
            self.id = id
            self.previous_context = previous
            self.next_context = None
            self.pipeline = SimpleNamespace(id="p1")
            self.status = ExecutionStatus.PENDING
            self.results = []
            self.errors = []
            self.metrics = SimpleNamespace(duration=0.25)
            self.calls = []
            self.during_dispatch = None

        def is_multitask(self):
            return False

        def get_task_profiles(self):
            return [SimpleNamespace(get_event_name=lambda: "A")]

        def get_depth(self):
            return 1

        async def dispatch(self, timeout=None):
            self.during_dispatch = OTelContextManager.get_async_context()
            self.status = ExecutionStatus.COMPLETED
            self.results.append("r")
            return "dispatched"

        async def spawn_child(self, task_profiles):
            return SimpleNamespace(id="child", get_depth=lambda: 2)

        def update_status(self, new_status):
            self.status = new_status

        async def update_status_async(self, new_status):
            self.status = new_status

        def add_error(self, error):
            self.errors.append(error)

        def add_result(self, result):
            self.results.append(result)

        def cancel(self):
            self.status = ExecutionStatus.CANCELLED

        async def cancel_async(self, propagate=True):
            self.calls.append(("cancel_async", propagate))
            self.status = ExecutionStatus.CANCELLED

        def abort(self):
            self.status = ExecutionStatus.ABORTED

        async def abort_async(self):
            self.status = ExecutionStatus.ABORTED

        def failed(self):
            self.status = ExecutionStatus.FAILED

        async def failed_async(self):
            self.status = ExecutionStatus.FAILED

        async def save(self, force_insert=False, ttl=None):
            self.calls.append(("save", force_insert, ttl))

    for name, instrument in cc._CONTEXT_PATCHES.items():
        setattr(FakeContext, name, instrument(getattr(FakeContext, name)))
    return FakeContext


@pytest.fixture
def Ctx(otel):
    return make_context_class()


class TestDispatch:
    async def test_a_dispatch_span_with_the_contexts_attributes(self, otel, Ctx):
        ctx = Ctx()
        assert await ctx.dispatch(timeout=5) == "dispatched"
        span = otel.span("context.dispatch")
        assert span.attributes["context.id"] == "c1"
        assert span.attributes["context.timeout"] == 5
        assert (
            span.attributes["context.status"] == "completed"
            or span.attributes["context.status"] == ExecutionStatus.COMPLETED.value
        )
        assert span.attributes["context.results_count"] == 1
        assert span.attributes["context.duration_ms"] == pytest.approx(250)
        assert span.status.status_code is StatusCode.OK
        assert [e.name for e in span.events] == [
            "context.dispatch_started",
            "context.dispatch_completed",
        ]

    async def test_the_span_is_stored_on_the_context(self, otel, Ctx):
        ctx = Ctx()
        await ctx.dispatch()
        assert OTelContextManager.get_span_from_context(ctx) is not None

    async def test_the_async_context_is_the_dispatch_span_while_it_runs(
        self, otel, Ctx
    ):
        ctx = Ctx()
        await ctx.dispatch()
        assert ctx.during_dispatch is not None
        assert OTelContextManager.get_async_context() is None  # and cleared after

    async def test_the_next_context_hangs_under_the_previous(self, otel, Ctx):
        first = Ctx("first")
        await first.dispatch()
        second = Ctx("second", previous=first)
        await second.dispatch()
        first_span, second_span = (s for s in otel.spans_named("context.dispatch"))
        assert second_span.parent.span_id == first_span.context.span_id

    async def test_nested_dispatches_unwind_the_async_context(self, otel, Ctx):
        inner_class = make_context_class()
        seen = {}

        async def outer_dispatch(self, timeout=None):
            seen["outer"] = OTelContextManager.get_async_context()
            await inner_class("inner").dispatch()
            # (clearing the inner one used to leave None here)
            seen["after_inner"] = OTelContextManager.get_async_context()

        outer = Ctx("outer")
        type(outer).dispatch = cc.instrument_execution_context_dispatch(outer_dispatch)
        await outer.dispatch()
        assert seen["outer"] is not None
        assert seen["after_inner"] is seen["outer"]
        assert OTelContextManager.get_async_context() is None

    async def test_failed_status_is_an_error_span(self, otel, Ctx):
        ctx = Ctx()

        async def failing(self, timeout=None):
            self.status = ExecutionStatus.FAILED
            self.errors.append(ValueError("bad"))

        type(ctx).dispatch = cc.instrument_execution_context_dispatch(failing)
        await ctx.dispatch()
        span = otel.span("context.dispatch")
        assert span.status.status_code is StatusCode.ERROR
        assert any(e.name == "exception" for e in span.events)

    async def test_an_exception_is_recorded_and_the_context_cleared(self, otel, Ctx):
        ctx = Ctx()

        async def broken(self, timeout=None):
            raise RuntimeError("boom")

        type(ctx).dispatch = cc.instrument_execution_context_dispatch(broken)
        with pytest.raises(RuntimeError):
            await ctx.dispatch()
        assert otel.span("context.dispatch").status.status_code is StatusCode.ERROR
        assert OTelContextManager.get_async_context() is None

    async def test_cancellation_is_marked(self, otel, Ctx):
        ctx = Ctx()
        started = asyncio.Event()

        async def slow(self, timeout=None):
            started.set()
            await asyncio.sleep(10)

        type(ctx).dispatch = cc.instrument_execution_context_dispatch(slow)
        task = asyncio.create_task(ctx.dispatch())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert otel.span("context.dispatch").status.status_code is StatusCode.ERROR
        assert OTelContextManager.get_async_context() is None

    async def test_without_a_tracer(self, no_otel):
        ctx = make_context_class()()
        assert await ctx.dispatch() == "dispatched"


class TestOtherMethods:
    async def test_spawn_child(self, otel, Ctx):
        child = await Ctx().spawn_child(["a", "b"])
        span = otel.span("context.spawn_child")
        assert span.attributes["child.task_count"] == 2
        assert span.attributes["child.context_id"] == "child"
        assert span.attributes["child.depth"] == 2
        assert child.id == "child"

    @pytest.mark.parametrize(
        "method, name, status",
        [
            ("cancel", "context.cancel", ExecutionStatus.CANCELLED),
            ("abort", "context.abort", ExecutionStatus.ABORTED),
            ("failed", "context.failed", ExecutionStatus.FAILED),
        ],
    )
    async def test_status_changing_methods(self, otel, Ctx, method, name, status):
        ctx = Ctx()
        getattr(ctx, method)()
        span = otel.span(name)
        assert span.attributes["context.current_status"] == status.value
        assert ctx.status is status

    @pytest.mark.parametrize(
        "method, name, status",
        [
            ("abort_async", "context.abort_async", ExecutionStatus.ABORTED),
            ("failed_async", "context.failed_async", ExecutionStatus.FAILED),
        ],
    )
    async def test_async_status_changing_methods(self, otel, Ctx, method, name, status):
        ctx = Ctx()
        await getattr(ctx, method)()
        assert otel.span(name).attributes["context.current_status"] == status.value

    async def test_update_status(self, otel, Ctx):
        ctx = Ctx()
        ctx.update_status(ExecutionStatus.COMPLETED)
        await ctx.update_status_async(ExecutionStatus.FAILED)
        assert otel.span("context.update_status") is not None
        assert (
            otel.span("context.update_status_async").attributes[
                "context.current_status"
            ]
            == ExecutionStatus.FAILED.value
        )

    async def test_cancel_async_forwards_propagate(self, otel, Ctx):
        # (the wrapper took only self: the coordinator's cancel_async(propagate=False)
        # raised TypeError, and propagate was never passed on)
        ctx = Ctx()
        await ctx.cancel_async(propagate=False)
        await ctx.cancel_async()
        await ctx.cancel_async(False)
        assert ctx.calls == [
            ("cancel_async", False),
            ("cancel_async", True),
            ("cancel_async", False),
        ]
        assert len(otel.spans_named("context.cancel_async")) == 3

    async def test_save_forwards_its_arguments(self, otel, Ctx):
        ctx = Ctx()
        await ctx.save(force_insert=True, ttl=60)
        assert ctx.calls == [("save", True, 60)]
        assert otel.span("context.save") is not None

    async def test_add_error_records_the_error_on_the_span(self, otel, Ctx):
        ctx = Ctx()
        ctx.add_error(ValueError("bad"))
        ctx.add_error("only text")
        spans = otel.spans_named("context.add_error")
        assert [s.events[0].attributes["exception.message"] for s in spans] == [
            "bad",
            "only text",
        ]
        assert all(s.status.status_code is StatusCode.ERROR for s in spans)
        assert len(ctx.errors) == 2

    async def test_add_result(self, otel, Ctx):
        ctx = Ctx()
        ctx.add_result({"x": 1})
        span = otel.span("context.add_result")
        assert span.attributes["context.result_type"] == "dict"
        assert span.attributes["context.results_count"] == 1

    async def test_an_exception_in_a_method_is_raised_unchanged(self, otel, Ctx):
        ctx = Ctx()

        def broken(self, result):
            raise KeyError("k")

        type(ctx).add_result = cc.instrument_execution_context_add_result(broken)
        with pytest.raises(KeyError):
            ctx.add_result(1)
        assert otel.span("context.add_result").status.status_code is StatusCode.ERROR

    async def test_without_a_tracer_every_method_is_a_pass_through(self, no_otel):
        ctx = make_context_class()()
        ctx.update_status(ExecutionStatus.RUNNING)
        ctx.add_error("e")
        ctx.add_result(1)
        ctx.cancel()
        await ctx.cancel_async(propagate=False)
        await ctx.save()
        assert ctx.calls[0] == ("cancel_async", False)


class TestCoordinator:
    class FakeCoordinator:
        _timeout = 3

        def __init__(self, outcome=None):
            self.outcome = outcome

        async def _execute_async(self):
            if isinstance(self.outcome, BaseException):
                raise self.outcome
            return "executed"

    @pytest.fixture
    def Coordinator(self, otel):
        cls = type("FakeCoordinator", (self.FakeCoordinator,), {})
        cls._execute_async = cc.instrument_coordinator_execute_async(cls._execute_async)
        return cls

    async def test_a_span_with_the_timeout(self, otel, Coordinator):
        assert await Coordinator()._execute_async() == "executed"
        span = otel.span("coordinator.execute_async")
        assert span.attributes["coordinator.timeout"] == 3
        assert span.status.status_code is StatusCode.OK
        assert [e.name for e in span.events] == [
            "coordinator_started_async",
            "coordinator_completed_async",
        ]

    @pytest.mark.parametrize(
        "error", [asyncio.TimeoutError(), ExecutionTimeoutError("t")]
    )
    async def test_a_timeout_is_labelled(self, otel, Coordinator, error):
        with pytest.raises(type(error)):
            await Coordinator(error)._execute_async()
        span = otel.span("coordinator.execute_async")
        assert span.status.description == "Execution timeout"

    async def test_another_error(self, otel, Coordinator):
        with pytest.raises(RuntimeError):
            await Coordinator(RuntimeError("x"))._execute_async()
        assert (
            otel.span("coordinator.execute_async").status.status_code
            is StatusCode.ERROR
        )

    async def test_it_parents_on_the_async_context_of_the_dispatch(
        self, otel, Ctx, Coordinator
    ):
        ctx = Ctx()

        async def dispatch_with_coordinator(self, timeout=None):
            await Coordinator()._execute_async()

        type(ctx).dispatch = cc.instrument_execution_context_dispatch(
            dispatch_with_coordinator
        )
        await ctx.dispatch()
        coordinator = otel.span("coordinator.execute_async")
        assert (
            coordinator.parent.span_id == otel.span("context.dispatch").context.span_id
        )

    async def test_without_a_tracer(self, no_otel):
        cls = type("C", (self.FakeCoordinator,), {})
        cls._execute_async = cc.instrument_coordinator_execute_async(cls._execute_async)
        assert await cls()._execute_async() == "executed"


class TestPatching:
    async def test_patch_execution_context_patches_each_method_once(self, otel):
        count = cc.patch_execution_context()
        assert count == len(cc._CONTEXT_PATCHES) == 13
        assert cc.patch_execution_context() == 0
        for name in cc._CONTEXT_PATCHES:
            assert is_patched(ExecutionContext, name)

    async def test_the_patched_methods_keep_their_kind(self, otel):
        import inspect

        before = {
            n: inspect.iscoroutinefunction(getattr(ExecutionContext, n))
            for n in cc._CONTEXT_PATCHES
        }
        cc.patch_execution_context()
        after = {
            n: inspect.iscoroutinefunction(getattr(ExecutionContext, n))
            for n in cc._CONTEXT_PATCHES
        }
        assert before == after

    async def test_cancel_async_accepts_propagate_on_the_real_class(self, otel):
        import inspect

        cc.patch_execution_context()
        # the wrapper forwards any arguments; calling it with propagate must bind
        wrapper = ExecutionContext.cancel_async
        assert wrapper.__wrapped__ is not None
        inspect.signature(wrapper).bind(object(), propagate=False)

    async def test_patch_execution_coordinator(self, otel):
        assert cc.patch_execution_coordinator() is True
        assert cc.patch_execution_coordinator() is False
        assert is_patched(ExecutionCoordinator, "_execute_async")

    async def test_patch_all_then_unpatch(self, otel):
        originals = {n: ExecutionContext.__dict__[n] for n in cc._CONTEXT_PATCHES}
        cc.patch_all_execution_components()
        unpatch_all()
        assert {
            n: ExecutionContext.__dict__[n] for n in cc._CONTEXT_PATCHES
        } == originals
