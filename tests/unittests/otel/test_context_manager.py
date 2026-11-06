"""OTelContextManager (span/context bookkeeping) and SpanHelper (attributes)."""

import asyncio
from types import SimpleNamespace

import pytest
from opentelemetry import trace
from opentelemetry.trace import StatusCode

from volnux.execution.status import ExecutionStatus
from volnux.otel.context_manager import (
    OTelContextManager,
    SpanHelper,
    _otel_context_stack,
    _otel_context_var,
)


def fake_context(**overrides):
    base = dict(
        id="ctx-1",
        pipeline=SimpleNamespace(id="pipe-1"),
        previous_context=None,
        status=ExecutionStatus.COMPLETED,
        errors=[],
    )
    base.update(overrides)
    ctx = SimpleNamespace(**base)
    ctx.is_multitask = lambda: False
    ctx.get_task_profiles = lambda: [
        SimpleNamespace(get_event_name=lambda: "A"),
        SimpleNamespace(get_event_name=lambda: "B"),
    ]
    ctx.get_depth = lambda: 2
    return ctx


@pytest.fixture(autouse=True)
def clean_async_context():
    _otel_context_var.set(None)
    _otel_context_stack.set(())
    yield
    _otel_context_var.set(None)
    _otel_context_stack.set(())


class TestAttachAndGet:
    def test_the_span_and_context_are_stored_on_the_execution_context(self, otel):
        ctx = fake_context()
        with otel.tracer.start_as_current_span("s") as span:
            OTelContextManager.attach_span_to_context(ctx, span)
            assert OTelContextManager.get_span_from_context(ctx) is span
            stored = OTelContextManager.get_otel_context_from_execution_context(ctx)
            assert trace.get_current_span(stored) is span

    def test_an_explicit_context_is_stored_as_given(self, otel):
        ctx = fake_context()
        explicit = trace.set_span_in_context(otel.tracer.start_span("x"))
        with otel.tracer.start_as_current_span("s") as span:
            OTelContextManager.attach_span_to_context(ctx, span, explicit)
        assert (
            OTelContextManager.get_otel_context_from_execution_context(ctx) is explicit
        )

    def test_nothing_attached(self):
        ctx = fake_context()
        assert OTelContextManager.get_span_from_context(ctx) is None
        assert OTelContextManager.get_otel_context_from_execution_context(ctx) is None

    def test_an_object_that_cannot_take_attributes_does_not_fail(self, otel):
        class Slotted:
            __slots__ = ()

        with otel.tracer.start_as_current_span("s") as span:
            OTelContextManager.attach_span_to_context(Slotted(), span)  # no raise


class TestParentContext:
    def test_it_is_the_previous_contexts(self, otel):
        previous = fake_context(id="prev")
        with otel.tracer.start_as_current_span("p") as span:
            OTelContextManager.attach_span_to_context(previous, span)
        current = fake_context(previous_context=previous)
        parent = OTelContextManager.get_parent_context(current)
        assert trace.get_current_span(parent).get_span_context().span_id == (
            span.get_span_context().span_id
        )

    def test_without_a_previous_context(self):
        assert OTelContextManager.get_parent_context(fake_context()) is None

    def test_propagate_context_to_next_is_what_the_next_one_falls_back_to(self, otel):
        current, nxt = fake_context(), fake_context(id="next")
        with otel.tracer.start_as_current_span("c") as span:
            OTelContextManager.attach_span_to_context(current, span)
        OTelContextManager.propagate_context_to_next(current, nxt)
        # (no previous_context: the propagated one is used)
        parent = OTelContextManager.get_parent_context(nxt)
        assert trace.get_current_span(parent).get_span_context().span_id == (
            span.get_span_context().span_id
        )

    def test_propagate_with_nothing_to_propagate(self):
        nxt = fake_context()
        OTelContextManager.propagate_context_to_next(fake_context(), nxt)
        assert OTelContextManager.get_parent_context(nxt) is None


class TestSerialization:
    def test_a_round_trip_keeps_the_trace(self, otel):
        ctx = fake_context()
        with otel.tracer.start_as_current_span("s") as span:
            OTelContextManager.attach_span_to_context(ctx, span)
        carrier = OTelContextManager.serialize_context(ctx)
        assert "traceparent" in carrier
        restored = OTelContextManager.deserialize_context(carrier)
        got = trace.get_current_span(restored).get_span_context()
        assert got.trace_id == span.get_span_context().trace_id
        assert got.span_id == span.get_span_context().span_id
        assert got.is_remote

    def test_nothing_to_serialize(self):
        assert OTelContextManager.serialize_context(fake_context()) == {}

    def test_an_empty_carrier(self):
        assert OTelContextManager.deserialize_context({}) is None


class TestAsyncContext:
    def test_set_get_clear(self, otel):
        context = trace.set_span_in_context(otel.tracer.start_span("a"))
        assert OTelContextManager.get_async_context() is None
        OTelContextManager.set_async_context(context)
        assert OTelContextManager.get_async_context() is context
        OTelContextManager.clear_async_context()
        assert OTelContextManager.get_async_context() is None

    def test_nested_contexts_unwind_to_their_parent(self, otel):
        outer = trace.set_span_in_context(otel.tracer.start_span("outer"))
        inner = trace.set_span_in_context(otel.tracer.start_span("inner"))
        OTelContextManager.set_async_context(outer)
        OTelContextManager.set_async_context(inner)
        assert OTelContextManager.get_async_context() is inner
        OTelContextManager.clear_async_context()
        # (clearing used to set None, losing the outer context)
        assert OTelContextManager.get_async_context() is outer
        OTelContextManager.clear_async_context()
        assert OTelContextManager.get_async_context() is None

    def test_clearing_with_nothing_set(self):
        OTelContextManager.clear_async_context()
        assert OTelContextManager.get_async_context() is None

    @pytest.mark.asyncio
    async def test_each_task_has_its_own(self, otel):
        seen = {}

        async def work(name):
            context = trace.set_span_in_context(otel.tracer.start_span(name))
            OTelContextManager.set_async_context(context)
            await asyncio.sleep(0.01)
            seen[name] = OTelContextManager.get_async_context() is context
            OTelContextManager.clear_async_context()

        await asyncio.gather(work("a"), work("b"))
        assert seen == {"a": True, "b": True}
        assert OTelContextManager.get_async_context() is None


class TestStatusFromExecution:
    def run(self, otel, status, errors=()):
        ctx = fake_context(status=status, errors=list(errors))
        with otel.tracer.start_as_current_span("s") as span:
            SpanHelper.set_status_from_execution(span, ctx)
        return otel.span("s")

    def test_completed(self, otel):
        assert (
            self.run(otel, ExecutionStatus.COMPLETED).status.status_code
            is StatusCode.OK
        )

    @pytest.mark.parametrize(
        "status, text",
        [
            (ExecutionStatus.FAILED, "Execution failed"),
            (ExecutionStatus.CANCELLED, "Execution cancelled"),
            (ExecutionStatus.ABORTED, "Execution aborted"),
        ],
    )
    def test_the_failed_states(self, otel, status, text):
        done = self.run(otel, status)
        assert done.status.status_code is StatusCode.ERROR
        assert done.status.description == text

    def test_errors_of_a_failed_context_are_recorded(self, otel):
        done = self.run(otel, ExecutionStatus.FAILED, [ValueError("one"), "two"])
        messages = [e.attributes["exception.message"] for e in done.events]
        assert messages == ["one", "two"]  # a stored error may be text

    def test_other_states_leave_the_status_alone(self, otel):
        done = self.run(otel, ExecutionStatus.PENDING)
        assert done.status.status_code is StatusCode.UNSET


class TestAttributes:
    def test_task_attributes(self):
        task = SimpleNamespace(
            get_id=lambda: 7,
            get_event_name=lambda: "Ev",
            is_conditional=True,
            is_parallel_execution_node=True,
            descriptor=3,
            is_sink=True,
        )
        assert SpanHelper.task_attributes(task) == {
            "task.id": "7",
            "task.name": "Ev",
            "task.is_conditional": True,
            "task.is_parallel": True,
            "task.descriptor": "3",
            "task.is_sink": True,
        }

    def test_a_task_that_lacks_things(self):
        # (is_conditional is not on every kind of task)
        task = SimpleNamespace(get_id=lambda: 1, get_event_name=lambda: "G")
        attrs = SpanHelper.task_attributes(task)
        assert attrs["task.id"] == "1" and attrs["task.name"] == "G"
        assert (
            "task.is_conditional" not in attrs or attrs["task.is_conditional"] is None
        )
        assert "task.descriptor" not in attrs and "task.is_sink" not in attrs

    def test_add_task_attributes_sets_them_on_the_span(self, otel):
        task = SimpleNamespace(get_id=lambda: 1, get_event_name=lambda: "E")
        with otel.tracer.start_as_current_span("s") as span:
            SpanHelper.add_task_attributes(span, task)
        assert otel.span("s").attributes["task.name"] == "E"

    def test_context_attributes(self):
        previous = fake_context(id="prev")
        attrs = SpanHelper.context_attributes(fake_context(previous_context=previous))
        assert attrs == {
            "context.id": "ctx-1",
            "context.is_multitask": False,
            "context.pipeline_id": "pipe-1",
            "context.task_count": 2,
            "context.tasks": "A,B",
            "context.depth": 2,
            "context.previous_id": "prev",
        }

    def test_a_context_that_fails_to_describe_itself_still_gives_the_rest(self):
        ctx = fake_context()
        ctx.get_depth = lambda: 1 / 0
        attrs = SpanHelper.context_attributes(ctx)
        assert "context.depth" not in attrs and attrs["context.id"] == "ctx-1"

    def test_a_context_without_a_pipeline(self):
        attrs = SpanHelper.context_attributes(fake_context(pipeline=None))
        assert "context.pipeline_id" not in attrs

    def test_add_context_attributes_with_non_primitive_ids(self, otel):
        import uuid

        ctx = fake_context(id=uuid.UUID(int=5))
        with otel.tracer.start_as_current_span("s") as span:
            SpanHelper.add_context_attributes(span, ctx)
        assert otel.span("s").attributes["context.id"] == str(uuid.UUID(int=5))

    def test_pipeline_attributes_redact_secrets_and_cut_long_values(self):
        class Pipe:
            id = "p"
            user = "ann"
            password = "hunter2"
            api_token = "abc"
            blob = "x" * 5000
            empty = None

            def get_fields(self):
                return [
                    (n, None)
                    for n in ("user", "password", "api_token", "blob", "empty")
                ]

        attrs = SpanHelper.pipeline_attributes(Pipe())
        assert attrs["pipeline.class"] == "Pipe" and attrs["pipeline.id"] == "p"
        assert attrs["pipeline.input.user"] == "ann"
        assert attrs["pipeline.input.password"] == "[redacted]"
        assert attrs["pipeline.input.api_token"] == "[redacted]"
        assert "pipeline.input.empty" not in attrs
        assert "hunter2" not in str(attrs)

    def test_add_pipeline_attributes_bounds_values(self, otel):
        class Pipe:
            id = "p"
            blob = "x" * 5000

            def get_fields(self):
                return [("blob", None)]

        with otel.tracer.start_as_current_span("s") as span:
            SpanHelper.add_pipeline_attributes(span, Pipe())
        assert len(otel.span("s").attributes["pipeline.input.blob"]) <= 256

    def test_a_pipeline_whose_fields_cannot_be_read(self):
        class Pipe:
            id = "p"

            def get_fields(self):
                raise RuntimeError

        assert SpanHelper.pipeline_attributes(Pipe())["pipeline.id"] == "p"
