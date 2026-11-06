"""The shared building blocks: clean_attributes, traced, end_span_with."""

import asyncio

import pytest
from opentelemetry import trace
from opentelemetry.trace import StatusCode

from volnux.otel.instrumentations._common import (
    MAX_ATTRIBUTE_LENGTH,
    clean_attributes,
    end_span_with,
    get_tracer,
    record_error,
    set_attributes,
    traced,
)


class TestCleanAttributes:
    def test_primitives_are_kept(self):
        assert clean_attributes({"a": 1, "b": 1.5, "c": True, "d": "x"}) == {
            "a": 1,
            "b": 1.5,
            "c": True,
            "d": "x",
        }

    def test_none_is_dropped(self):
        assert clean_attributes({"a": None, "b": 0, "c": ""}) == {"b": 0, "c": ""}

    def test_objects_become_text(self):
        class Thing:
            def __str__(self):
                return "thing"

        assert clean_attributes({"a": Thing(), "b": [1, 2], "c": {"k": 1}}) == {
            "a": "thing",
            "b": "[1, 2]",
            "c": "{'k': 1}",
        }

    def test_long_text_is_cut(self):
        out = clean_attributes({"a": "x" * 1000})["a"]
        assert len(out) == MAX_ATTRIBUTE_LENGTH and out.endswith("…")

    def test_text_at_the_limit_is_kept(self):
        text = "y" * MAX_ATTRIBUTE_LENGTH
        assert clean_attributes({"a": text})["a"] == text

    def test_a_value_that_cannot_be_printed_is_dropped(self):
        class Broken:
            def __str__(self):
                raise RuntimeError("no")

        assert clean_attributes({"a": Broken(), "b": 1}) == {"b": 1}

    def test_nothing(self):
        assert clean_attributes(None) == {}
        assert clean_attributes({}) == {}


class TestSetAttributes:
    def test_it_sets_cleaned_attributes(self, otel):
        with otel.tracer.start_as_current_span("s") as span:
            set_attributes(span, {"a": 1, "b": None, "c": object})
        attrs = otel.span("s").attributes
        assert attrs["a"] == 1 and "b" not in attrs and "c" in attrs

    def test_it_never_raises(self):
        class Broken:
            def set_attribute(self, *a):
                raise RuntimeError("x")

        set_attributes(Broken(), {"a": 1})


class TestRecordError:
    def test_an_exception_is_recorded(self, otel):
        with otel.tracer.start_as_current_span("s") as span:
            record_error(span, ValueError("bad"))
        done = otel.span("s")
        assert done.status.status_code is StatusCode.ERROR
        assert done.events[0].name == "exception"
        assert done.events[0].attributes["exception.type"] == "ValueError"

    @pytest.mark.parametrize("error", ["text error", {"code": 5}, 42])
    def test_what_is_not_an_exception_is_recorded_too(self, otel, error):
        with otel.tracer.start_as_current_span("s") as span:
            record_error(span, error)
        done = otel.span("s")
        assert done.status.status_code is StatusCode.ERROR
        assert str(error) in done.events[0].attributes["exception.message"]


class TestEndSpanWith:
    def test_success(self, otel):
        span = otel.tracer.start_span("s")
        end_span_with(span)
        assert otel.span("s").status.status_code is StatusCode.OK

    def test_error(self, otel):
        span = otel.tracer.start_span("s")
        end_span_with(span, RuntimeError("boom"))
        done = otel.span("s")
        assert done.status.status_code is StatusCode.ERROR
        assert done.events[0].attributes["exception.message"] == "boom"

    def test_the_span_is_always_ended(self, otel):
        class Flaky:
            def __init__(self):
                self.ended = False

            def is_recording(self):
                raise RuntimeError("x")

            def end(self):
                self.ended = True

        span = Flaky()
        with pytest.raises(RuntimeError):
            end_span_with(span)
        assert span.ended


class TestGetTracer:
    def test_none_before_initialisation(self, no_otel):
        assert get_tracer() is None

    def test_the_tracer_after(self, otel):
        assert get_tracer() is otel.tracer


class TestTracedSync:
    def test_a_span_around_the_call(self, otel):
        @traced("work", attributes=lambda x: {"x": x})
        def work(x):
            return x * 2

        assert work(4) == 8
        assert otel.span("work").attributes["x"] == 4

    def test_the_arguments_and_result_pass_through(self, otel):
        @traced("work")
        def work(a, b=2, *rest, **named):
            return (a, b, rest, named)

        assert work(1, 3, 4, k=5) == (1, 3, (4,), {"k": 5})

    def test_the_wrapper_keeps_the_name_and_kind(self, otel):
        @traced("work")
        def work():
            """doc"""

        assert work.__name__ == "work" and work.__doc__ == "doc"
        assert not asyncio.iscoroutinefunction(work)

    def test_an_exception_is_recorded_and_raised_unchanged(self, otel):
        error = ValueError("bad")

        @traced("work")
        def work():
            raise error

        with pytest.raises(ValueError) as caught:
            work()
        assert caught.value is error
        done = otel.span("work")
        assert done.status.status_code is StatusCode.ERROR
        assert done.events[0].name == "exception"

    def test_without_a_tracer_the_function_runs_as_is(self, no_otel):
        calls = []

        @traced("work", attributes=lambda: calls.append("attrs") or {})
        def work():
            calls.append("run")
            return 1

        assert work() == 1
        assert calls == ["run"]  # not even the attributes were built

    def test_a_span_name_function(self, otel):
        @traced(lambda kind: f"work.{kind}")
        def work(kind):
            return kind

        work("a")
        assert otel.names() == ["work.a"]

    def test_attributes_that_fail_do_not_fail_the_call(self, otel):
        @traced("work", attributes=lambda: 1 / 0)
        def work():
            return "ok"

        assert work() == "ok"
        assert otel.names() == ["work"]  # traced, just without them

    def test_a_span_name_that_fails_runs_the_function_untraced(self, otel):
        @traced(lambda: 1 / 0)
        def work():
            return "ok"

        assert work() == "ok"
        assert otel.names() == []

    def test_result_attributes(self, otel):
        @traced("work", result_attributes=lambda r: {"size": len(r)})
        def work():
            return [1, 2, 3]

        work()
        assert otel.span("work").attributes["size"] == 3

    def test_result_attributes_that_fail_are_ignored(self, otel):
        @traced("work", result_attributes=lambda r: 1 / 0)
        def work():
            return 5

        assert work() == 5

    def test_after_reads_the_state_the_call_left(self, otel):
        class Box:
            n = 0

            @traced("bump", after=lambda args, kwargs, result: {"n": args[0].n})
            def bump(self):
                self.n += 1

        Box().bump()
        assert otel.span("bump").attributes["n"] == 1

    def test_after_is_not_called_on_failure(self, otel):
        seen = []

        @traced("work", after=lambda *a: seen.append(1) or {})
        def work():
            raise RuntimeError

        with pytest.raises(RuntimeError):
            work()
        assert seen == []

    def test_on_finish_gets_the_duration_and_the_error(self, otel):
        seen = []

        @traced("work", on_finish=lambda a, k, d, e: seen.append((a, k, d >= 0, e)))
        def work(x):
            return x

        work(1)
        assert seen == [((1,), {}, True, None)]

        @traced("bad", on_finish=lambda a, k, d, e: seen.append(e))
        def bad():
            raise KeyError("k")

        with pytest.raises(KeyError):
            bad()
        assert isinstance(seen[-1], KeyError)

    def test_on_finish_failures_are_swallowed(self, otel):
        @traced("work", on_finish=lambda *a: 1 / 0)
        def work():
            return 7

        assert work() == 7

    def test_the_span_is_current_inside_the_call(self, otel):
        inside = []

        @traced("work")
        def work():
            inside.append(trace.get_current_span().get_span_context().span_id)

        work()
        assert inside[0] == otel.span("work").context.span_id

    def test_spans_nest(self, otel):
        @traced("inner")
        def inner():
            pass

        @traced("outer")
        def outer():
            inner()

        outer()
        assert otel.span("inner").parent.span_id == otel.span("outer").context.span_id

    def test_the_kind_can_be_set(self, otel):
        @traced("work", kind=trace.SpanKind.CLIENT)
        def work():
            pass

        work()
        assert otel.span("work").kind is trace.SpanKind.CLIENT


class TestTracedAsync:
    @pytest.mark.asyncio
    async def test_the_span_covers_the_awaited_call(self, otel):
        @traced("work")
        async def work():
            await asyncio.sleep(0.02)
            return "done"

        assert asyncio.iscoroutinefunction(work)
        assert await work() == "done"
        done = otel.span("work")
        assert (done.end_time - done.start_time) >= 15_000_000  # ns

    @pytest.mark.asyncio
    async def test_an_exception_is_recorded_and_raised(self, otel):
        @traced("work")
        async def work():
            raise ValueError("bad")

        with pytest.raises(ValueError):
            await work()
        assert otel.span("work").status.status_code is StatusCode.ERROR

    @pytest.mark.asyncio
    async def test_cancellation_ends_the_span_as_an_error(self, otel):
        started = asyncio.Event()

        @traced("work")
        async def work():
            started.set()
            await asyncio.sleep(10)

        task = asyncio.create_task(work())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert otel.span("work").status.status_code is StatusCode.ERROR

    @pytest.mark.asyncio
    async def test_on_finish_sees_a_cancellation(self, otel):
        seen = []
        started = asyncio.Event()

        @traced("work", on_finish=lambda a, k, d, e: seen.append(e))
        async def work():
            started.set()
            await asyncio.sleep(10)

        task = asyncio.create_task(work())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert isinstance(seen[0], asyncio.CancelledError)

    @pytest.mark.asyncio
    async def test_without_a_tracer(self, no_otel):
        @traced("work")
        async def work():
            return 5

        assert await work() == 5

    @pytest.mark.asyncio
    async def test_concurrent_calls_get_their_own_spans(self, otel):
        @traced("work", attributes=lambda n: {"n": n})
        async def work(n):
            await asyncio.sleep(0.01)
            return n

        assert await asyncio.gather(*(work(i) for i in range(4))) == [0, 1, 2, 3]
        assert sorted(s.attributes["n"] for s in otel.spans_named("work")) == [
            0,
            1,
            2,
            3,
        ]
