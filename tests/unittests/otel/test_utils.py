"""volnux.otel.utils: helpers for application code."""

import pytest

from volnux.otel import utils


class TestIds:
    def test_none_outside_a_span(self, otel):
        assert utils.get_current_trace_id() is None
        assert utils.get_current_span_id() is None

    def test_the_ids_of_the_current_span(self, otel):
        with otel.tracer.start_as_current_span("s") as span:
            context = span.get_span_context()
            assert utils.get_current_trace_id() == format(context.trace_id, "032x")
            assert utils.get_current_span_id() == format(context.span_id, "016x")
            assert len(utils.get_current_trace_id()) == 32
            assert len(utils.get_current_span_id()) == 16

    def test_a_span_that_is_not_recording(self, otel):
        from opentelemetry.trace import (
            NonRecordingSpan,
            SpanContext,
            TraceFlags,
            use_span,
        )

        span = NonRecordingSpan(SpanContext(1, 2, False, TraceFlags(1)))
        with use_span(span):
            assert utils.get_current_trace_id() is None


class TestAttributesAndEvents:
    def test_add_span_attribute(self, otel):
        with otel.tracer.start_as_current_span("s"):
            utils.add_span_attribute("user.id", 12345)
            utils.add_span_attribute("flag", True)
        attrs = otel.span("s").attributes
        assert attrs["user.id"] == "12345" and attrs["flag"] == "True"  # (as text)

    def test_add_span_event(self, otel):
        with otel.tracer.start_as_current_span("s"):
            utils.add_span_event("validated", {"records": 5})
            utils.add_span_event("plain")
        events = otel.span("s").events
        assert [e.name for e in events] == ["validated", "plain"]
        assert events[0].attributes["records"] == 5

    def test_without_a_span_they_do_nothing(self, otel):
        utils.add_span_attribute("a", 1)
        utils.add_span_event("e")
        assert otel.spans() == []

    def test_failures_are_swallowed(self, monkeypatch, otel):
        class Broken:
            def is_recording(self):
                raise RuntimeError

        monkeypatch.setattr(utils.trace, "get_current_span", lambda: Broken())
        assert utils.get_current_trace_id() is None
        assert utils.get_current_span_id() is None
        utils.add_span_attribute("a", 1)
        utils.add_span_event("e")
