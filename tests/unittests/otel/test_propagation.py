"""A trace that crosses a process: the carrier of volnux.context carries it."""

import asyncio

import pytest
from opentelemetry import trace

from volnux import context as volnux_context_module
from volnux.context import (
    dispatch_worker_task,
    extract_carrier,
    inject_carrier,
    snapshot_context,
    volnux_context,
)
from volnux.otel.propagation import (
    attach_trace_context,
    current_trace_context,
    install_trace_propagation,
    remove_trace_propagation,
)


@pytest.fixture
def propagating(otel):
    install_trace_propagation()
    yield otel
    remove_trace_propagation()


class TestCurrentTraceContext:
    def test_none_without_a_span(self, otel):
        assert current_trace_context() is None

    def test_the_w3c_headers_of_the_active_span(self, otel):
        with otel.tracer.start_as_current_span("s") as span:
            carrier = current_trace_context()
        context = span.get_span_context()
        version, trace_id, span_id, flags = carrier["traceparent"].split("-")
        assert (version, int(trace_id, 16), int(span_id, 16)) == (
            "00",
            context.trace_id,
            context.span_id,
        )


class TestAttach:
    def test_spans_inside_continue_the_carried_trace(self, otel):
        with otel.tracer.start_as_current_span("dispatcher") as dispatcher:
            carrier = current_trace_context()
        with attach_trace_context(carrier):
            with otel.tracer.start_as_current_span("worker"):
                pass
        worker = otel.span("worker")
        assert worker.context.trace_id == dispatcher.get_span_context().trace_id
        assert worker.parent.span_id == dispatcher.get_span_context().span_id
        assert worker.parent.is_remote

    def test_the_previous_context_is_restored(self, otel):
        with otel.tracer.start_as_current_span("dispatcher"):
            carrier = current_trace_context()
        with attach_trace_context(carrier):
            pass
        assert not trace.get_current_span().get_span_context().is_valid

    def test_restored_even_when_the_block_raises(self, otel):
        with otel.tracer.start_as_current_span("d"):
            carrier = current_trace_context()
        with pytest.raises(RuntimeError):
            with attach_trace_context(carrier):
                raise RuntimeError
        assert not trace.get_current_span().get_span_context().is_valid


class TestThroughTheVolnuxContext:
    def test_the_snapshot_carries_the_trace(self, propagating):
        otel = propagating
        with otel.tracer.start_as_current_span("dispatcher"):
            snapshot = snapshot_context()
        assert "traceparent" in snapshot.trace_context

    def test_no_trace_no_trace_context(self, propagating):
        assert snapshot_context().trace_context is None

    def test_without_the_hook_the_snapshot_has_none(self, otel):
        with otel.tracer.start_as_current_span("dispatcher"):
            assert snapshot_context().trace_context is None

    def test_it_survives_the_carrier_round_trip(self, propagating):
        otel = propagating
        with otel.tracer.start_as_current_span("dispatcher") as dispatcher:
            carrier = inject_carrier({})
        import json

        restored = extract_carrier(json.loads(json.dumps(carrier)))  # a broker
        assert (
            restored.trace_context
            == carrier[volnux_context_module.CARRIER_KEY]["trace_context"]
        )
        with volnux_context(snapshot=restored):
            with otel.tracer.start_as_current_span("worker"):
                pass
        worker = otel.span("worker")
        assert worker.parent.span_id == dispatcher.get_span_context().span_id

    def test_a_worker_entrypoint_joins_the_dispatchers_trace(self, propagating):
        otel = propagating
        with otel.tracer.start_as_current_span("dispatcher") as dispatcher:
            payload = inject_carrier({"workflow_name": "wf"})

        @dispatch_worker_task
        def task(payload):
            with otel.tracer.start_as_current_span("worker.run"):
                return "ran"

        assert task(payload) == "ran"
        assert otel.span("worker.run").context.trace_id == (
            dispatcher.get_span_context().trace_id
        )

    @pytest.mark.asyncio
    async def test_an_async_worker_entrypoint(self, propagating):
        otel = propagating
        with otel.tracer.start_as_current_span("dispatcher") as dispatcher:
            payload = inject_carrier({})

        @dispatch_worker_task
        async def task(payload):
            await asyncio.sleep(0)
            with otel.tracer.start_as_current_span("worker.run"):
                pass

        await task(payload)
        assert otel.span("worker.run").parent.span_id == (
            dispatcher.get_span_context().span_id
        )

    def test_the_trace_is_detached_after_the_context(self, propagating):
        otel = propagating
        with otel.tracer.start_as_current_span("d"):
            snapshot = snapshot_context()
        with volnux_context(snapshot=snapshot):
            pass
        assert not trace.get_current_span().get_span_context().is_valid

    def test_a_hook_that_fails_does_not_break_the_context(self, otel):
        from volnux.context import TracePropagation, register_trace_propagation

        def broken(*a):
            raise RuntimeError("tracing broke")

        register_trace_propagation(TracePropagation(inject=broken, attach=broken))
        try:
            snapshot = snapshot_context()
            assert snapshot.trace_context is None
            snapshot = type(snapshot)(trace_context={"traceparent": "x"})
            with volnux_context(snapshot=snapshot, workflow_id="w"):
                assert volnux_context_module.get_current_workflow_id() == "w"
        finally:
            register_trace_propagation(None)

    def test_removing_it_stops_the_propagation(self, otel):
        install_trace_propagation()
        remove_trace_propagation()
        with otel.tracer.start_as_current_span("d"):
            assert snapshot_context().trace_context is None

    def test_a_snapshot_without_a_trace_context_activates_cleanly(self, propagating):
        snapshot = snapshot_context()
        with volnux_context(snapshot=snapshot, workflow_id="w"):
            pass
