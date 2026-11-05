import asyncio
import json

import pytest
from celery import Celery

from tests.unittests.support import SerializingMemoryBackend

from volnux.execution.remote.celery import make_volnux_celery_task
from volnux.execution.remote.context import MiniExecutionContext
from volnux.execution.remote.stream import (
    BatchStreamReader,
    RemoteBatch,
    StreamTimeout,
    batch_key_for,
    batch_route,
    done_entry,
)
from volnux.execution.remote.task import (
    RemoteTaskPayload,
    RemoteTaskResult,
    execute_remote_task,
)
from volnux.result import EventResult


class RtOkEvent:
    def __init__(self, execution_context, **kw):
        self.ctx = execution_context
        self.kw = kw

    async def __call__(self, *args, **kwargs):
        return EventResult(
            error=False,
            content={"args": list(args), "kwargs": kwargs, "init": self.kw},
            task_id="t1",
            event_name="RtOkEvent",
        )


class RtManyEvent(RtOkEvent):
    async def __call__(self, n):
        for i in range(n):
            await self.ctx.add_result({"error": False, "content": {"value": i}})
        return EventResult(error=False, content=n, task_id="t1", event_name="E")


class RtRaisingEvent(RtOkEvent):
    async def __call__(self):
        raise ValueError("kaput")


class RtWrongTypeEvent(RtOkEvent):
    async def __call__(self):
        return "not an EventResult"


class RtDatetimeEvent(RtOkEvent):
    async def __call__(self):
        import datetime

        for i in range(150):
            await self.ctx.add_result(
                {"error": False, "content": {"at": datetime.datetime(2026, 1, 1)}}
            )
        return EventResult(error=False, content="x", task_id="t1", event_name="E")


def payload(cls, *call_args, state_id="st", task_id="t1", **init):
    return RemoteTaskPayload(
        event_class_path=f"{__name__}.{cls}",
        event_init_kwargs=init,
        mini_context=MiniExecutionContext(state_id=state_id).__dict__
        and {"state_id": state_id},
        event_call_args=call_args,
        event_call_kwargs={},
        task_id=task_id,
    )


class TestPayloadAndResult:
    def test_payload_round_trip_restores_tuple(self):
        p = payload("RtOkEvent", 1, 2)
        again = RemoteTaskPayload.from_dict(json.loads(json.dumps(p.to_dict())))
        assert again == p and again.event_call_args == (1, 2)

    def test_result_round_trip(self):
        r = RemoteTaskResult({"a": 1}, {"b": 2}, "h", 5, 0.5)
        assert RemoteTaskResult.from_dict(r.to_dict()) == r

    def test_result_from_dict_tolerates_unknown_and_missing_keys(self):
        r = RemoteTaskResult.from_dict(
            {"event_result": {}, "context_delta": {}, "extra": 1}
        )
        assert r.worker_pid == 0 and r.execution_duration == 0.0


class TestExecuteRemoteTask:
    @pytest.mark.asyncio
    async def test_success(self):
        r = await execute_remote_task(payload("RtOkEvent", 1, flag=True), batch_key="K")
        assert r.event_result["error"] is False
        assert r.event_result["content"]["args"] == [1]
        assert r.event_result["content"]["init"] == {"flag": True}
        assert r.context_delta["batch_key"] == "K"
        assert r.worker_pid > 0 and r.worker_hostname
        assert r.execution_duration >= 0

    @pytest.mark.asyncio
    async def test_exception_becomes_an_error_result_and_a_failed_delta(self):
        r = await execute_remote_task(payload("RtRaisingEvent"))
        assert r.event_result["error"] is True
        assert "kaput" in str(r.event_result["content"])
        d = r.context_delta
        assert d["final_status"] == "FAILED"
        err = d["new_errors"][0]
        assert err["type"] == "ValueError"
        assert err["task_id"] == "t1" and err["event_name"] == "RtRaisingEvent"

    @pytest.mark.asyncio
    async def test_non_event_result_is_reported_as_an_error(self):
        r = await execute_remote_task(payload("RtWrongTypeEvent"))
        assert r.event_result["error"] is True
        assert "expected EventResult" in str(r.event_result["content"])

    @pytest.mark.asyncio
    async def test_unimportable_event_is_an_error_result_not_a_crash(self):
        p = payload("RtOkEvent")
        p.event_class_path = "no_such_module_xyz.Event"
        r = await execute_remote_task(p)
        assert r.event_result["error"] is True
        assert r.context_delta["final_status"] == "FAILED"
        assert r.event_result["event_name"] == "Event"

    @pytest.mark.asyncio
    async def test_streams_when_volume_is_high(self):
        got = []
        r = await execute_remote_task(
            payload("RtManyEvent", 450), flush_callback=got.append
        )
        assert r.context_delta["mode"] == "batch"
        assert sum(b["count"] for b in got) == 450
        assert r.context_delta["new_results"] == []
        assert r.context_delta["final_status"] == "COMPLETED"


@pytest.fixture
def bus(monkeypatch):
    """The messaging backend the worker and the requester share (in memory here)."""
    backend = SerializingMemoryBackend(namespace_prefix="remote-test")
    monkeypatch.setattr(RemoteBatch, "_backend_store", backend, raising=False)
    monkeypatch.setattr(RemoteBatch, "_backend_initialized", True, raising=False)
    yield backend
    backend.close()


def build():
    app = Celery("t", broker="memory://", backend="cache+memory://")
    return make_volnux_celery_task(app, task_name=f"t.{id(app)}")


def read_all(state_id, task_id, timeout=1):
    # (the requester is another process, with a loop of its own)
    reader = BatchStreamReader(state_id, task_id)
    results, errors = asyncio.run(reader.read_all(timeout=timeout))
    return reader, results, errors


class TestCeleryTask:
    def test_streams_batches_then_done_with_status(self, bus):
        out = build().run(
            payload("RtManyEvent", 450, state_id="S", task_id="T").to_dict()
        )
        reader, results, errors = read_all("S", "T")
        assert len(results) == 450 and errors == []
        assert reader.done and reader.status == "COMPLETED"
        key = batch_key_for("S", "T")
        assert reader.key == key
        assert RemoteTaskResult.from_dict(out).context_delta["batch_key"] == key

    def test_the_stream_is_a_queue_that_expires(self, bus):
        build().run(payload("RtManyEvent", 450, state_id="S", task_id="T").to_dict())
        deadlines = bus._queue_deadlines
        assert len(deadlines) == 1
        (key,) = deadlines
        assert key.endswith("S:T") and "remote" in key

    def test_two_tasks_of_one_state_do_not_share_a_stream(self, bus):
        task = build()
        for tid in ("A", "B"):
            task.run(payload("RtManyEvent", 150, state_id="S", task_id=tid).to_dict())
        _, a, _ = read_all("S", "A")
        _, b, _ = read_all("S", "B")
        assert len(a) == len(b) == 150

    def test_values_json_does_not_know_are_streamed(self, bus):
        build().run(payload("RtDatetimeEvent", state_id="S", task_id="T").to_dict())
        _, results, _ = read_all("S", "T")
        assert len(results) == 150 and "2026-01-01" in results[0]["content"]["at"]

    def test_a_crashing_task_still_ends_the_stream_as_failed(self, bus):
        bad = payload("RtOkEvent", state_id="S", task_id="T").to_dict()
        bad["mini_context"] = {"state_id": "S", "mode": "nonsense", "results": 5}
        # whatever happens inside, the done entry must be there
        try:
            build().run(bad)
        except Exception:
            pass
        reader = BatchStreamReader("S", "T")

        async def drain():
            return [b async for b in reader.batches(timeout=1)]

        asyncio.run(drain())
        assert reader.done

    def test_a_backend_outage_fails_the_task_instead_of_losing_results(
        self, bus, monkeypatch
    ):
        async def down(*args, **kwargs):
            raise ConnectionError("down")

        monkeypatch.setattr(bus, "push", down)
        with pytest.raises(ConnectionError):
            build().run(
                payload("RtManyEvent", 450, state_id="S", task_id="T").to_dict()
            )

    def test_a_failing_done_entry_is_only_logged(self, bus, monkeypatch, caplog):
        real = bus.push

        async def push(key, *values, **kwargs):
            if any(v.kind == "done" for v in values):
                raise ConnectionError("down")
            return await real(key, *values, **kwargs)

        monkeypatch.setattr(bus, "push", push)
        out = build().run(payload("RtOkEvent", state_id="S", task_id="T").to_dict())
        assert out  # the task itself succeeded
        assert "Failed to push done entry" in caplog.text


class TestBatchStreamReader:
    async def put(self, *entries, state_id="S", task_id="T"):
        for e in entries:
            await RemoteBatch.push(e, route_override=batch_route(state_id, task_id))

    def batch(self, kind, data):
        return RemoteBatch(kind=kind, data=data, count=len(data))

    @pytest.mark.asyncio
    async def test_reads_in_order_and_stops_at_done(self, bus):
        await self.put(
            self.batch("result_batch", [1]),
            self.batch("error_batch", [2]),
            done_entry("FAILED"),
            self.batch("result_batch", [3]),  # next stream
        )
        r = BatchStreamReader("S", "T")
        assert [b["data"] async for b in r.batches(timeout=1)] == [[1], [2]]
        assert r.status == "FAILED" and r.done
        # nothing past the done entry was consumed
        assert await RemoteBatch.queue_length(route_override=batch_route("S", "T")) == 1

    @pytest.mark.asyncio
    async def test_times_out_without_a_done_entry(self, bus):
        await self.put(self.batch("result_batch", [1]))
        with pytest.raises(StreamTimeout):
            async for _ in BatchStreamReader("S", "T").batches(timeout=0.2):
                pass

    @pytest.mark.asyncio
    async def test_it_waits_for_entries_that_arrive_later(self, bus):
        async def writer():
            await asyncio.sleep(0.05)
            await self.put(self.batch("result_batch", [1]), done_entry("COMPLETED"))

        task = asyncio.create_task(writer())
        results, errors = await BatchStreamReader("S", "T").read_all(timeout=2)
        await task
        assert (results, errors) == ([1], [])

    @pytest.mark.asyncio
    async def test_read_all_splits_results_and_errors(self, bus):
        await self.put(
            self.batch("result_batch", [1, 2]),
            self.batch("error_batch", ["e"]),
            done_entry("FAILED"),
        )
        assert await BatchStreamReader("S", "T").read_all(1) == ([1, 2], ["e"])

    @pytest.mark.asyncio
    async def test_a_timeout_must_be_positive(self, bus):
        with pytest.raises(ValueError):
            async for _ in BatchStreamReader("S", "T").batches(timeout=0):
                pass

    def test_from_batch_handles_unknown_types(self):
        import decimal

        entry = RemoteBatch.from_batch(
            {"type": "result_batch", "data": [{"d": decimal.Decimal("1.5")}]}
        )
        assert entry.data == [{"d": "1.5"}] and entry.count == 1
        assert entry.to_batch()["type"] == "result_batch"


class TestAsyncFlushCallback:
    """The context awaits an async flush callback, in order."""

    @pytest.mark.asyncio
    async def test_batches_are_awaited_in_order(self):
        seen = []

        async def callback(batch):
            await asyncio.sleep(0)
            seen.append((batch["type"], batch["count"]))

        ctx = MiniExecutionContext(state_id="S")
        await ctx.set_flush_callback(callback)
        for i in range(450):
            await ctx.add_result({"error": False, "content": {"i": i}})
        await ctx.finalize()
        assert sum(count for _, count in seen) == 450
        assert all(kind == "result_batch" for kind, _ in seen)

    @pytest.mark.asyncio
    async def test_a_failing_async_callback_fails_the_operation(self):
        async def callback(batch):
            raise ConnectionError("down")

        ctx = MiniExecutionContext(state_id="S")
        await ctx.set_flush_callback(callback)
        with pytest.raises(ConnectionError):
            for i in range(450):
                await ctx.add_result({"error": False, "content": {"i": i}})

    @pytest.mark.asyncio
    async def test_a_sync_callback_still_works(self):
        got = []
        ctx = MiniExecutionContext(state_id="S")
        await ctx.set_flush_callback(got.append)
        for i in range(450):
            await ctx.add_result({"error": False, "content": {"i": i}})
        await ctx.finalize()
        assert sum(b["count"] for b in got) == 450
