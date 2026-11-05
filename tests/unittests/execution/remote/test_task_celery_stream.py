import json

import orjson
import pytest
import redis
from celery import Celery

from volnux.execution.remote.celery import make_volnux_celery_task
from volnux.execution.remote.context import MiniExecutionContext
from volnux.execution.remote.stream import (
    BatchStreamReader,
    StreamTimeout,
    batch_key_for,
    encode_batch,
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


class FakeRedis:
    def __init__(self, fail_on=()):
        self.lists = {}
        self.ttls = {}
        self.fail_on = set(fail_on)

    def rpush(self, key, value):
        if "rpush" in self.fail_on:
            raise redis.RedisError("down")
        self.lists.setdefault(key, []).append(value)

    def expire(self, key, ttl):
        self.ttls[key] = ttl

    def lpop(self, key):
        items = self.lists.get(key)
        return items.pop(0) if items else None


def build(fake):
    app = Celery("t", broker="memory://", backend="cache+memory://")
    return make_volnux_celery_task(app, fake, task_name=f"t.{id(fake)}")


class TestCeleryTask:
    def test_streams_batches_then_done_with_status(self):
        fake = FakeRedis()
        task = build(fake)
        out = task.run(payload("RtManyEvent", 450, state_id="S", task_id="T").to_dict())
        key = batch_key_for("S", "T")
        assert list(fake.lists) == [key] and fake.ttls[key] == 3600
        reader = BatchStreamReader(fake, key)
        results, errors = reader.read_all(timeout=1)
        assert len(results) == 450 and errors == []
        assert reader.done and reader.status == "COMPLETED"
        assert RemoteTaskResult.from_dict(out).context_delta["batch_key"] == key

    def test_two_tasks_of_one_state_do_not_share_a_stream(self):
        fake = FakeRedis()
        task = build(fake)
        for tid in ("A", "B"):
            task.run(payload("RtManyEvent", 150, state_id="S", task_id=tid).to_dict())
        assert len(fake.lists) == 2
        a, _ = BatchStreamReader(fake, batch_key_for("S", "A")).read_all(timeout=1)
        assert len(a) == 150

    def test_values_json_does_not_know_are_streamed(self):
        fake = FakeRedis()
        build(fake).run(payload("RtDatetimeEvent", state_id="S", task_id="T").to_dict())
        results, _ = BatchStreamReader(fake, batch_key_for("S", "T")).read_all(1)
        assert len(results) == 150 and "2026-01-01" in results[0]["content"]["at"]

    def test_a_crashing_task_still_ends_the_stream_as_failed(self):
        fake = FakeRedis()
        task = build(fake)
        bad = payload("RtOkEvent", state_id="S", task_id="T").to_dict()
        bad["mini_context"] = {"state_id": "S", "mode": "nonsense", "results": 5}
        # whatever happens inside, the sentinel must be there
        try:
            task.run(bad)
        except Exception:
            pass
        reader = BatchStreamReader(fake, batch_key_for("S", "T"))
        list(reader.batches(timeout=1))
        assert reader.done

    def test_redis_outage_fails_the_task_instead_of_losing_results(self):
        fake = FakeRedis(fail_on={"rpush"})
        with pytest.raises(redis.RedisError):
            build(fake).run(
                payload("RtManyEvent", 450, state_id="S", task_id="T").to_dict()
            )


class TestBatchStreamReader:
    def push(self, fake, key, *entries):
        for e in entries:
            fake.rpush(key, e if isinstance(e, bytes) else encode_batch(e))

    def test_reads_in_order_and_stops_at_done(self):
        fake = FakeRedis()
        self.push(
            fake,
            "k",
            {"type": "result_batch", "data": [1], "count": 1},
            {"type": "error_batch", "data": [2], "count": 1},
            {"type": "done", "status": "FAILED"},
            {"type": "result_batch", "data": [3], "count": 1},  # next stream
        )
        r = BatchStreamReader(fake, "k")
        assert [b["data"] for b in r.batches(timeout=1)] == [[1], [2]]
        assert r.status == "FAILED" and r.done
        assert len(fake.lists["k"]) == 1  # nothing past the sentinel was consumed

    def test_times_out_without_a_sentinel(self):
        fake = FakeRedis()
        self.push(fake, "k", {"type": "result_batch", "data": [1], "count": 1})
        with pytest.raises(StreamTimeout):
            list(BatchStreamReader(fake, "k").batches(timeout=0.2))

    def test_read_all_splits_results_and_errors(self):
        fake = FakeRedis()
        self.push(
            fake,
            "k",
            {"type": "result_batch", "data": [1, 2], "count": 2},
            {"type": "error_batch", "data": ["e"], "count": 1},
            {"type": "done", "status": "FAILED"},
        )
        assert BatchStreamReader(fake, "k").read_all(1) == ([1, 2], ["e"])

    def test_encode_handles_unknown_types(self):
        import decimal

        assert orjson.loads(encode_batch({"d": decimal.Decimal("1.5")})) == {"d": "1.5"}
