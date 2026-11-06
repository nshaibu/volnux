"""The records behind a suspension: queue entry (KV), response (messaging), claim."""

import asyncio
import time
from datetime import datetime, timedelta, timezone

import pytest

from volnux.exceptions import ObjectDoesNotExist
from volnux.mixins.event.communication.datastructures import (
    ExternalCommunicationQueueEntry,
    ExternalCommunicationResponse,
    RequestStatus,
    ResumeClaim,
)


def make_entry(request_id="r1", **overrides):
    fields = dict(
        request_id=request_id,
        workflow_name="Orders",
        workflow_id="wf-1",
        task_id="t1",
        request_type="hitl",
        checkpoint_key="ck-1",
        context_id="ctx-1",
        request_title="Approve order",
        request_payload={"order": 7},
        options=["approve", "reject"],
    )
    fields.update(overrides)
    return ExternalCommunicationQueueEntry.create(**fields)


class TestQueueEntry:
    def test_defaults(self):
        entry = make_entry()
        assert entry.status == RequestStatus.WAITING.value
        assert entry.response is None and entry.attempts == 0
        assert entry.timeout_at is None and entry.last_error is None
        assert datetime.fromisoformat(entry.enqueued_at).tzinfo is not None

    def test_record_id_is_the_request_id(self):
        assert make_entry("req-9").id == "req-9"

    def test_route_is_scoped_and_distinct_per_record_kind(self):
        routes = {
            tuple(cls.get_storage_route().components)
            for cls in (
                ExternalCommunicationQueueEntry,
                ExternalCommunicationResponse,
                ResumeClaim,
            )
        }
        assert len(routes) == 3
        assert ExternalCommunicationQueueEntry.get_storage_route().components[:2] == [
            "volnux",
            "{project_id}",
        ]

    @pytest.mark.parametrize(
        "delta, expected",
        [(timedelta(minutes=-1), True), (timedelta(minutes=5), False)],
    )
    def test_expiry(self, delta, expected):
        deadline = (datetime.now(timezone.utc) + delta).isoformat()
        assert make_entry(timeout_at=deadline).is_expired() is expected

    def test_no_timeout_never_expires(self):
        assert make_entry(timeout_at=None).is_expired() is False

    def test_naive_timestamps_are_read_as_utc(self):
        naive = (datetime.utcnow() - timedelta(minutes=1)).isoformat()
        assert make_entry(timeout_at=naive).is_expired() is True

    @pytest.mark.asyncio
    async def test_round_trip_through_the_store(self, memory_store):
        entry = make_entry()
        await entry.save()
        loaded = await ExternalCommunicationQueueEntry.get("r1")
        assert loaded.workflow_name == "Orders" and loaded.options == [
            "approve",
            "reject",
        ]
        assert loaded.request_payload == {"order": 7}

    @pytest.mark.asyncio
    async def test_lookup_by_request_id_and_by_status(self, memory_store):
        await make_entry("a").save()
        waiting = make_entry("b")
        waiting.status = RequestStatus.RESUMED.value
        await waiting.save()
        assert (await ExternalCommunicationQueueEntry.get_or_none("zzz")) is None
        found = await (
            await ExternalCommunicationQueueEntry.filter(status="waiting")
        ).to_list()
        assert [e.request_id for e in found] == ["a"]

    @pytest.mark.asyncio
    async def test_status_updates_persist(self, memory_store):
        entry = make_entry()
        await entry.save()
        entry.status = RequestStatus.READY.value
        entry.response = {"decision": "approve"}
        await entry.save()
        again = await ExternalCommunicationQueueEntry.get("r1")
        assert again.status == "ready" and again.response == {"decision": "approve"}


class TestResponse:
    def test_defaults_and_timeout_factory(self):
        response = ExternalCommunicationResponse(
            request_id="r", decision="approve", data={}
        )
        assert response.source == "human" and response.reviewer == ""
        timeout = ExternalCommunicationResponse.timeout("r")
        assert (timeout.decision, timeout.source, timeout.reviewer) == (
            "timeout",
            "timeout",
            "system",
        )

    def test_wire_state_carries_the_id_and_round_trips(self):
        response = ExternalCommunicationResponse(
            request_id="r", decision="reject", data={"why": "x"}, reviewer="ann"
        )
        state = response.get_state()
        assert state["id"] == response.id
        rebuilt = ExternalCommunicationResponse.__new__(ExternalCommunicationResponse)
        rebuilt.set_state({**state, "from_the_future": 1})  # unknown keys are ignored
        assert (rebuilt.request_id, rebuilt.decision) == ("r", "reject")
        assert rebuilt.data == {"why": "x"} and rebuilt.id == response.id

    @pytest.mark.asyncio
    async def test_delivered_in_order_over_the_messaging_queue(self, memory_store):
        for request_id in ("r1", "r2", "r3"):
            await ExternalCommunicationResponse.enqueue(
                ExternalCommunicationResponse(
                    request_id=request_id, decision="approve", data={}
                )
            )
        assert await ExternalCommunicationResponse.queue_length() == 3
        got = [
            (await ExternalCommunicationResponse.dequeue(timeout=0.2)).request_id
            for _ in range(3)
        ]
        assert got == ["r1", "r2", "r3"]
        assert await ExternalCommunicationResponse.dequeue(timeout=0.05) is None

    @pytest.mark.asyncio
    async def test_a_blocked_consumer_wakes_when_a_response_arrives(self, memory_store):
        consumer = asyncio.ensure_future(
            ExternalCommunicationResponse.dequeue(timeout=2)
        )
        await asyncio.sleep(0.05)
        assert not consumer.done()
        await ExternalCommunicationResponse.enqueue(
            ExternalCommunicationResponse(
                request_id="late", decision="approve", data={}
            )
        )
        assert (await asyncio.wait_for(consumer, 2)).request_id == "late"


class TestResumeClaim:
    @pytest.mark.asyncio
    async def test_first_claim_wins(self, memory_store):
        mine = await ResumeClaim.acquire("r1", owner="worker-a", lease_seconds=30)
        assert mine is not None and mine.owner == "worker-a"
        assert (
            await ResumeClaim.acquire("r1", owner="worker-b", lease_seconds=30) is None
        )

    @pytest.mark.asyncio
    async def test_claims_are_per_request(self, memory_store):
        assert await ResumeClaim.acquire("r1", "a", 30) is not None
        assert await ResumeClaim.acquire("r2", "b", 30) is not None

    @pytest.mark.asyncio
    async def test_concurrent_claimants_yield_one_winner(self, memory_store):
        results = await asyncio.gather(
            *(ResumeClaim.acquire("r1", "w%d" % i, 30) for i in range(10))
        )
        assert len([r for r in results if r is not None]) == 1

    @pytest.mark.asyncio
    async def test_release_frees_the_request(self, memory_store):
        claim = await ResumeClaim.acquire("r1", "a", 30)
        await claim.release()
        await claim.release()  # idempotent
        assert await ResumeClaim.acquire("r1", "b", 30) is not None

    @pytest.mark.asyncio
    async def test_an_expired_claim_is_taken_over(self, memory_store):
        now = time.time()
        await ResumeClaim.acquire("r1", "dead-worker", 5, now=now - 100)
        taken = await ResumeClaim.acquire("r1", "new-worker", 30, now=now)
        assert taken is not None and taken.owner == "new-worker"
        assert (await ResumeClaim.get("r1")).owner == "new-worker"

    @pytest.mark.asyncio
    async def test_a_live_claim_is_not_taken_over(self, memory_store):
        now = time.time()
        await ResumeClaim.acquire("r1", "a", 60, now=now)
        assert await ResumeClaim.acquire("r1", "b", 60, now=now + 1) is None
        assert (await ResumeClaim.get("r1")).owner == "a"

    @pytest.mark.asyncio
    async def test_claim_record_is_gone_after_release(self, memory_store):
        claim = await ResumeClaim.acquire("r1", "a", 30)
        await claim.release()
        with pytest.raises(ObjectDoesNotExist):
            await ResumeClaim.get("r1")
