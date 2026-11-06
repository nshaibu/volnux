"""Kafka messaging logic that needs no broker: naming, globbing, ack bookkeeping."""

import threading
from unittest.mock import MagicMock

import pytest

pytest.importorskip("confluent_kafka")

from confluent_kafka import KafkaError, KafkaException, TopicPartition  # noqa: E402

from volnux.backends.connectors.kafka import KafkaConnector  # noqa: E402
from volnux.backends.messaging.base import QueueSide  # noqa: E402
from volnux.backends.messaging.stores.kafka import (  # noqa: E402
    KafkaStorePushPopMixin,
    _GroupConsumer,
    _lrange,
    _split_entry_id,
    glob_to_regex,
    sanitize_name,
    topic_name,
)


class FakeMsg:
    def __init__(self, partition, offset, value=b"v", error=None):
        self._p, self._o, self._v, self._e = partition, offset, value, error

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return self._v

    def error(self):
        return self._e


def group(consumer=None):
    return _GroupConsumer(consumer or MagicMock(), "t.log", "g")


def eids(entries):
    return [eid for eid, _ in entries]


class TestNaming:
    def test_sanitize_replaces_illegal_characters(self):
        assert sanitize_name("volnux:Workflow/a b") == "volnux_Workflow_a_b"
        assert sanitize_name("ok.name-1_x") == "ok.name-1_x"

    def test_topic_name_adds_kind_suffix(self):
        assert topic_name("volnux.Job", "q") == "volnux.Job.q"
        assert topic_name("a:b", "ps") == "a_b.ps"

    def test_kinds_never_collide(self):
        names = {topic_name("x", kind) for kind in ("kv", "ps", "q", "log")}
        assert len(names) == 4

    def test_long_names_are_truncated_unique_and_stable(self):
        a = topic_name("a" * 400, "q")
        b = topic_name("a" * 399 + "b", "q")
        assert len(a) <= 249 and len(b) <= 249
        assert a != b
        assert a == topic_name("a" * 400, "q")


class TestGlob:
    @pytest.mark.parametrize(
        "pattern, base, matches",
        [
            ("volnux:task:*", "volnux_task_abc", True),
            ("volnux:task:*", "volnux_other_abc", False),
            ("a?c", "abc", True),
            ("a?c", "abbc", False),
            ("*", "anything.at-all_1", True),
            ("job[12]", "job1", True),
            ("job[12]", "job3", False),
            ("job[!12]", "job3", True),
            ("exact", "exact", True),
            ("exact", "exactly", False),
        ],
    )
    def test_matches(self, pattern, base, matches):
        assert bool(glob_to_regex(pattern).fullmatch(base)) is matches

    def test_regex_metacharacters_in_the_pattern_are_literal(self):
        assert glob_to_regex("a.b").fullmatch("a.b")
        assert not glob_to_regex("a.b").fullmatch("axb")


class TestHelpers:
    @pytest.mark.parametrize(
        "start, stop, expected",
        [
            (0, -1, [0, 1, 2, 3, 4]),
            (1, 2, [1, 2]),
            (-2, -1, [3, 4]),
            (0, -2, [0, 1, 2, 3]),
            (10, 20, []),
            (3, 1, []),
        ],
    )
    def test_lrange_matches_redis(self, start, stop, expected):
        assert _lrange([0, 1, 2, 3, 4], start, stop) == expected

    def test_entry_id_round_trip(self):
        assert _split_entry_id("3-1250") == (3, 1250)


class TestConnector:
    def test_bootstrap_defaults_to_host_port(self):
        assert KafkaConnector(host="h", port=1234).bootstrap_servers == "h:1234"

    def test_explicit_bootstrap_servers_win(self):
        c = KafkaConnector(bootstrap_servers="a:1,b:2")
        assert c.base_config()["bootstrap.servers"] == "a:1,b:2"

    def test_sasl_settings_derived_from_credentials(self):
        conf = KafkaConnector(
            username="u", password="p", ssl_enabled=True
        ).base_config()
        assert conf["security.protocol"] == "SASL_SSL"
        assert (conf["sasl.username"], conf["sasl.password"]) == ("u", "p")

    def test_plain_connection_has_no_security_settings(self):
        conf = KafkaConnector().base_config()
        assert "security.protocol" not in conf and "sasl.username" not in conf

    def test_backend_options_are_kept_for_the_backend(self):
        c = KafkaConnector(num_partitions=4, max_blocking=2)
        assert c.config.extra_params["num_partitions"] == 4

    def test_transactions_are_explicitly_unsupported(self):
        with pytest.raises(NotImplementedError):
            with KafkaConnector().transaction():
                pass

    def test_not_connected_until_connect(self):
        c = KafkaConnector()
        assert not c.is_connected()
        with pytest.raises(ConnectionError):
            c.get_cursor()


class TestAckBookkeeping:
    def committed(self, consumer):
        return [
            (tp.partition, tp.offset)
            for call in consumer.commit.call_args_list
            for tp in call.kwargs["offsets"]
        ]

    def test_in_order_acks_commit_offset_plus_one(self):
        consumer = MagicMock()
        gc = group(consumer)
        ids = eids(gc.register([FakeMsg(0, 5), FakeMsg(0, 6), FakeMsg(0, 7)], "c"))

        assert gc.ack(ids[:2]) == 2
        assert self.committed(consumer) == [(0, 7)]

    def test_out_of_order_ack_waits_for_the_gap(self):
        consumer = MagicMock()
        gc = group(consumer)
        ids = eids(gc.register([FakeMsg(0, 0), FakeMsg(0, 1), FakeMsg(0, 2)], "c"))

        assert gc.ack([ids[2]]) == 1
        assert consumer.commit.call_count == 0  # entry 0 is still unacknowledged

        gc.ack([ids[0]])
        assert self.committed(consumer) == [(0, 1)]  # only up to the gap

        gc.ack([ids[1]])
        assert self.committed(consumer)[-1] == (0, 3)  # gap closed: all three

    def test_partitions_commit_independently(self):
        consumer = MagicMock()
        gc = group(consumer)
        ids = eids(gc.register([FakeMsg(0, 0), FakeMsg(1, 0)], "c"))

        gc.ack([ids[1]])
        assert self.committed(consumer) == [(1, 1)]

    def test_unknown_and_repeated_acks_count_zero(self):
        gc = group()
        (eid,) = eids(gc.register([FakeMsg(0, 0)], "c"))
        assert gc.ack(["9-9"]) == 0
        assert gc.ack([eid]) == 1
        assert gc.ack([eid]) == 0

    def test_offsets_skipped_by_the_broker_do_not_block_commits(self):
        consumer = MagicMock()
        gc = group(consumer)
        ids = eids(
            gc.register([FakeMsg(0, 0), FakeMsg(0, 5)], "c")
        )  # 1-4 never delivered
        gc.ack(ids)
        assert self.committed(consumer) == [(0, 6)]

    def test_revoked_partitions_forget_pending_entries(self):
        consumer = MagicMock()
        gc = group(consumer)
        ids = eids(gc.register([FakeMsg(0, 0), FakeMsg(1, 0)], "c"))

        gc._on_revoke(consumer, [TopicPartition("t.log", 0)])

        assert gc.ack(ids) == 1  # partition 0 now belongs to someone else
        assert self.committed(consumer) == [(1, 1)]


class TestClaimIdle:
    def test_claims_only_entries_idle_long_enough(self, monkeypatch):
        clock = {"now": 100.0}
        monkeypatch.setattr(
            "volnux.backends.messaging.stores.kafka.time.monotonic",
            lambda: clock["now"],
        )
        gc = group()
        gc.register([FakeMsg(0, 0, b"a")], "dead-worker")
        clock["now"] += 10
        gc.register([FakeMsg(0, 1, b"b")], "live-worker")

        claimed = gc.claim_idle("me", min_idle_ms=5_000, count=10)

        assert claimed == [("0-0", b"a")]

    def test_claim_resets_idle_time_and_respects_count(self):
        gc = group()
        gc.register([FakeMsg(0, i) for i in range(3)], "dead")

        assert len(gc.claim_idle("me", 0, count=2)) == 2
        assert [eid for eid, _ in gc.claim_idle("me", 60_000, count=10)] == []


class TestPolling:
    def test_group_is_joined_lazily_on_first_fetch(self):
        consumer = MagicMock()
        consumer.consume.return_value = []
        gc = group(consumer)

        gc.register([], "c")
        gc.ack([])
        consumer.subscribe.assert_not_called()  # metadata-only use never joins

        gc.poll_batch(1, None, threading.Event(), join_timeout=0.0)
        consumer.subscribe.assert_called_once()

    def test_messages_fetched_for_a_cancelled_caller_are_rewound(self):
        consumer = MagicMock()
        cancelled = threading.Event()

        def consume(num_messages, timeout):
            cancelled.set()  # the caller leaves while the fetch is in flight
            return [FakeMsg(0, 7), FakeMsg(0, 8), FakeMsg(1, 3)]

        consumer.consume.side_effect = consume
        gc = group(consumer)

        assert gc.poll_batch(5, 0, cancelled, 1.0) == []
        seeks = {
            (c.args[0].partition, c.args[0].offset)
            for c in consumer.seek.call_args_list
        }
        assert seeks == {(0, 7), (1, 3)}  # back to the first offset of each partition

    def test_partition_eof_is_ignored_and_fatal_errors_raise(self):
        consumer = MagicMock()
        eof = MagicMock(code=lambda: KafkaError._PARTITION_EOF, fatal=lambda: False)
        consumer.consume.return_value = [FakeMsg(0, 0, error=eof), FakeMsg(0, 1)]
        gc = group(consumer)
        out = gc.poll_batch(5, 0, threading.Event(), 1.0)
        assert [m.offset() for m in out] == [1]

        fatal = MagicMock(code=lambda: 1, fatal=lambda: True)
        consumer.consume.return_value = [FakeMsg(0, 2, error=fatal)]
        with pytest.raises(KafkaException):
            gc.poll_batch(5, 0, threading.Event(), 1.0)

    def test_non_blocking_fetch_gives_up_after_the_join_timeout(self):
        consumer = MagicMock()
        consumer.consume.return_value = []
        gc = group(consumer)  # never assigned
        assert gc.poll_batch(1, None, threading.Event(), join_timeout=0.3) == []

    def test_blocking_fetch_stops_once_its_deadline_passes(self):
        consumer = MagicMock()
        consumer.consume.return_value = []
        gc = group(consumer)
        assert gc.poll_batch(1, 0.2, threading.Event(), 1.0) == []


class TestUnsupportedQueueOperations:
    @pytest.mark.asyncio
    async def test_lifo_operations_fail_loudly(self):
        class Backend(KafkaStorePushPopMixin):
            pass

        backend = Backend()
        with pytest.raises(NotImplementedError, match="FIFO"):
            await backend.push("q", object(), side=QueueSide.LEFT)
        with pytest.raises(NotImplementedError, match="FIFO"):
            await backend.pop("q", object, side=QueueSide.RIGHT)
        with pytest.raises(NotImplementedError, match="FIFO"):
            await backend.pop_many("q", object, side=QueueSide.RIGHT)

    @pytest.mark.asyncio
    async def test_argument_validation(self):
        class Backend(KafkaStorePushPopMixin):
            pass

        with pytest.raises(ValueError):
            await Backend().push("q")
        with pytest.raises(ValueError):
            await Backend().pop("q", object, timeout=-1)


@pytest.mark.asyncio
async def test_kafka_cannot_expire_a_queue():
    """Kafka keeps the default: a clear refusal, never data kept for ever."""
    from volnux.backends.messaging.base import PushPopCapabilityMixin
    from volnux.backends.messaging.stores.kafka import KafkaStorePushPopMixin

    assert KafkaStorePushPopMixin.queue_expire is PushPopCapabilityMixin.queue_expire
    with pytest.raises(NotImplementedError, match="expiring a queue"):
        await KafkaStorePushPopMixin.queue_expire(
            object.__new__(KafkaStorePushPopMixin), "q", 5
        )
