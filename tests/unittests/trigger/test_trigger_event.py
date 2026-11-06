import datetime

from volnux.engine.workflows.trigger.event import TriggerEvent

UTC = datetime.timezone.utc


def make(**kw):
    kw.setdefault("event_type", "order.created")
    kw.setdefault("source", "tests")
    return TriggerEvent(**kw)


class TestDefaults:
    def test_generated_id_is_unique(self):
        assert make().event_id != make().event_id

    def test_defaults(self):
        e = make()
        assert e.data == {} and e.metadata == {} and e.correlation_id is None

    def test_timestamp_is_a_utc_datetime(self):
        e = make()
        assert isinstance(e.timestamp, datetime.datetime)
        assert e.timestamp.utcoffset() == datetime.timedelta(0)

    def test_mutable_defaults_are_not_shared(self):
        a, b = make(), make()
        a.data["k"] = 1
        assert b.data == {}


class TestSerialisation:
    def test_dict_round_trip(self):
        e = make(data={"a": [1, 2]}, metadata={"m": 1}, correlation_id="c")
        back = TriggerEvent.from_dict(e.to_dict())
        assert back.to_dict() == e.to_dict()
        assert back.timestamp == e.timestamp

    def test_to_dict_is_json_compatible(self):
        import json

        json.dumps(make(data={"a": 1}).to_dict())

    def test_from_dict_accepts_missing_optionals(self):
        e = TriggerEvent.from_dict(
            {"event_id": "1", "event_type": "t", "source": "s", "timestamp": None}
        )
        assert e.data == {} and e.timestamp is not None

    def test_wire_state_round_trip(self):
        e = make(data={"x": 1})
        fresh = make()
        fresh.set_state(e.get_state())
        assert (fresh.event_id, fresh.data, fresh.timestamp) == (
            e.event_id,
            e.data,
            e.timestamp,
        )
