import datetime
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest
from formax import BaseModel, ValidationError

from volnux.backends.fields import DateField, DateTimeField, DTConfig
from volnux.backends.fields.datetime import parse_params

UTC = datetime.timezone.utc
PLUS_TWO = datetime.timezone(datetime.timedelta(hours=2))


def _attrib(annotation):
    return annotation.__metadata__[0]


def _pre(annotation):
    return _attrib(annotation).pre_formatter


def _post(annotation):
    return _attrib(annotation).post_formatter


def _to_state(annotation):
    return _attrib(annotation).metadata["to_state"]


def _from_state(annotation):
    return _attrib(annotation).metadata["from_state"]


@pytest.fixture
def non_utc_local_time(monkeypatch):
    """Run the test with a local timezone that is not UTC."""
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


class TestDTConfig:
    def test_defaults(self):
        config = DTConfig("created_at")
        assert config.field_name == "created_at"
        assert config.auto_now is False
        assert config.auto_now_add is False
        assert config.tz_aware is True
        assert config.nullable is False

    def test_field_name_is_optional(self):
        config = DTConfig(auto_now_add=True)
        assert config.field_name is None
        assert config.auto_now_add is True

    @pytest.mark.parametrize(
        "module",
        [
            "volnux.models.base",
            "volnux.models.users",
            "volnux.models.workflows",
            "volnux.mixins.stream.queue",
            "volnux.engine.workflows.trigger.state",
        ],
    )
    def test_modules_using_dtconfig_import(self, module):
        # Fresh interpreter: a failed import leaves partial modules in sys.modules
        result = subprocess.run(
            [sys.executable, "-c", f"import {module}"],
            capture_output=True,
            text=True,
        )
        assert "DTConfig" not in result.stderr
        assert result.returncode == 0, result.stderr.strip().splitlines()[-1]


class TestParseParams:
    def test_config_instance(self):
        config = DTConfig("f")
        assert parse_params(config, field_type="DateTimeField") is config

    def test_single_item_tuple(self):
        config = DTConfig("f")
        assert parse_params((config,), field_type="DateTimeField") is config

    def test_none(self):
        with pytest.raises(TypeError, match=r"DateTimeField\[...\] expects a DTConfig"):
            parse_params(None, field_type="DateTimeField")

    @pytest.mark.parametrize(
        "params", ["f", {"field_name": "f"}, (DTConfig("a"), DTConfig("b")), ("f",)]
    )
    def test_invalid(self, params):
        with pytest.raises(TypeError, match="expects a DTConfig instance, got"):
            parse_params(params, field_type="DateField")


@pytest.mark.parametrize("field_cls", [DateTimeField, DateField])
class TestFieldDeclaration:
    def test_cannot_instantiate(self, field_cls):
        with pytest.raises(TypeError, match="cannot be instantiated"):
            field_cls()

    def test_cannot_subclass(self, field_cls):
        with pytest.raises(TypeError, match="Cannot subclass"):
            type("Sub", (field_cls,), {})

    def test_requires_config(self, field_cls):
        with pytest.raises(TypeError, match="expects a DTConfig instance"):
            field_cls["created_at"]

    def test_auto_now_and_auto_now_add_conflict(self, field_cls):
        with pytest.raises(ValueError, match="both auto_now and auto_now_add"):
            field_cls[DTConfig("f", auto_now=True, auto_now_add=True)]

    def test_non_nullable_adds_null_validator(self, field_cls):
        assert _attrib(field_cls[DTConfig("f")]).validators

    @pytest.mark.parametrize(
        "config",
        [
            DTConfig("f", nullable=True),
            DTConfig("f", auto_now=True),
            DTConfig("f", auto_now_add=True),
        ],
    )
    def test_no_null_validator(self, field_cls, config):
        assert not _attrib(field_cls[config]).validators


class TestDateTimeFieldMetadata:
    def test_metadata(self):
        config = DTConfig("f", auto_now=True, tz_aware=False, nullable=True)
        metadata = _attrib(DateTimeField[config]).metadata
        assert {k: v for k, v in metadata.items() if not callable(v)} == {
            "type": "datetime",
            "auto_now": True,
            "auto_now_add": False,
            "tz_aware": False,
            "nullable": True,
        }

    def test_carries_state_codecs(self):
        metadata = _attrib(DateTimeField[DTConfig("f")]).metadata
        assert callable(metadata["to_state"]) and callable(metadata["from_state"])


class TestOptionalFields:
    """Auto and nullable fields may be left out when a record is created."""

    @pytest.mark.parametrize(
        "field_cls, config",
        [
            (DateTimeField, DTConfig(nullable=True)),
            (DateTimeField, DTConfig(auto_now=True)),
            (DateTimeField, DTConfig(auto_now_add=True)),
            (DateField, DTConfig(nullable=True)),
            (DateField, DTConfig(auto_now=True)),
            (DateField, DTConfig(auto_now_add=True)),
        ],
    )
    def test_has_a_default(self, field_cls, config):
        assert _attrib(field_cls[config]).has_default()

    @pytest.mark.parametrize("field_cls", [DateTimeField, DateField])
    def test_required_field_has_no_default(self, field_cls):
        assert not _attrib(field_cls[DTConfig()]).has_default()


class TestDateTimeFieldPreFormatter:
    """The pre-formatter turns an assigned value into the real ``datetime`` held."""

    def test_none_returns_none(self):
        assert _pre(DateTimeField[DTConfig("f", nullable=True)])(None, None) is None

    def test_returns_a_datetime_not_a_string(self):
        pre = _pre(DateTimeField[DTConfig("f")])
        result = pre(None, datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC))
        assert isinstance(result, datetime.datetime)

    def test_aware_datetime_is_converted_to_utc(self):
        pre = _pre(DateTimeField[DTConfig("f")])
        value = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=PLUS_TWO)
        result = pre(None, value)
        assert result == value  # the same instant
        assert result.utcoffset() == datetime.timedelta(0)
        assert result.hour == 1

    def test_naive_datetime_assumed_utc_when_tz_aware(self):
        pre = _pre(DateTimeField[DTConfig("f")])
        assert pre(None, datetime.datetime(2024, 1, 2, 3, 4, 5)) == datetime.datetime(
            2024, 1, 2, 3, 4, 5, tzinfo=UTC
        )

    def test_naive_datetime_kept_naive_when_not_tz_aware(self):
        pre = _pre(DateTimeField[DTConfig("f", tz_aware=False)])
        result = pre(None, datetime.datetime(2024, 1, 2, 3, 4, 5))
        assert result == datetime.datetime(2024, 1, 2, 3, 4, 5)
        assert result.tzinfo is None

    def test_aware_datetime_kept_as_given_when_not_tz_aware(self):
        pre = _pre(DateTimeField[DTConfig("f", tz_aware=False)])
        value = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=PLUS_TWO)
        result = pre(None, value)
        assert result.utcoffset() == datetime.timedelta(hours=2)

    def test_microseconds_preserved(self):
        pre = _pre(DateTimeField[DTConfig("f")])
        value = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
        assert pre(None, value).microsecond == 123456

    @pytest.mark.parametrize(
        "text, expected",
        [
            (
                "2024-01-02T03:04:05+00:00",
                datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
            ),
            (
                "2024-01-02T03:04:05Z",
                datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
            ),
            (
                "2024-01-02T03:04:05z",
                datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
            ),
            (
                "2024-01-02T05:04:05+02:00",
                datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
            ),
            ("2024-01-02 03:04:05", datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)),
            ("2024-01-02T03:04:05", datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)),
            ("2024-01-02", datetime.datetime(2024, 1, 2, tzinfo=UTC)),
            (
                "2024-01-02T03:04:05.123456+00:00",
                datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=UTC),
            ),
            (
                "2024-01-02T03:04:05.000000+0000",
                datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
            ),
            (
                "  2024-01-02T03:04:05Z  ",
                datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
            ),
        ],
    )
    def test_accepts_iso_strings(self, text, expected):
        assert _pre(DateTimeField[DTConfig("f")])(None, text) == expected

    @pytest.mark.parametrize("timestamp", [1704164645, 1704164645.0])
    def test_accepts_epoch_seconds(self, timestamp):
        assert _pre(DateTimeField[DTConfig("f")])(None, timestamp) == (
            datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        )

    def test_epoch_is_naive_utc_when_not_tz_aware(self):
        result = _pre(DateTimeField[DTConfig("f", tz_aware=False)])(None, 1704164645)
        assert (
            result == datetime.datetime(2024, 1, 2, 3, 4, 5) and result.tzinfo is None
        )

    def test_negative_and_zero_epochs(self):
        pre = _pre(DateTimeField[DTConfig("f")])
        assert pre(None, 0) == datetime.datetime(1970, 1, 1, tzinfo=UTC)
        assert pre(None, -1) == datetime.datetime(1969, 12, 31, 23, 59, 59, tzinfo=UTC)

    @pytest.mark.parametrize(
        "value, error, message",
        [
            (True, TypeError, "cannot hold a bool"),
            (False, TypeError, "cannot hold a bool"),
            (datetime.date(2024, 1, 2), TypeError, "got date"),
            ([2024], TypeError, "got list"),
            (b"2024-01-02", TypeError, "got bytes"),
            (None.__class__, TypeError, "got type"),
            ("", ValueError, "blank string"),
            ("   ", ValueError, "blank string"),
            ("not-a-date", ValueError, "not an ISO-8601"),
            ("2024-13-45", ValueError, "not an ISO-8601"),
            (float("nan"), ValueError, "not a valid epoch"),
            (float("inf"), ValueError, "not a valid epoch"),
            (float("-inf"), ValueError, "not a valid epoch"),
            (1700000000000, ValueError, "milliseconds"),
            (1e20, ValueError, "out of range"),
        ],
    )
    def test_rejects_with_a_clear_message(self, value, error, message):
        pre = _pre(DateTimeField[DTConfig("f")])
        with pytest.raises(error, match=message):
            pre(None, value)

    def test_error_names_the_field_and_value(self):
        pre = _pre(DateTimeField[DTConfig("f")])
        with pytest.raises(ValueError, match=r"DateTimeField cannot read 'nope'"):
            pre(None, "nope")

    def test_auto_now_fills_none(self):
        pre = _pre(DateTimeField[DTConfig("f", auto_now=True)])
        before = datetime.datetime.now(UTC)
        result = pre(None, None)
        assert isinstance(result, datetime.datetime) and result.tzinfo is not None
        assert before <= result <= datetime.datetime.now(UTC)

    def test_auto_now_keeps_explicit_value(self):
        pre = _pre(DateTimeField[DTConfig("f", auto_now=True)])
        value = datetime.datetime(2024, 1, 2, tzinfo=UTC)
        assert pre(None, value) == value

    def test_auto_now_is_current_utc_time(self, non_utc_local_time):
        pre = _pre(DateTimeField[DTConfig("f", auto_now=True)])
        assert abs(pre(None, None) - datetime.datetime.now(UTC)) < datetime.timedelta(
            seconds=5
        )

    def test_auto_now_add_fills_when_instance_value_is_none(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        assert isinstance(pre(SimpleNamespace(created=None), None), datetime.datetime)

    def test_auto_now_add_keeps_explicit_value(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        value = datetime.datetime(2024, 1, 2, tzinfo=UTC)
        assert pre(SimpleNamespace(created=None), value) == value

    def test_auto_now_add_reuses_existing_naive_datetime(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        existing = datetime.datetime(2024, 1, 2, 3, 4, 5)
        assert pre(SimpleNamespace(created=existing), None) == existing.replace(
            tzinfo=UTC
        )

    def test_auto_now_add_preserves_existing_aware_instant(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        existing = datetime.datetime(2024, 1, 2, 12, tzinfo=PLUS_TWO)
        assert pre(SimpleNamespace(created=existing), None) == existing

    def test_auto_now_add_reuses_a_stored_string(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        assert pre(SimpleNamespace(created="2024-01-02T03:04:05+00:00"), None) == (
            datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        )

    def test_auto_now_add_when_field_not_yet_set(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        assert abs(
            pre(SimpleNamespace(), None) - datetime.datetime.now(UTC)
        ) < datetime.timedelta(seconds=5)

    def test_auto_now_add_without_instance(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        assert pre(None, None).tzinfo is not None

    def test_auto_now_add_is_current_utc_time(self, non_utc_local_time):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        assert abs(
            pre(SimpleNamespace(), None) - datetime.datetime.now(UTC)
        ) < datetime.timedelta(seconds=5)

    def test_auto_now_add_discovers_field_name(self):
        annotation = DateTimeField[DTConfig(auto_now_add=True)]

        class Host:
            __annotations__ = {"created": annotation}

        host = Host()
        host.created = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert _pre(annotation)(host, None) == host.created

    def test_auto_now_add_discovers_field_name_on_base_class(self):
        annotation = DateTimeField[DTConfig(auto_now_add=True)]

        class Base:
            __annotations__ = {"created": annotation}

        class Child(Base):
            __annotations__ = {"other": int}

        child = Child()
        child.created = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert _pre(annotation)(child, None) == child.created

    def test_auto_now_add_unknown_field_generates_value(self):
        pre = _pre(DateTimeField[DTConfig(auto_now_add=True)])
        assert pre(SimpleNamespace(x=1), None).tzinfo is not None

    def test_a_wrong_field_name_in_the_config_does_not_lose_the_value(self):
        """The attribute is found by the Attrib it is bound to; the configured name
        is only a fallback, so a typo cannot silently regenerate the timestamp."""
        annotation = DateTimeField[DTConfig("typo", auto_now_add=True)]

        class Host:
            __annotations__ = {"created": annotation}

        host = Host()
        host.created = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert _pre(annotation)(host, None) == host.created


class TestDateTimeFieldStateCodecs:
    def test_to_state_is_an_iso_string(self):
        to_state = _to_state(DateTimeField[DTConfig("f")])
        value = datetime.datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=UTC)
        assert to_state(None, value) == "2024-01-02T03:04:05.000006+00:00"

    def test_to_state_none(self):
        assert (
            _to_state(DateTimeField[DTConfig("f", nullable=True)])(None, None) is None
        )

    def test_to_state_normalizes_before_writing(self):
        to_state = _to_state(DateTimeField[DTConfig("f")])
        value = datetime.datetime(2024, 1, 2, 5, 0, tzinfo=PLUS_TWO)
        assert to_state(None, value) == "2024-01-02T03:00:00+00:00"

    def test_from_state_none(self):
        assert (
            _from_state(DateTimeField[DTConfig("f", nullable=True)])(None, None) is None
        )

    @pytest.mark.parametrize(
        "raw",
        [
            "2024-01-02T03:04:05+00:00",
            "2024-01-02T03:04:05Z",
            1704164645,
            1704164645.0,
            datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
        ],
    )
    def test_from_state_reads_what_a_store_may_return(self, raw):
        assert _from_state(DateTimeField[DTConfig("f")])(None, raw) == (
            datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        )

    def test_from_state_rejects_garbage(self):
        with pytest.raises(ValueError):
            _from_state(DateTimeField[DTConfig("f")])(None, "garbage")

    @pytest.mark.parametrize("tz_aware", [True, False])
    def test_round_trip(self, tz_aware):
        annotation = DateTimeField[DTConfig("f", tz_aware=tz_aware)]
        value = _pre(annotation)(
            None, datetime.datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=PLUS_TWO)
        )
        restored = _from_state(annotation)(None, _to_state(annotation)(None, value))
        assert restored == value and restored.utcoffset() == value.utcoffset()

    def test_naive_round_trip_when_not_tz_aware(self):
        annotation = DateTimeField[DTConfig("f", tz_aware=False)]
        value = datetime.datetime(2024, 1, 2, 3, 4, 5)
        restored = _from_state(annotation)(None, _to_state(annotation)(None, value))
        assert restored == value and restored.tzinfo is None

    def test_post_formatter_reads_like_from_state(self):
        annotation = DateTimeField[DTConfig("f")]
        assert _post(annotation)(None, "2024-01-02T03:04:05Z") == (
            datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        )


class TestDateFieldMetadata:
    def test_metadata(self):
        config = DTConfig("f", auto_now_add=True, nullable=True)
        metadata = _attrib(DateField[config]).metadata
        assert {k: v for k, v in metadata.items() if not callable(v)} == {
            "type": "date",
            "auto_now": False,
            "auto_now_add": True,
            "nullable": True,
        }

    def test_carries_state_codecs(self):
        metadata = _attrib(DateField[DTConfig("f")]).metadata
        assert callable(metadata["to_state"]) and callable(metadata["from_state"])


class TestDateFieldPreFormatter:
    def test_none_returns_none(self):
        assert _pre(DateField[DTConfig("f", nullable=True)])(None, None) is None

    def test_date(self):
        pre = _pre(DateField[DTConfig("f")])
        result = pre(None, datetime.date(2024, 1, 2))
        assert result == datetime.date(2024, 1, 2)
        assert type(result) is datetime.date

    def test_datetime_truncated_to_date(self):
        pre = _pre(DateField[DTConfig("f")])
        result = pre(None, datetime.datetime(2024, 1, 2, 23, 59))
        assert result == datetime.date(2024, 1, 2) and type(result) is datetime.date

    def test_aware_datetime_is_converted_to_utc_first(self):
        """02:30+02:00 on Jan 3rd is 00:30 UTC on Jan 3rd; 00:30+02:00 is Jan 1st UTC."""
        pre = _pre(DateField[DTConfig("f")])
        assert pre(None, datetime.datetime(2024, 1, 3, 0, 30, tzinfo=PLUS_TWO)) == (
            datetime.date(2024, 1, 2)
        )
        assert pre(None, datetime.datetime(2024, 1, 3, 2, 30, tzinfo=PLUS_TWO)) == (
            datetime.date(2024, 1, 3)
        )

    @pytest.mark.parametrize(
        "text",
        [
            "2024-01-02",
            " 2024-01-02 ",
            "2024-01-02T10:00:00+00:00",
            "2024-01-02T10:00:00Z",
        ],
    )
    def test_accepts_iso_strings(self, text):
        assert _pre(DateField[DTConfig("f")])(None, text) == datetime.date(2024, 1, 2)

    def test_accepts_epoch_seconds_as_the_utc_date(self):
        assert _pre(DateField[DTConfig("f")])(None, 1704164645) == datetime.date(
            2024, 1, 2
        )

    @pytest.mark.parametrize(
        "value, error, message",
        [
            (True, TypeError, "cannot hold a bool"),
            ([2024], TypeError, "got list"),
            ("", ValueError, "expected ISO-8601"),
            ("nope", ValueError, "expected ISO-8601"),
            ("2024-13-45", ValueError, "expected ISO-8601"),
            (float("nan"), ValueError, "not a valid epoch"),
            (1700000000000, ValueError, "milliseconds"),
        ],
    )
    def test_rejects_with_a_clear_message(self, value, error, message):
        with pytest.raises(error, match=message):
            _pre(DateField[DTConfig("f")])(None, value)

    def test_auto_now_fills_none_with_the_utc_date(self, non_utc_local_time):
        pre = _pre(DateField[DTConfig("f", auto_now=True)])
        assert pre(None, None) == datetime.datetime.now(UTC).date()

    def test_auto_now_add_fills_when_instance_value_is_none(self):
        pre = _pre(DateField[DTConfig("d", auto_now_add=True)])
        assert pre(SimpleNamespace(d=None), None) == datetime.datetime.now(UTC).date()

    def test_auto_now_add_reuses_existing_date(self):
        pre = _pre(DateField[DTConfig("d", auto_now_add=True)])
        existing = datetime.date(2024, 1, 2)
        assert pre(SimpleNamespace(d=existing), None) == existing

    def test_auto_now_add_reuses_a_stored_string(self):
        pre = _pre(DateField[DTConfig("d", auto_now_add=True)])
        assert pre(SimpleNamespace(d="2024-01-02"), None) == datetime.date(2024, 1, 2)

    def test_auto_now_add_when_field_not_yet_set(self):
        pre = _pre(DateField[DTConfig("d", auto_now_add=True)])
        assert pre(SimpleNamespace(), None) == datetime.datetime.now(UTC).date()

    def test_auto_now_add_discovers_field_name(self):
        annotation = DateField[DTConfig(auto_now_add=True)]

        class Host:
            __annotations__ = {"d": annotation}

        host = Host()
        host.d = datetime.date(2024, 1, 2)
        assert _pre(annotation)(host, None) == datetime.date(2024, 1, 2)


class TestDateFieldStateCodecs:
    def test_to_state(self):
        assert _to_state(DateField[DTConfig("f")])(None, datetime.date(2024, 1, 2)) == (
            "2024-01-02"
        )

    def test_to_state_none(self):
        assert _to_state(DateField[DTConfig("f", nullable=True)])(None, None) is None

    def test_from_state_none(self):
        assert _from_state(DateField[DTConfig("f", nullable=True)])(None, None) is None

    @pytest.mark.parametrize(
        "raw",
        ["2024-01-02", "2024-01-02T10:00:00Z", datetime.date(2024, 1, 2), 1704164645],
    )
    def test_from_state_reads_what_a_store_may_return(self, raw):
        assert _from_state(DateField[DTConfig("f")])(None, raw) == datetime.date(
            2024, 1, 2
        )

    def test_from_state_rejects_garbage(self):
        with pytest.raises(ValueError, match="expected ISO-8601"):
            _from_state(DateField[DTConfig("f")])(None, "nope")

    def test_round_trip(self):
        annotation = DateField[DTConfig("f")]
        value = datetime.date(2024, 1, 2)
        assert (
            _from_state(annotation)(None, _to_state(annotation)(None, value)) == value
        )


class TestFieldsOnModel:
    def test_datetime_assignment_holds_a_datetime(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig("when")]

        event = Event(when=datetime.datetime(2024, 1, 2, tzinfo=UTC))
        assert event.when == datetime.datetime(2024, 1, 2, tzinfo=UTC)
        assert isinstance(event.when, datetime.datetime)

    def test_date_assignment_holds_a_date(self):
        class Invoice(BaseModel):
            due: DateField[DTConfig("due")]

        invoice = Invoice(due=datetime.date(2024, 1, 2))
        assert invoice.due == datetime.date(2024, 1, 2)
        assert type(invoice.due) is datetime.date

    def test_values_can_be_computed_with_directly(self):
        class Event(BaseModel):
            start: DateTimeField[DTConfig()]
            end: DateTimeField[DTConfig()]

        event = Event(start="2024-01-02T00:00:00Z", end="2024-01-02T01:30:00+00:00")
        assert event.end - event.start == datetime.timedelta(minutes=90)
        assert event.start < event.end and event.end.hour == 1

    def test_assigning_a_string_is_parsed(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig()]

        event = Event(when="2024-01-02T00:00:00Z")
        event.when = "2025-06-07T08:09:10+02:00"
        assert event.when == datetime.datetime(2025, 6, 7, 6, 9, 10, tzinfo=UTC)

    def test_non_nullable_rejects_none(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig("when")]

        with pytest.raises(ValidationError, match="cannot be null"):
            Event(when=None)

    def test_nullable_accepts_none(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig("when", nullable=True)]

        assert Event(when=None).when is None

    def test_nullable_field_can_be_left_out(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig(nullable=True)]

        assert Event().when is None

    def test_required_field_cannot_be_left_out(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig()]

        with pytest.raises((TypeError, AttributeError), match="when"):
            Event()

    def test_wrong_type_rejected_with_the_cause_attached(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig("when")]

        with pytest.raises(RuntimeError) as exc_info:
            Event(when=["2024"])
        assert isinstance(exc_info.value.__cause__, TypeError)
        assert "DateTimeField" in str(exc_info.value.__cause__)

    def test_a_bad_string_is_rejected_with_the_cause_attached(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig("when")]

        with pytest.raises(RuntimeError) as exc_info:
            Event(when="yesterday")
        assert isinstance(exc_info.value.__cause__, ValueError)

    def test_auto_now_is_stamped_on_creation(self):
        class Event(BaseModel):
            updated: DateTimeField[DTConfig("updated", auto_now=True)]

        assert isinstance(Event().updated, datetime.datetime)

    @pytest.mark.parametrize("field_name", ["created", None])
    def test_auto_now_add_keeps_first_value(self, field_name):
        class Event(BaseModel):
            created: DateTimeField[DTConfig(field_name, auto_now_add=True)]

        event = Event()
        first = event.created
        assert isinstance(first, datetime.datetime)

        event.created = None
        assert event.created == first

    def test_auto_now_add_date_keeps_first_value(self):
        class Invoice(BaseModel):
            issued: DateField[DTConfig(auto_now_add=True)]

        invoice = Invoice(issued=datetime.date(2024, 1, 2))
        invoice.issued = None
        assert invoice.issued == datetime.date(2024, 1, 2)

    def test_auto_now_refreshes_on_reassignment_of_none(self):
        class Event(BaseModel):
            updated: DateTimeField[DTConfig(auto_now=True)]

        event = Event(updated=datetime.datetime(2024, 1, 2, tzinfo=UTC))
        assert event.updated == datetime.datetime(2024, 1, 2, tzinfo=UTC)
        event.updated = None
        assert event.updated > datetime.datetime(2024, 1, 2, tzinfo=UTC)

    def test_two_models_sharing_one_declaration_do_not_share_state(self):
        shared = DateTimeField[DTConfig(auto_now_add=True)]

        class First(BaseModel):
            created: shared

        class Second(BaseModel):
            created: shared

        a = First(created=datetime.datetime(2024, 1, 2, tzinfo=UTC))
        b = Second(created=datetime.datetime(2025, 1, 2, tzinfo=UTC))
        a.created = None
        b.created = None
        assert a.created.year == 2024 and b.created.year == 2025
