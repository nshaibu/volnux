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

FORWARD_FK_BUG = (
    "KeyValueStoreIntegrationMixin resolves FK forward refs at class creation; "
    "Workflow references ApprovalChain before volnux.models.approvals is loaded"
)


def _attrib(annotation):
    return annotation.__metadata__[0]


def _pre(annotation):
    return _attrib(annotation).pre_formatter


def _post(annotation):
    return _attrib(annotation).post_formatter


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
            pytest.param(
                module,
                marks=pytest.mark.xfail(
                    strict=True, raises=AssertionError, reason=FORWARD_FK_BUG
                ),
            )
            for module in (
                "volnux.models.base",
                "volnux.models.users",
                "volnux.models.workflows",
            )
        ]
        + ["volnux.mixins.stream.queue", "volnux.engine.workflows.trigger.state"],
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
        assert _attrib(DateTimeField[config]).metadata == {
            "type": "datetime",
            "auto_now": True,
            "auto_now_add": False,
            "tz_aware": False,
            "nullable": True,
        }


class TestDateTimeFieldPreFormatter:
    def test_none_returns_none(self):
        assert _pre(DateTimeField[DTConfig("f", nullable=True)])(None, None) is None

    def test_aware_datetime(self):
        pre = _pre(DateTimeField[DTConfig("f")])
        value = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=PLUS_TWO)
        assert pre(None, value) == "2024-01-02T03:04:05+02:00"

    def test_naive_datetime_assumed_utc_when_tz_aware(self):
        pre = _pre(DateTimeField[DTConfig("f")])
        assert pre(None, datetime.datetime(2024, 1, 2, 3, 4, 5)) == (
            "2024-01-02T03:04:05+00:00"
        )

    def test_naive_datetime_kept_naive_when_not_tz_aware(self):
        pre = _pre(DateTimeField[DTConfig("f", tz_aware=False)])
        assert pre(None, datetime.datetime(2024, 1, 2, 3, 4, 5)) == (
            "2024-01-02T03:04:05"
        )

    def test_microseconds_preserved(self):
        pre = _pre(DateTimeField[DTConfig("f")])
        value = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
        assert pre(None, value) == "2024-01-02T03:04:05.123456+00:00"

    @pytest.mark.parametrize(
        "value", ["2024-01-02T03:04:05", 1700000000, datetime.date(2024, 1, 2)]
    )
    def test_rejects_non_datetime(self, value):
        pre = _pre(DateTimeField[DTConfig("f")])
        with pytest.raises(TypeError, match="Expected datetime.datetime instance"):
            pre(None, value)

    def test_auto_now_fills_none(self):
        pre = _pre(DateTimeField[DTConfig("f", auto_now=True)])
        before = datetime.datetime.now(UTC)
        result = datetime.datetime.fromisoformat(pre(None, None))
        assert result.tzinfo is not None
        assert before.date() <= result.date()

    def test_auto_now_keeps_explicit_value(self):
        pre = _pre(DateTimeField[DTConfig("f", auto_now=True)])
        value = datetime.datetime(2024, 1, 2, tzinfo=UTC)
        assert pre(None, value) == "2024-01-02T00:00:00+00:00"

    def test_auto_now_is_current_utc_time(self, non_utc_local_time):
        pre = _pre(DateTimeField[DTConfig("f", auto_now=True)])
        result = datetime.datetime.fromisoformat(pre(None, None))
        assert abs(result - datetime.datetime.now(UTC)) < datetime.timedelta(seconds=5)

    def test_auto_now_add_fills_when_instance_value_is_none(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        result = pre(SimpleNamespace(created=None), None)
        assert isinstance(datetime.datetime.fromisoformat(result), datetime.datetime)

    def test_auto_now_add_keeps_explicit_value(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        value = datetime.datetime(2024, 1, 2, tzinfo=UTC)
        assert pre(SimpleNamespace(created=None), value) == (
            "2024-01-02T00:00:00+00:00"
        )

    def test_auto_now_add_reuses_existing_naive_datetime(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        existing = datetime.datetime(2024, 1, 2, 3, 4, 5)
        assert pre(SimpleNamespace(created=existing), None) == (
            "2024-01-02T03:04:05+00:00"
        )

    def test_auto_now_add_preserves_existing_aware_instant(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        existing = datetime.datetime(2024, 1, 2, 12, tzinfo=PLUS_TWO)
        result = datetime.datetime.fromisoformat(
            pre(SimpleNamespace(created=existing), None)
        )
        assert result == existing

    def test_auto_now_add_reuses_stored_iso_string(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        stored = "2024-01-02T03:04:05+00:00"
        assert pre(SimpleNamespace(created=stored), None) == stored

    def test_auto_now_add_reuses_stored_naive_iso_string(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        assert pre(SimpleNamespace(created="2024-01-02T03:04:05"), None) == (
            "2024-01-02T03:04:05+00:00"
        )

    def test_auto_now_add_when_field_not_yet_set(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        result = datetime.datetime.fromisoformat(pre(SimpleNamespace(), None))
        assert abs(result - datetime.datetime.now(UTC)) < datetime.timedelta(seconds=5)

    def test_auto_now_add_without_instance(self):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        assert datetime.datetime.fromisoformat(pre(None, None)).tzinfo is not None

    def test_auto_now_add_is_current_utc_time(self, non_utc_local_time):
        pre = _pre(DateTimeField[DTConfig("created", auto_now_add=True)])
        result = datetime.datetime.fromisoformat(pre(SimpleNamespace(), None))
        assert abs(result - datetime.datetime.now(UTC)) < datetime.timedelta(seconds=5)

    def test_auto_now_add_discovers_field_name(self):
        annotation = DateTimeField[DTConfig(auto_now_add=True)]

        class Host:
            __annotations__ = {"created": annotation}

        host = Host()
        host.created = "2024-01-02T03:04:05+00:00"
        assert _pre(annotation)(host, None) == "2024-01-02T03:04:05+00:00"

    def test_auto_now_add_discovers_field_name_on_base_class(self):
        annotation = DateTimeField[DTConfig(auto_now_add=True)]

        class Base:
            __annotations__ = {"created": annotation}

        class Child(Base):
            __annotations__ = {"other": int}

        child = Child()
        child.created = "2024-01-02T03:04:05+00:00"
        assert _pre(annotation)(child, None) == "2024-01-02T03:04:05+00:00"

    def test_auto_now_add_unknown_field_generates_value(self):
        pre = _pre(DateTimeField[DTConfig(auto_now_add=True)])
        result = datetime.datetime.fromisoformat(pre(SimpleNamespace(x=1), None))
        assert result.tzinfo is not None


class TestDateTimeFieldPostFormatter:
    def test_none(self):
        assert _post(DateTimeField[DTConfig("f")])(None, None) is None

    def test_aware_iso_string(self):
        post = _post(DateTimeField[DTConfig("f")])
        assert post(None, "2024-01-02T03:04:05+02:00") == datetime.datetime(
            2024, 1, 2, 3, 4, 5, tzinfo=PLUS_TWO
        )

    def test_naive_iso_string_assumed_utc_when_tz_aware(self):
        post = _post(DateTimeField[DTConfig("f")])
        result = post(None, "2024-01-02T03:04:05")
        assert result == datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)

    def test_naive_iso_string_kept_naive_when_not_tz_aware(self):
        post = _post(DateTimeField[DTConfig("f", tz_aware=False)])
        result = post(None, "2024-01-02T03:04:05")
        assert result == datetime.datetime(2024, 1, 2, 3, 4, 5)
        assert result.tzinfo is None

    def test_compact_offset_string(self):
        post = _post(DateTimeField[DTConfig("f")])
        assert post(None, "2024-01-02T03:04:05.000000+0000") == datetime.datetime(
            2024, 1, 2, 3, 4, 5, tzinfo=UTC
        )

    @pytest.mark.parametrize("timestamp", [1704164645, 1704164645.0])
    def test_unix_timestamp(self, timestamp):
        post = _post(DateTimeField[DTConfig("f")])
        assert post(None, timestamp) == datetime.datetime(
            2024, 1, 2, 3, 4, 5, tzinfo=UTC
        )

    def test_invalid_string(self):
        post = _post(DateTimeField[DTConfig("f")])
        with pytest.raises(ValueError):
            post(None, "not-a-date")

    def test_wrong_type(self):
        post = _post(DateTimeField[DTConfig("f")])
        with pytest.raises(TypeError, match="Cannot deserialize datetime from list"):
            post(None, [2024])

    @pytest.mark.parametrize("tz_aware", [True, False])
    def test_round_trip(self, tz_aware):
        annotation = DateTimeField[DTConfig("f", tz_aware=tz_aware)]
        value = datetime.datetime(2024, 1, 2, 3, 4, 5, 6, tzinfo=PLUS_TWO)
        assert _post(annotation)(None, _pre(annotation)(None, value)) == value


class TestDateFieldMetadata:
    def test_metadata(self):
        config = DTConfig("f", auto_now_add=True, nullable=True)
        assert _attrib(DateField[config]).metadata == {
            "type": "date",
            "auto_now": False,
            "auto_now_add": True,
            "nullable": True,
        }


class TestDateFieldPreFormatter:
    def test_none_returns_none(self):
        assert _pre(DateField[DTConfig("f", nullable=True)])(None, None) is None

    def test_date(self):
        pre = _pre(DateField[DTConfig("f")])
        assert pre(None, datetime.date(2024, 1, 2)) == "2024-01-02"

    def test_datetime_truncated_to_date(self):
        pre = _pre(DateField[DTConfig("f")])
        assert pre(None, datetime.datetime(2024, 1, 2, 23, 59)) == "2024-01-02"

    @pytest.mark.parametrize("value", ["2024-01-02", 20240102])
    def test_rejects_non_date(self, value):
        pre = _pre(DateField[DTConfig("f")])
        with pytest.raises(TypeError, match="Expected datetime.date instance"):
            pre(None, value)

    def test_auto_now_fills_none(self):
        pre = _pre(DateField[DTConfig("f", auto_now=True)])
        assert pre(None, None) == datetime.date.today().isoformat()

    def test_auto_now_add_fills_when_instance_value_is_none(self):
        pre = _pre(DateField[DTConfig("d", auto_now_add=True)])
        assert pre(SimpleNamespace(d=None), None) == (datetime.date.today().isoformat())

    def test_auto_now_add_reuses_existing_date(self):
        pre = _pre(DateField[DTConfig("d", auto_now_add=True)])
        existing = datetime.date(2024, 1, 2)
        assert pre(SimpleNamespace(d=existing), None) == "2024-01-02"

    def test_auto_now_add_reuses_stored_iso_string(self):
        pre = _pre(DateField[DTConfig("d", auto_now_add=True)])
        assert pre(SimpleNamespace(d="2024-01-02"), None) == "2024-01-02"

    def test_auto_now_add_when_field_not_yet_set(self):
        pre = _pre(DateField[DTConfig("d", auto_now_add=True)])
        assert pre(SimpleNamespace(), None) == datetime.date.today().isoformat()

    def test_auto_now_add_discovers_field_name(self):
        annotation = DateField[DTConfig(auto_now_add=True)]

        class Host:
            __annotations__ = {"d": annotation}

        host = Host()
        host.d = "2024-01-02"
        assert _pre(annotation)(host, None) == "2024-01-02"


class TestDateFieldPostFormatter:
    def test_none(self):
        assert _post(DateField[DTConfig("f")])(None, None) is None

    def test_iso_string(self):
        post = _post(DateField[DTConfig("f")])
        assert post(None, "2024-01-02") == datetime.date(2024, 1, 2)

    def test_invalid_string(self):
        post = _post(DateField[DTConfig("f")])
        with pytest.raises(ValueError, match="Invalid date format 'nope'"):
            post(None, "nope")

    def test_wrong_type(self):
        post = _post(DateField[DTConfig("f")])
        with pytest.raises(TypeError, match="Cannot deserialize date from int"):
            post(None, 20240102)

    def test_round_trip(self):
        annotation = DateField[DTConfig("f")]
        value = datetime.date(2024, 1, 2)
        assert _post(annotation)(None, _pre(annotation)(None, value)) == value


class TestFieldsOnModel:
    def test_datetime_assignment_serializes(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig("when")]

        event = Event(when=datetime.datetime(2024, 1, 2, tzinfo=UTC))
        assert event.when == "2024-01-02T00:00:00+00:00"

    def test_date_assignment_serializes(self):
        class Invoice(BaseModel):
            due: DateField[DTConfig("due")]

        assert Invoice(due=datetime.date(2024, 1, 2)).due == "2024-01-02"

    def test_non_nullable_rejects_none(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig("when")]

        with pytest.raises(ValidationError, match="cannot be null"):
            Event(when=None)

    def test_nullable_accepts_none(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig("when", nullable=True)]

        assert Event(when=None).when is None

    def test_wrong_type_rejected(self):
        class Event(BaseModel):
            when: DateTimeField[DTConfig("when")]

        with pytest.raises(RuntimeError) as exc_info:
            Event(when="2024-01-02")
        assert isinstance(exc_info.value.__cause__, TypeError)

    def test_auto_now_on_reassignment(self):
        class Event(BaseModel):
            updated: DateTimeField[DTConfig("updated", auto_now=True)]

        event = Event(updated=None)
        assert isinstance(event.updated, str)

    @pytest.mark.parametrize("field_name", ["created", None])
    def test_auto_now_add_keeps_first_value(self, field_name):
        class Event(BaseModel):
            created: DateTimeField[DTConfig(field_name, auto_now_add=True)]

        event = Event(created=None)
        first = event.created
        assert isinstance(datetime.datetime.fromisoformat(first), datetime.datetime)

        event.created = None
        assert event.created == first

    def test_auto_now_add_date_keeps_first_value(self):
        class Invoice(BaseModel):
            issued: DateField[DTConfig(auto_now_add=True)]

        invoice = Invoice(issued=datetime.date(2024, 1, 2))
        invoice.issued = None
        assert invoice.issued == "2024-01-02"

    def test_auto_now_refreshes_on_reassignment(self):
        class Event(BaseModel):
            updated: DateTimeField[DTConfig(auto_now=True)]

        event = Event(updated=datetime.datetime(2024, 1, 2, tzinfo=UTC))
        event.updated = None
        assert event.updated != "2024-01-02T00:00:00+00:00"
