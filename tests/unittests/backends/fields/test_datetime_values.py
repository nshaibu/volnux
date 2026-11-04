"""Date and datetime fields on stored records: what is held, written, read back,
refreshed on save, and matched by filters.

Records go through ``SerializingMemoryBackend``, which stores JSON like a real store,
so a value read back is a copy rebuilt from its serialised state.
"""

import datetime

import orjson
import pytest
import pytest_asyncio
from formax import BaseModel, InitStrategy

from tests.unittests.support import SerializingMemoryBackend
from volnux.backends.fields import DateField, DateTimeField, DTConfig
from volnux.backends.q_compiler import create_filter_predicate
from volnux.backends.store import SQLDialect, YoyoMigrationsMixin
from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin

UTC = datetime.timezone.utc
PLUS_TWO = datetime.timezone(datetime.timedelta(hours=2))


class Stamped(KeyValueStoreIntegrationMixin, BaseModel):
    name: str
    created_at: DateTimeField[DTConfig(auto_now_add=True)]
    updated_at: DateTimeField[DTConfig(auto_now=True)]
    seen: DateTimeField[DTConfig(nullable=True)]
    local: DateTimeField[DTConfig(tz_aware=False, nullable=True)]
    day: DateField[DTConfig(nullable=True)]
    issued: DateField[DTConfig(auto_now_add=True)]
    touched: DateField[DTConfig(auto_now=True)]

    class Config:
        init_strategy = InitStrategy.DATACLASS


@pytest.fixture
def store(monkeypatch):
    backend = SerializingMemoryBackend()
    monkeypatch.setattr(Stamped, "_backend_store", backend, raising=False)
    monkeypatch.setattr(Stamped, "_backend_initialized", True, raising=False)
    yield backend
    backend.close()


def make(name="a", **fields):
    record = Stamped(name=name, **fields)
    record.change_object_id(name)
    return record


async def stored(name="a"):
    return await Stamped.get(name)


class TestWhatIsHeld:
    def test_values_are_real_datetimes_and_dates(self):
        record = make(seen="2024-01-02T03:04:05Z", day="2024-05-06")
        assert isinstance(record.created_at, datetime.datetime)
        assert isinstance(record.updated_at, datetime.datetime)
        assert record.seen == datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert type(record.day) is datetime.date
        assert type(record.issued) is datetime.date

    def test_omitted_optional_fields_default(self):
        record = make()
        assert record.seen is None and record.local is None and record.day is None

    def test_everything_is_utc_when_tz_aware(self):
        record = make(seen=datetime.datetime(2024, 1, 2, 5, 0, tzinfo=PLUS_TWO))
        assert record.seen.utcoffset() == datetime.timedelta(0)
        assert record.seen.hour == 3

    def test_tz_aware_false_is_kept_as_given(self):
        record = make(local=datetime.datetime(2024, 1, 2, 5, 0))
        assert record.local.tzinfo is None


class TestState:
    def test_state_holds_iso_strings_for_the_store(self):
        record = make(seen="2024-01-02T03:04:05Z", day="2024-05-06")
        state = record.get_state()
        assert state["seen"] == "2024-01-02T03:04:05+00:00"
        assert state["day"] == "2024-05-06"
        assert isinstance(state["created_at"], str)
        assert state["local"] is None

    def test_state_is_json_serialisable(self):
        record = make(seen="2024-01-02T03:04:05Z", day="2024-05-06")
        decoded = orjson.loads(orjson.dumps(record.__getstate__()))
        assert decoded["seen"] == "2024-01-02T03:04:05+00:00"

    def test_the_api_payload_stays_iso_strings(self):
        """The API serialiser returns ``__getstate__()``, which goes through the codecs."""
        payload = make(seen="2024-01-02T03:04:05Z").__getstate__()
        assert payload["seen"] == "2024-01-02T03:04:05+00:00"
        assert isinstance(payload["updated_at"], str)
        assert isinstance(payload["day"], (str, type(None)))

    def test_set_state_restores_real_values(self):
        record = Stamped.__new__(Stamped)
        record.set_state(
            {
                "id": "x",
                "name": "x",
                "seen": "2024-01-02T03:04:05Z",
                "day": "2024-05-06",
                "created_at": "2024-01-01T00:00:00+00:00",
                "updated_at": 1704164645,
                "issued": "2024-01-01",
                "touched": "2024-01-01",
            }
        )
        assert record.seen == datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert record.updated_at == datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert record.day == datetime.date(2024, 5, 6)
        assert record.created_at.year == 2024

    def test_set_state_fills_fields_added_since_it_was_saved(self):
        record = Stamped.__new__(Stamped)
        record.set_state({"id": "x", "name": "x"})
        assert record.seen is None
        assert isinstance(record.created_at, datetime.datetime)  # stamped, not missing

    def test_set_state_rejects_unreadable_values(self):
        record = Stamped.__new__(Stamped)
        with pytest.raises(ValueError):
            record.set_state({"id": "x", "name": "x", "seen": "garbage"})


class TestRoundTrip:
    @pytest.mark.asyncio
    async def test_values_survive_the_store(self, store):
        original = make(
            seen=datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=PLUS_TWO),
            local=datetime.datetime(2024, 1, 2, 3, 4, 5),
            day=datetime.date(2024, 5, 6),
        )
        await original.save()
        loaded = await stored()

        assert isinstance(loaded.seen, datetime.datetime)
        assert loaded.seen == original.seen and loaded.seen.microsecond == 123456
        assert loaded.local == datetime.datetime(2024, 1, 2, 3, 4, 5)
        assert loaded.local.tzinfo is None
        assert loaded.day == datetime.date(2024, 5, 6)
        assert loaded.created_at == original.created_at

    @pytest.mark.asyncio
    async def test_the_loaded_record_is_a_copy_with_real_types(self, store):
        await make().save()
        loaded = await stored()
        assert type(loaded.day) is type(None)  # left out -> None, not "None"
        assert isinstance(loaded.updated_at, datetime.datetime)
        assert loaded.updated_at - loaded.created_at >= datetime.timedelta(0)

    @pytest.mark.asyncio
    async def test_none_stays_none(self, store):
        await make(seen=None).save()
        assert (await stored()).seen is None

    @pytest.mark.asyncio
    async def test_a_value_assigned_as_a_string_is_stored_and_loaded_as_a_datetime(
        self, store
    ):
        record = make()
        record.seen = "2025-06-07T08:09:10+02:00"
        await record.save()
        assert (await stored()).seen == datetime.datetime(
            2025, 6, 7, 6, 9, 10, tzinfo=UTC
        )


class TestAutoNowOnSave:
    @pytest.mark.asyncio
    async def test_save_refreshes_auto_now(self, store):
        record = make()
        first = record.updated_at
        await record.save()
        assert record.updated_at > first
        second = record.updated_at
        await record.save()
        assert record.updated_at > second

    @pytest.mark.asyncio
    async def test_the_refreshed_value_is_what_is_stored(self, store):
        record = make()
        await record.save()
        assert (await stored()).updated_at == record.updated_at

    @pytest.mark.asyncio
    async def test_an_explicit_value_is_respected_until_the_next_save(self, store):
        record = make(updated_at=datetime.datetime(2020, 1, 1, tzinfo=UTC))
        assert record.updated_at == datetime.datetime(2020, 1, 1, tzinfo=UTC)
        await record.save()
        assert record.updated_at.year != 2020  # saving stamps it

    @pytest.mark.asyncio
    async def test_force_insert_stamps_too(self, store):
        record = make(updated_at=datetime.datetime(2020, 1, 1, tzinfo=UTC))
        await record.save(force_insert=True)
        assert record.updated_at.year != 2020

    @pytest.mark.asyncio
    async def test_update_refreshes(self, store):
        record = make()
        await record.save()
        before = record.updated_at
        await record.update()
        assert record.updated_at > before

    @pytest.mark.asyncio
    async def test_auto_now_add_never_changes(self, store):
        record = make()
        created = record.created_at
        for _ in range(3):
            await record.save()
        assert record.created_at == created
        assert (await stored()).created_at == created

    @pytest.mark.asyncio
    async def test_auto_now_add_survives_an_assignment_of_none_after_reload(
        self, store
    ):
        await make().save()
        loaded = await stored()
        created = loaded.created_at
        loaded.created_at = None
        assert loaded.created_at == created

    @pytest.mark.asyncio
    async def test_date_fields_use_the_utc_date(self, store):
        record = make()
        await record.save()
        today = datetime.datetime.now(UTC).date()
        assert record.touched in (today, today - datetime.timedelta(days=1))
        assert record.issued == record.touched or record.issued <= record.touched

    @pytest.mark.asyncio
    async def test_a_failed_write_does_not_prevent_a_retry(self, store, monkeypatch):
        record = make()
        calls = []
        original = store.upsert

        def boom(*a, **k):
            calls.append(1)
            if len(calls) == 1:
                raise ConnectionError("down")
            return original(*a, **k)

        monkeypatch.setattr(store, "upsert", boom, raising=False)
        with pytest.raises(ConnectionError):
            await record.save()
        await record.save()
        assert (await stored()).name == "a"


class TestFilters:
    @pytest_asyncio.fixture
    async def three(self, store):
        for name, hour in (("early", 1), ("middle", 12), ("late", 23)):
            record = make(
                name,
                seen=datetime.datetime(2024, 1, 2, hour, tzinfo=UTC),
                day=datetime.date(2024, 1, hour if hour < 28 else 1),
            )
            await record.save()

    async def names(self, **filters):
        stream = await Stamped.filter(**filters)
        return sorted(r.name for r in await stream.to_list())

    @pytest.mark.asyncio
    async def test_by_datetime(self, three):
        cut = datetime.datetime(2024, 1, 2, 6, tzinfo=UTC)
        assert await self.names(seen__gt=cut) == ["late", "middle"]
        assert await self.names(seen__lt=cut) == ["early"]
        assert await self.names(
            seen__gte=datetime.datetime(2024, 1, 2, 12, tzinfo=UTC)
        ) == [
            "late",
            "middle",
        ]

    @pytest.mark.asyncio
    async def test_by_iso_string(self, three):
        assert await self.names(seen__gt="2024-01-02T06:00:00Z") == ["late", "middle"]
        assert await self.names(seen__lte="2024-01-02T01:00:00+00:00") == ["early"]

    @pytest.mark.asyncio
    async def test_by_epoch_seconds(self, three):
        cut = int(datetime.datetime(2024, 1, 2, 6, tzinfo=UTC).timestamp())
        assert await self.names(seen__gt=cut) == ["late", "middle"]

    @pytest.mark.asyncio
    async def test_exact_match_in_any_notation(self, three):
        target = datetime.datetime(2024, 1, 2, 12, tzinfo=UTC)
        assert await self.names(seen=target) == ["middle"]
        assert await self.names(seen="2024-01-02T12:00:00Z") == ["middle"]
        assert await self.names(seen="2024-01-02T14:00:00+02:00") == ["middle"]

    @pytest.mark.asyncio
    async def test_a_filter_in_another_offset_means_the_same_instant(self, three):
        # 08:00+02:00 is 06:00 UTC
        assert await self.names(seen__gt="2024-01-02T08:00:00+02:00") == [
            "late",
            "middle",
        ]

    @pytest.mark.asyncio
    async def test_naive_filter_values_are_taken_as_utc(self, three):
        assert await self.names(seen__gt=datetime.datetime(2024, 1, 2, 6)) == [
            "late",
            "middle",
        ]

    @pytest.mark.asyncio
    async def test_range_and_in(self, three):
        assert await self.names(
            seen__range=("2024-01-02T00:00:00Z", "2024-01-02T12:00:00Z")
        ) == ["early", "middle"]
        assert await self.names(
            seen__in=[
                "2024-01-02T01:00:00Z",
                datetime.datetime(2024, 1, 2, 23, tzinfo=UTC),
            ]
        ) == ["early", "late"]

    @pytest.mark.asyncio
    async def test_dates(self, three):
        assert await self.names(day=datetime.date(2024, 1, 12)) == ["middle"]
        assert await self.names(day="2024-01-12") == ["middle"]
        assert await self.names(day__gt="2024-01-05") == ["late", "middle"]

    @pytest.mark.asyncio
    async def test_isnull(self, three, store):
        await make("never").save()
        assert await self.names(seen__isnull=True) == ["never"]

    @pytest.mark.asyncio
    async def test_an_unreadable_filter_value_matches_nothing(self, three):
        assert await self.names(seen__gt="not a date") == []

    @pytest.mark.asyncio
    async def test_mixed_offsets_order_by_instant_not_by_text(self, store):
        """'13:00+02:00' is 11:00 UTC: earlier than '12:00+00:00', though its text sorts later."""
        await make("a", seen="2024-01-02T13:00:00+02:00").save()
        await make("b", seen="2024-01-02T12:00:00+00:00").save()
        assert await self.names(seen__lt="2024-01-02T12:00:00+00:00") == ["a"]


class TestPredicate:
    """The predicate used by the in-memory stores, on plain objects."""

    class Row:
        def __init__(self, at):
            self.at = at

    def test_aware_record_with_a_naive_filter_value(self):
        row = self.Row(datetime.datetime(2024, 1, 2, tzinfo=UTC))
        assert create_filter_predicate(at__gte=datetime.datetime(2024, 1, 1))(row)

    def test_naive_record_with_an_aware_filter_value(self):
        row = self.Row(datetime.datetime(2024, 1, 2))
        assert create_filter_predicate(
            at__gte=datetime.datetime(2024, 1, 1, tzinfo=UTC)
        )(row)

    def test_datetime_record_is_not_changed_by_coercion_of_non_temporal_values(self):
        row = self.Row("text")
        assert create_filter_predicate(at__startswith="te")(row)
        assert create_filter_predicate(at="text")(row)

    def test_text_lookups_on_a_datetime_are_untouched(self):
        row = self.Row(datetime.datetime(2024, 1, 2, tzinfo=UTC))
        assert create_filter_predicate(at__contains="2024-01-02")(row)


class TestPostgresColumns:
    def compile(self, model):
        class PostgresCompiler(YoyoMigrationsMixin):
            dialect = SQLDialect.POSTGRES
            _id_field_length = 36

            def resolve_physical_target(self, info):
                return info

            def map_python_type_to_sql(self, field_type):
                return "TEXT"

        graph = {}
        PostgresCompiler().compile_schema("stamped", model, graph, set())
        return " ".join(graph["stamped"].columns)

    def test_aware_datetimes_are_timestamptz_by_default(self):
        columns = self.compile(Stamped)
        assert '"seen" TIMESTAMPTZ' in columns
        assert '"updated_at" TIMESTAMPTZ' in columns

    def test_tz_unaware_datetimes_are_plain_timestamps(self):
        assert '"local" TIMESTAMP' in self.compile(Stamped)
        assert '"local" TIMESTAMPTZ' not in self.compile(Stamped)

    def test_dates_are_date_columns(self):
        assert '"day" DATE' in self.compile(Stamped)

    def test_metadata_without_tz_aware_means_aware_like_the_config_default(self):
        """DTConfig.tz_aware defaults to True; the column mapping must agree."""
        from typing import Any

        from formax import Attrib, MiniAnnotated

        class Handwritten(KeyValueStoreIntegrationMixin, BaseModel):
            at: MiniAnnotated[Any, Attrib(metadata={"type": "datetime"})]

            class Config:
                init_strategy = InitStrategy.DATACLASS

        assert '"at" TIMESTAMPTZ' in self.compile(Handwritten)
