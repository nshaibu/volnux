import datetime

import pytest

from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import TriggerLifecycle

UTC = datetime.timezone.utc


def make_record(**overrides):
    fields = dict(
        workflow_name="wf",
        lifecycle=TriggerLifecycle.CREATED,
        enabled=True,
        last_fired=None,
    )
    fields.update(overrides)
    return TriggerStateRecord(**fields)


async def reloaded(record):
    return await TriggerStateRecord.get(record.id)


class TestConstruction:
    def test_defaults(self):
        record = make_record()
        assert (record.fire_count, record.error_count, record.dirty) == (0, 0, False)
        assert record.last_fired is None
        assert record.lifecycle is TriggerLifecycle.CREATED

    def test_last_fired_can_be_left_out(self):
        record = TriggerStateRecord(
            workflow_name="wf", lifecycle=TriggerLifecycle.CREATED, enabled=True
        )
        assert record.last_fired is None

    def test_updated_at_is_a_utc_datetime_from_the_start(self):
        record = make_record()
        assert isinstance(record.updated_at, datetime.datetime)
        assert record.updated_at.utcoffset() == datetime.timedelta(0)

    def test_last_fired_is_a_datetime(self):
        moment = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert make_record(last_fired=moment).last_fired == moment

    def test_last_fired_reads_iso_strings_and_epochs(self):
        expected = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert make_record(last_fired="2024-01-02T03:04:05Z").last_fired == expected
        assert make_record(last_fired=1704164645).last_fired == expected

    def test_last_fired_is_normalized_to_utc(self):
        plus_two = datetime.timezone(datetime.timedelta(hours=2))
        record = make_record(
            last_fired=datetime.datetime(2024, 1, 2, 5, 0, tzinfo=plus_two)
        )
        assert record.last_fired.hour == 3 and record.last_fired.utcoffset() == (
            datetime.timedelta(0)
        )

    def test_a_str_formatted_datetime_is_not_what_is_held(self):
        """Regression: last_fired used to hold str(datetime) ('... 03:04:05+00:00')."""
        record = make_record(last_fired=datetime.datetime(2024, 1, 2, tzinfo=UTC))
        assert not isinstance(record.last_fired, str)


class TestState:
    def test_lifecycle_is_stored_as_its_value(self):
        state = make_record(lifecycle=TriggerLifecycle.PAUSED).get_state()
        assert state["lifecycle"] == "paused"

    def test_timestamps_are_stored_as_iso_strings(self):
        state = make_record(
            last_fired=datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        ).get_state()
        assert state["last_fired"] == "2024-01-02T03:04:05+00:00"
        assert isinstance(state["updated_at"], str)

    def test_a_record_loaded_from_plain_values_gets_real_types(self):
        """What SQLite returns: strings for the enum and timestamps, 0/1 for flags."""
        record = TriggerStateRecord.__new__(TriggerStateRecord)
        record.set_state(
            {
                "id": "t1",
                "workflow_name": "wf",
                "lifecycle": "paused",
                "enabled": 0,
                "fire_count": 3,
                "error_count": 1,
                "last_fired": "2024-01-02T03:04:05+00:00",
                "dirty": 1,
                "updated_at": "2024-01-02T03:04:05+00:00",
            }
        )
        assert record.lifecycle is TriggerLifecycle.PAUSED
        assert record.enabled is False and record.dirty is True
        assert record.last_fired == datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert (record.fire_count, record.error_count) == (3, 1)

    def test_an_unknown_lifecycle_is_rejected(self):
        record = TriggerStateRecord.__new__(TriggerStateRecord)
        with pytest.raises(ValueError):
            record.set_state({"id": "t", "workflow_name": "w", "lifecycle": "bogus"})


class TestPersistence:
    @pytest.mark.asyncio
    async def test_round_trip_keeps_types(self, store):
        moment = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
        record = make_record(
            lifecycle=TriggerLifecycle.ACTIVE, last_fired=moment, dirty=True
        )
        record.fire_count = 4
        await record.save()

        loaded = await reloaded(record)
        assert loaded.lifecycle is TriggerLifecycle.ACTIVE
        assert loaded.lifecycle == TriggerLifecycle.ACTIVE
        assert loaded.enabled is True and loaded.dirty is True
        assert loaded.last_fired == moment
        assert loaded.fire_count == 4
        assert isinstance(loaded.updated_at, datetime.datetime)

    @pytest.mark.asyncio
    async def test_a_loaded_record_can_be_transitioned(self, store):
        """Regression: a loaded lifecycle was a str, so pause() hit ``str.value``."""
        record = make_record(lifecycle=TriggerLifecycle.ACTIVE)
        await record.save()
        loaded = await reloaded(record)
        await loaded.pause()
        assert (await reloaded(record)).lifecycle is TriggerLifecycle.PAUSED

    @pytest.mark.asyncio
    async def test_saving_stamps_updated_at(self, store):
        record = make_record()
        first = record.updated_at
        await record.save()
        second = record.updated_at
        await record.save()
        assert first < second < record.updated_at

    @pytest.mark.asyncio
    async def test_the_stamp_is_what_is_stored(self, store):
        record = make_record()
        await record.save()
        assert (await reloaded(record)).updated_at == record.updated_at

    @pytest.mark.asyncio
    async def test_a_record_with_no_last_fired_stays_so(self, store):
        record = make_record()
        await record.save()
        assert (await reloaded(record)).last_fired is None


class TestTransitions:
    @pytest.mark.asyncio
    async def test_start(self, store):
        record = make_record()
        await record.start()
        assert record.lifecycle is TriggerLifecycle.ACTIVE
        assert (await reloaded(record)).lifecycle is TriggerLifecycle.ACTIVE

    @pytest.mark.asyncio
    async def test_start_twice_is_an_error(self, store):
        record = make_record()
        await record.start()
        with pytest.raises(RuntimeError, match="already active"):
            await record.start()

    @pytest.mark.asyncio
    async def test_a_trigger_built_disabled_stays_disabled_when_started(self, store):
        record = make_record(enabled=False)
        await record.start()
        assert record.lifecycle is TriggerLifecycle.ACTIVE
        assert record.enabled is False

    @pytest.mark.asyncio
    async def test_a_stopped_trigger_is_enabled_again_when_restarted(self, store):
        record = make_record()
        await record.start()
        await record.stop()
        assert record.enabled is False
        await record.start()
        assert record.enabled is True and record.lifecycle is TriggerLifecycle.ACTIVE

    @pytest.mark.asyncio
    async def test_stop(self, store):
        record = make_record(lifecycle=TriggerLifecycle.ACTIVE)
        await record.stop()
        assert record.lifecycle is TriggerLifecycle.STOPPED and record.enabled is False
        assert (await reloaded(record)).lifecycle is TriggerLifecycle.STOPPED

    @pytest.mark.asyncio
    async def test_stop_twice_is_an_error(self, store):
        record = make_record()
        await record.stop()
        with pytest.raises(RuntimeError, match="already stopped"):
            await record.stop()

    @pytest.mark.asyncio
    async def test_pause_and_resume(self, store):
        record = make_record(lifecycle=TriggerLifecycle.ACTIVE)
        await record.pause()
        assert record.lifecycle is TriggerLifecycle.PAUSED and record.enabled is False
        await record.resume()
        assert record.lifecycle is TriggerLifecycle.ACTIVE and record.enabled is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "lifecycle",
        [TriggerLifecycle.CREATED, TriggerLifecycle.PAUSED, TriggerLifecycle.STOPPED],
    )
    async def test_pause_needs_an_active_trigger(self, store, lifecycle):
        record = make_record(lifecycle=lifecycle)
        with pytest.raises(
            RuntimeError, match=f"is '{lifecycle.value}', expected 'active'"
        ):
            await record.pause()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "lifecycle", [TriggerLifecycle.CREATED, TriggerLifecycle.ACTIVE]
    )
    async def test_resume_needs_a_paused_trigger(self, store, lifecycle):
        record = make_record(lifecycle=lifecycle)
        with pytest.raises(RuntimeError, match="expected 'paused'"):
            await record.resume()

    @pytest.mark.asyncio
    async def test_mark_error(self, store):
        record = make_record(lifecycle=TriggerLifecycle.ACTIVE)
        await record.mark_error()
        assert (await reloaded(record)).lifecycle is TriggerLifecycle.ERROR

    @pytest.mark.asyncio
    async def test_an_errored_trigger_can_be_started_again(self, store):
        record = make_record()
        await record.mark_error()
        await record.start()
        assert record.lifecycle is TriggerLifecycle.ACTIVE

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "transition", ["start", "stop", "pause", "resume", "mark_error"]
    )
    async def test_the_engines_own_transitions_do_not_set_dirty(
        self, store, transition
    ):
        """``dirty`` means 'written from outside, not applied yet'."""
        lifecycle = {
            "start": TriggerLifecycle.CREATED,
            "stop": TriggerLifecycle.ACTIVE,
            "pause": TriggerLifecycle.ACTIVE,
            "resume": TriggerLifecycle.PAUSED,
            "mark_error": TriggerLifecycle.ACTIVE,
        }[transition]
        record = make_record(lifecycle=lifecycle)
        await getattr(record, transition)()
        assert record.dirty is False
        assert (await reloaded(record)).dirty is False

    @pytest.mark.asyncio
    async def test_a_change_written_from_outside_stays_dirty(self, store):
        record = make_record(lifecycle=TriggerLifecycle.ACTIVE)
        await record.save()
        outside = await reloaded(record)
        outside.lifecycle = TriggerLifecycle.PAUSED
        outside.dirty = True
        await outside.save()
        assert (await reloaded(record)).dirty is True

    @pytest.mark.asyncio
    async def test_transitions_do_not_need_a_transaction(self, store, monkeypatch):
        """Regression: saving inside transaction() failed on a fresh SQLite database."""

        def forbidden(*args, **kwargs):
            raise AssertionError("transitions must not open a transaction")

        monkeypatch.setattr(TriggerStateRecord, "transaction", forbidden)
        record = make_record()
        await record.start()
        await record.pause()
        await record.resume()
        await record.stop()

    @pytest.mark.asyncio
    async def test_every_transition_refreshes_updated_at(self, store):
        record = make_record()
        stamps = [record.updated_at]
        for step in ("start", "pause", "resume", "stop"):
            await getattr(record, step)()
            stamps.append(record.updated_at)
        assert stamps == sorted(stamps) and len(set(stamps)) == len(stamps)


class TestDirtyRecords:
    @pytest.mark.asyncio
    async def test_get_dirty_returns_only_dirty_records(self, store):
        clean = make_record()
        dirty = make_record(dirty=True)
        await clean.save()
        await dirty.save()
        found = await (await TriggerStateRecord.get_dirty()).to_list()
        assert [r.id for r in found] == [dirty.id]

    @pytest.mark.asyncio
    async def test_dirty_records_come_back_with_real_types(self, store):
        record = make_record(dirty=True, lifecycle=TriggerLifecycle.PAUSED)
        await record.save()
        [found] = await (await TriggerStateRecord.get_dirty()).to_list()
        assert found.lifecycle is TriggerLifecycle.PAUSED and found.dirty is True

    @pytest.mark.asyncio
    async def test_mark_clean(self, store):
        record = make_record(dirty=True)
        await record.save()
        await TriggerStateRecord.mark_clean(record.id)
        assert (await reloaded(record)).dirty is False

    @pytest.mark.asyncio
    async def test_mark_clean_of_an_unknown_trigger_is_logged_not_raised(
        self, store, caplog
    ):
        await TriggerStateRecord.mark_clean("missing")
        assert "not found" in caplog.text
