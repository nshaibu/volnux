import asyncio

import pytest
from celery import Celery
from celery.schedules import crontab

from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import SchedulerTrigger, TriggerLifecycle
from volnux.engine.workflows.trigger.triggers.base import TriggerType


from volnux.engine.workflows.trigger.event import TriggerEvent
from volnux.engine.workflows.trigger.triggers.schedule import (
    FIRE_TASK_NAME,
    SCHEDULE_FIRED_EVENT,
    fire_schedule,
    register_schedule_task,
)


@pytest.fixture
def app():
    return Celery("scheduler-tests")


def make(app, **kw):
    t = SchedulerTrigger("wf", app, **kw)
    got = []

    async def cb(activation):
        got.append(activation)

    t.set_activation_callback(cb)
    return t, got


def run_task(app, trigger):
    """What a Celery worker does when beat dispatches the trigger's entry."""
    entry = app.conf.beat_schedule[trigger._beat_key]
    app.tasks[entry["task"]](*entry["args"], **entry["kwargs"])


async def settle(predicate, timeout=2.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not predicate():
        if loop.time() > end:
            raise AssertionError("not reached in time")
        await asyncio.sleep(0.005)


async def quiet():
    await asyncio.sleep(0.05)


class TestConstruction:
    def test_type_and_default_schedule_is_every_minute(self, app):
        t = SchedulerTrigger("wf", app)
        assert t.trigger_type is TriggerType.SCHEDULE
        assert t._crontab == crontab()

    def test_fields_build_the_crontab(self, app):
        t = SchedulerTrigger("wf", app, hour="9", minute="30", day_of_week="mon-fri")
        assert t._crontab == crontab(hour="9", minute="30", day_of_week="mon-fri")

    def test_prebuilt_schedule_is_used(self, app):
        schedule = crontab(hour="2", minute="0")
        assert SchedulerTrigger("wf", app, schedule=schedule)._crontab is schedule

    def test_schedule_and_fields_together_are_refused(self, app):
        with pytest.raises(ValueError, match="not both"):
            SchedulerTrigger("wf", app, schedule=crontab(), hour="9")

    def test_celery_app_is_required(self):
        with pytest.raises(ValueError, match="celery_app"):
            SchedulerTrigger("wf", None)

    @pytest.mark.parametrize("bad", ["* * * * *", 60, object()])
    def test_schedule_must_be_a_crontab(self, app, bad):
        with pytest.raises(TypeError, match="crontab"):
            SchedulerTrigger("wf", app, schedule=bad)

    def test_invalid_cron_field_is_refused(self, app):
        with pytest.raises(Exception):
            SchedulerTrigger("wf", app, minute="99")

    def test_beat_key_comes_from_the_trigger_id(self, app):
        t = SchedulerTrigger("wf", app)
        assert t._make_beat_key() == f"volnux.schedule_trigger.{t.trigger_id}"
        assert t._beat_key is None

    def test_building_a_trigger_registers_the_shared_task(self, app):
        assert FIRE_TASK_NAME not in app.tasks
        SchedulerTrigger("wf", app)
        assert FIRE_TASK_NAME in app.tasks

    def test_registering_twice_keeps_one_task(self, app):
        register_schedule_task(app)
        task = app.tasks[FIRE_TASK_NAME]
        register_schedule_task(app)
        SchedulerTrigger("wf", app)
        assert app.tasks[FIRE_TASK_NAME] is task


@pytest.mark.asyncio
class TestStartStop:
    async def test_start_adds_a_beat_entry_for_the_shared_task(
        self, store, message_bus, app
    ):
        t, _ = make(app, hour="9", minute="30")
        await t.start()
        entry = app.conf.beat_schedule[t._make_beat_key()]
        assert entry["task"] == FIRE_TASK_NAME
        assert entry["schedule"] is t._crontab
        assert entry["args"] == (t.trigger_id, str(t._crontab))
        assert entry["kwargs"] == {}
        assert entry["options"]["expires"] == 86400
        assert t.subscribed
        await t.stop()

    async def test_the_entry_is_plain_data_a_beat_process_can_use(
        self, store, message_bus, app
    ):
        # beat only needs the task name and picklable arguments, not this object
        import pickle

        t, _ = make(app)
        await t.start()
        entry = app.conf.beat_schedule[t._beat_key]
        pickle.dumps((entry["task"], entry["args"], entry["kwargs"]))
        await t.stop()

    async def test_start_is_idempotent(self, store, message_bus, app):
        t, _ = make(app)
        await t.start()
        entry = app.conf.beat_schedule[t._beat_key]
        task = t._subscription_task
        await t.start()
        assert app.conf.beat_schedule[t._beat_key] is entry
        assert len(app.conf.beat_schedule) == 1
        assert t._subscription_task is task
        await t.stop()

    async def test_stop_removes_the_entry_and_stops_listening(
        self, store, message_bus, app
    ):
        t, _ = make(app)
        await t.start()
        key = t._beat_key
        await t.stop()
        assert key not in app.conf.beat_schedule
        assert t._beat_key is None and not t.subscribed

    async def test_the_shared_task_outlives_a_single_trigger(
        self, store, message_bus, app
    ):
        t, _ = make(app)
        await t.start()
        await t.stop()
        assert FIRE_TASK_NAME in app.tasks

    async def test_stop_before_start_and_twice_is_safe(self, store, message_bus, app):
        t, _ = make(app)
        await t.stop()
        await t.start()
        await t.stop()
        await t.stop()

    async def test_restart_works(self, store, message_bus, app):
        t, got = make(app)
        await t.start()
        await t.stop()
        await t.start()
        await asyncio.to_thread(run_task, app, t)
        await settle(lambda: got)
        await t.stop()

    async def test_stop_after_a_repeated_start_still_cleans_up(
        self, store, message_bus, app
    ):
        t, _ = make(app)
        await t.start()
        t._beat_key = None  # e.g. the object was rebuilt around the same entry
        await t.start()
        await t.stop()
        assert app.conf.beat_schedule == {}

    async def test_triggers_are_independent(self, store, message_bus, app):
        a, _ = make(app)
        b, _ = make(app)
        await a.start()
        await b.start()
        assert len(app.conf.beat_schedule) == 2
        await a.stop()
        assert list(app.conf.beat_schedule) == [b._beat_key]
        await b.stop()

    async def test_run_and_end_update_the_record(self, store, message_bus, app):
        t, _ = make(app)
        await t.run()
        assert t._beat_key in app.conf.beat_schedule
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.ACTIVE
        )
        await t.end()
        assert app.conf.beat_schedule == {}
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.STOPPED
        )


@pytest.mark.asyncio
class TestWorkerSide:
    """The worker holds no trigger: it only says that a schedule is due."""

    async def test_the_task_publishes_a_schedule_fired_event(self, message_bus, app):
        seen = []
        listener = asyncio.create_task(TriggerEvent.subscribe(seen.append))
        await asyncio.sleep(0.05)
        register_schedule_task(app)
        await asyncio.to_thread(app.tasks[FIRE_TASK_NAME], "trigger-1", "* * * * *")
        await settle(lambda: seen)
        listener.cancel()
        event = seen[0]
        assert event.event_type == SCHEDULE_FIRED_EVENT
        assert event.data["trigger_id"] == "trigger-1"
        assert event.data["crontab"] == "* * * * *"
        assert event.data["scheduled_at"]

    async def test_a_separate_worker_app_drives_the_engines_trigger(
        self, store, message_bus, app
    ):
        # The engine and the worker are different processes with different Celery
        # apps; the worker never builds a trigger. Only beat's entry crosses over.
        t, got = make(app)
        await t.start()
        entry = app.conf.beat_schedule[t._beat_key]

        worker_app = Celery("the-worker")
        register_schedule_task(worker_app)
        await asyncio.to_thread(worker_app.tasks[entry["task"]], *entry["args"])

        await settle(lambda: got)
        await t.stop()

    async def test_it_works_without_any_trigger_object(self, message_bus):
        # a worker process that only imported the module
        await asyncio.to_thread(fire_schedule, "nobody", "")

    async def test_publishing_from_a_running_loop_works(self, message_bus):
        fire_schedule("t", "")  # an eager task on the event loop's own thread


@pytest.mark.asyncio
class TestFiring:
    async def test_the_event_activates_the_trigger(self, store, message_bus, app):
        t, got = make(app, workflow_params={"fixed": 1})
        await t.start()
        await asyncio.to_thread(run_task, app, t)
        await settle(lambda: got)
        params = got[0].workflow_params
        assert params["fixed"] == 1
        assert params["scheduled_at"] and params["crontab"]
        assert params["correlation_id"].count(":") == 3
        assert got[0].activation_source is TriggerType.SCHEDULE
        await t.stop()

    async def test_firing_counts_and_persists(self, store, message_bus, app):
        t, got = make(app)
        await t.start()
        await asyncio.to_thread(run_task, app, t)
        await asyncio.to_thread(run_task, app, t)
        await settle(lambda: len(got) == 2)
        record = await TriggerStateRecord.get(t.trigger_id)
        assert record.fire_count == 2 and record.last_fired is not None
        await t.stop()

    async def test_only_the_due_trigger_activates(self, store, message_bus, app):
        (a, got_a), (b, got_b) = make(app), make(app)
        await a.start()
        await b.start()
        await asyncio.to_thread(run_task, app, a)
        await settle(lambda: got_a)
        await quiet()
        assert got_b == []
        await a.stop()
        await b.stop()

    async def test_other_events_are_ignored(self, store, message_bus, app):
        t, got = make(app)
        await t.start()
        await TriggerEvent.publish(TriggerEvent(event_type="order.created", source="x"))
        await TriggerEvent.publish(
            TriggerEvent(
                event_type=SCHEDULE_FIRED_EVENT,
                source="x",
                data={"trigger_id": "other"},
            )
        )
        await quiet()
        assert got == []
        await t.stop()

    async def test_no_activation_after_stop(self, store, message_bus, app):
        t, got = make(app)
        await t.start()
        entry = dict(app.conf.beat_schedule[t._beat_key])
        await t.stop()
        await asyncio.to_thread(app.tasks[entry["task"]], *entry["args"])
        await quiet()
        assert got == []

    async def test_disabled_trigger_does_not_activate(self, store, message_bus, app):
        t, got = make(app, enabled=False)
        await t.start()
        await asyncio.to_thread(run_task, app, t)
        await quiet()
        assert got == []
        await t.stop()

    async def test_activation_failure_is_counted_and_listening_continues(
        self, store, message_bus, app
    ):
        t = SchedulerTrigger("wf", app)
        calls = []

        async def flaky(activation):
            calls.append(1)
            if len(calls) == 1:
                raise ValueError("engine busy")

        t.set_activation_callback(flaky)
        await t.start()
        await asyncio.to_thread(run_task, app, t)
        await asyncio.to_thread(run_task, app, t)
        await settle(lambda: len(calls) == 2)
        assert t.error_count == 1
        await t.stop()

    async def test_no_callback_counts_the_error(self, store, message_bus, app):
        t = SchedulerTrigger("wf", app)
        await t.start()
        await asyncio.to_thread(run_task, app, t)
        await settle(lambda: t.error_count == 1)
        await t.stop()

    async def test_a_dead_subscription_marks_the_trigger_error(
        self, store, message_bus, app, monkeypatch
    ):
        async def broken(*args, **kwargs):
            raise ConnectionError("backend gone")

        monkeypatch.setattr(TriggerEvent, "subscribe", broken)
        t, _ = make(app)
        await t.run()
        await settle(lambda: t._subscription_task.done())
        assert t.lifecycle is TriggerLifecycle.ERROR
        await t.stop()


class TestExpiry:
    @pytest.mark.parametrize(
        "fields, seconds",
        [
            ({}, 60),
            ({"minute": "*/5"}, 300),
            ({"minute": "0"}, 3600),
            ({"hour": "9", "minute": "30"}, 86400),
            ({"hour": "9,17", "minute": "0"}, 16 * 3600),
            ({"hour": "9", "minute": "30", "day_of_week": "mon-fri"}, 3 * 86400),
            ({"day_of_month": "1", "hour": "0", "minute": "0"}, 31 * 86400),
        ],
    )
    def test_expiry_is_the_longest_gap_between_runs(self, app, fields, seconds):
        t = SchedulerTrigger("wf", app, **fields)
        got = t._crontab_period_seconds()
        # month-start gaps vary (28-31 days): accept the longest one seen
        assert got == seconds or (
            seconds == 31 * 86400 and 28 * 86400 <= got <= seconds
        )

    def test_never_below_one_minute(self, app):
        assert SchedulerTrigger("wf", app)._crontab_period_seconds() >= 60

    def test_unmeasurable_schedule_falls_back_to_the_floor(self, app):
        t = SchedulerTrigger("wf", app)

        class Broken:
            def now(self):
                raise RuntimeError("no clock")

        t._crontab = Broken()
        assert t._crontab_period_seconds() == 60
