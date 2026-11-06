import asyncio
from pathlib import Path

import pytest
from celery import Celery

from volnux.engine.workflows.trigger import declarative
from volnux.engine.workflows.trigger.declarative import (
    TriggerDeclarationError,
    build_trigger,
    build_triggers,
    register_condition_function,
)
from volnux.engine.workflows.trigger.triggers import (
    ConditionalTrigger,
    EventTrigger,
    SchedulerTrigger,
    WebhookTrigger,
)
from volnux.engine.workflows.workflow import WorkflowConfig
from volnux.parser import pointy_parser


@pytest.fixture
def app():
    return Celery("declarative-tests")


def build(source, app=None, **kw):
    return build_triggers(pointy_parser(source + "\nA"), "wf", celery_app=app, **kw)


class TestBuild:
    def test_the_specification_example(self, store, app, tmp_path):
        f = tmp_path / "input.csv"
        triggers = build(
            '@trigger: schedule("0 6 * * MON-FRI", timezone="America/New_York")\n'
            '@trigger: webhook("/hooks/trade-alert")\n'
            f'@trigger: when file_exists("{f}")',
            app,
        )
        schedule, webhook, when = triggers
        assert isinstance(schedule, SchedulerTrigger)
        assert isinstance(webhook, WebhookTrigger)
        assert isinstance(when, ConditionalTrigger)
        assert webhook.endpoint_path == "/hooks/trade-alert"
        assert all(t.workflow_name == "wf" for t in triggers)

    def test_cron_fields_and_timezone(self, store, app):
        (t,) = build(
            '@trigger: schedule("30 9 1 * MON-FRI", timezone="Asia/Tokyo")', app
        )
        assert str(t._crontab) == "<crontab: 30 9 1 * MON-FRI (m/h/dM/MY/d)>"
        assert t.timezone == "Asia/Tokyo"
        assert str(t._crontab.now().tzinfo) == "Asia/Tokyo"

    def test_webhook_options(self, store):
        (t,) = build(
            '@trigger: webhook("/a", secret="s3", token_header="X-T", tolerance_seconds=30)'
        )
        assert t.secret_token == "s3" and t.token_header == "x-t"
        assert t.tolerance_seconds == 30.0

    def test_declared_variables_are_used(self, store):
        (t,) = build('@path = "/from/var"\n@trigger: webhook($path)')
        assert t.endpoint_path == "/from/var"

    def test_env_is_read_when_the_trigger_is_built(self, store, monkeypatch):
        monkeypatch.setenv("HOOK", "/from/env")
        (t,) = build("@trigger: webhook($env.HOOK)")
        assert t.endpoint_path == "/from/env"

    def test_poll_interval_is_passed_to_conditions(self, store):
        (t,) = build("@trigger: when $env.X", poll_interval=5)
        assert t.poll_interval == 5


class TestEvent:
    def test_built_from_types_pattern_and_name(self, store):
        (t,) = build(
            '@trigger: event("order.created", "order.updated", '
            'pattern={"status": "paid"}, name="orders")'
        )
        assert isinstance(t, EventTrigger)
        assert t.event_types == ["order.created", "order.updated"]
        assert t.event_pattern == {"status": "paid"} and t.name == "orders"

    def test_without_pattern(self, store):
        (t,) = build('@trigger: event("*")')
        assert t.event_pattern is None and t.accepts_type("anything")

    def test_pattern_values_come_from_env(self, store, monkeypatch):
        monkeypatch.setenv("REGION", "eu")
        (t,) = build('@trigger: event("a", pattern={"region": $env.REGION})')
        assert t.event_pattern == {"region": "eu"}

    def test_event_types_must_be_strings_at_run_time(self, store, monkeypatch):
        monkeypatch.delenv("KIND", raising=False)
        with pytest.raises(TriggerDeclarationError, match="event types"):
            build("@trigger: event($env.KIND)")

    def test_pattern_must_be_a_map_at_run_time(self, store, monkeypatch):
        monkeypatch.setenv("P", "oops")
        with pytest.raises(TriggerDeclarationError, match="pattern"):
            build('@trigger: event("a", pattern=$env.P)')

    @pytest.mark.asyncio
    async def test_fires_for_matching_events_only(self, store, message_bus):
        from volnux.engine.workflows.trigger.event import TriggerEvent

        (t,) = build('@trigger: event("order.created", pattern={"status": "paid"})')
        got = []

        async def cb(activation):
            got.append(activation)

        t.set_activation_callback(cb)
        await t.start()
        try:
            for kind, status in [
                ("order.created", "open"),
                ("order.deleted", "paid"),
                ("order.created", "paid"),
            ]:
                await TriggerEvent.publish(
                    TriggerEvent(event_type=kind, source="t", data={"status": status})
                )
            for _ in range(200):
                if got:
                    break
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.05)
            assert len(got) == 1
        finally:
            await t.stop()


class TestErrors:
    def test_schedule_needs_a_celery_app(self, store):
        with pytest.raises(TriggerDeclarationError, match="Celery app"):
            build('@trigger: schedule("* * * * *")')

    def test_unset_env_for_a_required_value(self, store, monkeypatch):
        monkeypatch.delenv("HOOK", raising=False)
        with pytest.raises(TriggerDeclarationError, match="path"):
            build("@trigger: webhook($env.HOOK)")

    def test_invalid_value_from_env_names_the_declaration(
        self, store, app, monkeypatch
    ):
        monkeypatch.setenv("CRON", "not a cron")
        with pytest.raises(TriggerDeclarationError, match="schedule"):
            build("@trigger: schedule($env.CRON)", app)

    def test_unknown_time_zone_at_run_time(self, store, app, monkeypatch):
        monkeypatch.setenv("TZ_NAME", "Mars/Base")
        with pytest.raises(TriggerDeclarationError, match="time zone"):
            build('@trigger: schedule("* * * * *", timezone=$env.TZ_NAME)', app)

    def test_unknown_kind(self, store):
        from volnux.parser.ast import TriggerNode

        with pytest.raises(TriggerDeclarationError, match="Unknown trigger"):
            build_trigger(TriggerNode(kind="nope"), "wf")


class TestConditions:
    @pytest.mark.asyncio
    async def test_file_exists(self, store, tmp_path):
        f = tmp_path / "in.csv"
        (t,) = build(f'@trigger: when file_exists("{f}")')
        assert t.condition_fn() is False
        f.write_text("x")
        assert t.condition_fn() is True

    def test_a_directory_is_not_a_file(self, store, tmp_path):
        (t,) = build(f'@trigger: when file_exists("{tmp_path}")')
        assert t.condition_fn() is False

    def test_env_is_re_read_on_every_evaluation(self, store, monkeypatch):
        (t,) = build("@trigger: when $env.QUEUE_DEPTH > 100")
        monkeypatch.setenv("QUEUE_DEPTH", "5")
        assert t.condition_fn() is False
        monkeypatch.setenv("QUEUE_DEPTH", "150")
        assert t.condition_fn() is True
        monkeypatch.setenv("QUEUE_DEPTH", "100.5")
        assert t.condition_fn() is True

    def test_unset_env_is_unmet_not_an_error(self, store, monkeypatch):
        monkeypatch.delenv("QUEUE_DEPTH", raising=False)
        (t,) = build("@trigger: when $env.QUEUE_DEPTH > 100")
        assert t.condition_fn() is False

    def test_boolean_env(self, store, monkeypatch):
        (t,) = build("@trigger: when $env.READY == true")
        monkeypatch.setenv("READY", "true")
        assert t.condition_fn() is True
        monkeypatch.setenv("READY", "false")
        assert t.condition_fn() is False

    def test_a_failing_condition_counts_as_unmet(self, store, monkeypatch):
        saved = dict(declarative._conditions)
        monkeypatch.setattr(declarative, "_conditions", saved)

        def boom(path):
            raise OSError("disk gone")

        register_condition_function("flaky", boom)
        try:
            (t,) = build('@trigger: when flaky("x")')
            assert t.condition_fn() is False
        finally:
            declarative.CONDITION_FUNCTIONS.pop("flaky", None)

    def test_custom_condition_function(self, store, monkeypatch):
        monkeypatch.setattr(declarative, "_conditions", dict(declarative._conditions))
        register_condition_function("always", lambda: True, arity=0)
        try:
            (t,) = build("@trigger: when always()")
            assert t.condition_fn() is True
        finally:
            declarative.CONDITION_FUNCTIONS.pop("always", None)

    @pytest.mark.asyncio
    async def test_the_trigger_fires_when_the_condition_becomes_true(
        self, store, monkeypatch
    ):
        (t,) = build("@trigger: when $env.GO == 1", poll_interval=0.01)
        got = []

        async def cb(activation):
            got.append(activation)

        t.set_activation_callback(cb)
        monkeypatch.setenv("GO", "0")
        await t.start()
        try:
            await asyncio.sleep(0.05)
            assert got == []
            monkeypatch.setenv("GO", "1")
            for _ in range(100):
                if got:
                    break
                await asyncio.sleep(0.01)
            assert len(got) == 1
        finally:
            await t.stop()


class TestSchedulerTimezone:
    def test_default_follows_the_celery_app(self, store, app):
        t = SchedulerTrigger("wf", app, hour="6")
        assert t.timezone is None

    def test_unknown_zone(self, store, app):
        with pytest.raises(ValueError, match="time zone"):
            SchedulerTrigger("wf", app, hour="6", timezone="Mars/Base")

    def test_not_with_a_prebuilt_crontab(self, store, app):
        from celery.schedules import crontab

        with pytest.raises(ValueError, match="timezone"):
            SchedulerTrigger("wf", app, schedule=crontab(), timezone="UTC")

    def test_due_time_is_evaluated_in_the_zone(self, store, app):
        t = SchedulerTrigger("wf", app, hour="6", minute="0", timezone="Asia/Tokyo")
        assert t._crontab.now().utcoffset().total_seconds() == 9 * 3600


class StubWorkflow(WorkflowConfig):
    name = "stub"

    def ready(self):
        self.registered = []

    def register_trigger(self, trigger):
        self.registered.append(trigger)

    def get_pointy_ast(self):
        return self.program


class TestWorkflowConfig:
    def workflow(self, source, tmp_path):
        wf = StubWorkflow(Path(tmp_path))
        wf.program = pointy_parser(source + "\nA")
        return wf

    def test_registers_every_declared_trigger_in_order(self, store, app, tmp_path):
        wf = self.workflow(
            '@trigger: webhook("/a")\n@trigger: schedule("* * * * *")', tmp_path
        )
        built = wf.register_declared_triggers(celery_app=app)
        assert wf.registered == built
        assert [t.trigger_type.value for t in built] == ["webhook", "schedule"]

    def test_nothing_is_registered_when_one_declaration_fails(self, store, tmp_path):
        wf = self.workflow(
            '@trigger: webhook("/a")\n@trigger: schedule("* * * * *")', tmp_path
        )
        with pytest.raises(TriggerDeclarationError):
            wf.register_declared_triggers()  # no celery app for the schedule
        assert wf.registered == []

    def test_a_workflow_without_triggers(self, store, tmp_path):
        assert self.workflow("", tmp_path).register_declared_triggers() == []
