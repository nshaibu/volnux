import pytest

from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import ManualTrigger, TriggerLifecycle
from volnux.engine.workflows.trigger.triggers.base import TriggerType

pytestmark = pytest.mark.asyncio


def make(**kw):
    t = ManualTrigger("wf", **kw)
    got = []

    async def cb(activation):
        got.append(activation)

    t.set_activation_callback(cb)
    return t, got


class TestConstruction:
    async def test_type_and_defaults(self):
        t = ManualTrigger("wf")
        assert t.trigger_type is TriggerType.MANUAL
        assert t.require_confirmation is False and t.enabled is True

    async def test_confirmation_flag_is_not_persisted(self, store):
        t, _ = make(require_confirmation=True)
        await t.initialize()
        assert not hasattr(
            await TriggerStateRecord.get(t.trigger_id), "require_confirmation"
        )


class TestInvoke:
    async def test_activates_with_defaults(self, store):
        t, got = make()
        await t.run()
        await t.invoke()
        p = got[0].workflow_params
        assert p["invoked_by"] == "unknown" and p["invocation_source"] == "manual"
        assert got[0].activation_source is TriggerType.MANUAL

    async def test_user_and_source_are_forwarded_and_consumed(self, store):
        t, got = make()
        await t.run()
        await t.invoke(user="ada", source="cli")
        p = got[0].workflow_params
        assert p["invoked_by"] == "ada" and p["invocation_source"] == "cli"
        assert "user" not in p and "source" not in p

    async def test_extra_params_pass_through_and_merge(self, store):
        t, got = make(workflow_params={"fixed": 1})
        await t.run()
        await t.invoke(report="monthly")
        p = got[0].workflow_params
        assert p["fixed"] == 1 and p["report"] == "monthly"

    async def test_confirmed_never_reaches_the_workflow(self, store):
        t, got = make()
        await t.run()
        await t.invoke(confirmed=True)
        assert "confirmed" not in got[0].workflow_params

    async def test_every_invocation_counts(self, store):
        t, got = make()
        await t.run()
        for _ in range(3):
            await t.invoke()
        record = await TriggerStateRecord.get(t.trigger_id)
        assert len(got) == 3 and record.fire_count == 3
        assert record.last_fired is not None

    async def test_workable_without_run(self, store):
        t, got = make()
        await t.invoke()
        assert len(got) == 1

    async def test_no_callback_raises_and_counts_error(self, store):
        t = ManualTrigger("wf")
        with pytest.raises(RuntimeError):
            await t.invoke()
        assert t.error_count == 1


class TestConfirmation:
    async def test_required_and_missing(self, store):
        t, got = make(require_confirmation=True)
        with pytest.raises(ValueError, match="confirmation"):
            await t.invoke()
        assert got == [] and t.fire_count == 0

    @pytest.mark.parametrize("value", [False, None, 0, "", "false", "yes", 1])
    async def test_only_true_confirms(self, store, value):
        t, got = make(require_confirmation=True)
        with pytest.raises(ValueError):
            await t.invoke(confirmed=value)
        assert got == []

    async def test_confirmed_true_activates(self, store):
        t, got = make(require_confirmation=True)
        await t.invoke(confirmed=True, user="ada")
        assert len(got) == 1 and "confirmed" not in got[0].workflow_params

    async def test_not_required_ignores_confirmation(self, store):
        t, got = make()
        await t.invoke(confirmed=False)
        assert len(got) == 1


class TestDisabled:
    async def test_constructed_disabled_refuses(self, store):
        t, got = make(enabled=False)
        with pytest.raises(RuntimeError, match="disabled"):
            await t.invoke()
        assert got == []

    async def test_paused_refuses_until_resumed(self, store):
        t, got = make()
        await t.run()
        await t.pause()
        with pytest.raises(RuntimeError):
            await t.invoke()
        await t.resume()
        await t.invoke()
        assert len(got) == 1

    async def test_stopped_refuses(self, store):
        t, got = make()
        await t.run()
        await t.end()
        with pytest.raises(RuntimeError):
            await t.invoke()
        assert got == []

    async def test_refusal_is_checked_before_confirmation(self, store):
        t, _ = make(require_confirmation=True, enabled=False)
        with pytest.raises(RuntimeError):
            await t.invoke()


class TestFailures:
    async def test_callback_failure_propagates_and_counts(self, store):
        t = ManualTrigger("wf")

        async def boom(activation):
            raise ValueError("engine busy")

        t.set_activation_callback(boom)
        with pytest.raises(ValueError):
            await t.invoke()
        assert t.error_count == 1 and t.fire_count == 1


class TestLifecycle:
    async def test_run_and_end_update_the_record(self, store):
        t, _ = make()
        await t.run()
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.ACTIVE
        )
        await t.end()
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.STOPPED
        )

    async def test_start_and_stop_are_noops_and_repeatable(self):
        t = ManualTrigger("wf")
        await t.start()
        await t.start()
        await t.stop()
        await t.stop()
