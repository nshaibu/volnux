import asyncio
import datetime
from abc import abstractmethod
from unittest.mock import AsyncMock

import pytest

from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import TriggerLifecycle
from volnux.engine.workflows.trigger.triggers.base import (
    TriggerActivation,
    TriggerBase,
    TriggerType,
    _correlation_prefix,
)

UTC = datetime.timezone.utc


class DummyTrigger(TriggerBase):
    trigger_type = TriggerType.EVENT

    def __init__(self, *args, fail_start=False, fail_stop=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_start = fail_start
        self.fail_stop = fail_stop
        self.started = 0
        self.stopped = 0

    async def start(self):
        if self.fail_start:
            raise RuntimeError("cannot start")
        self.started += 1

    async def stop(self):
        if self.fail_stop:
            raise RuntimeError("cannot stop")
        self.stopped += 1


def trigger(**kwargs):
    kwargs.setdefault("workflow_name", "wf")
    return DummyTrigger(**kwargs)


def wired(**kwargs):
    """A trigger with an activation callback that records what it receives."""
    t = trigger(**kwargs)
    received = []

    async def callback(activation):
        received.append(activation)

    t.set_activation_callback(callback)
    return t, received


async def stored(t):
    return await TriggerStateRecord.get(t.trigger_id)


class TestConstruction:
    def test_state_record(self):
        t = trigger(workflow_params={"a": 1}, metadata={"owner": "me"})
        assert t.workflow_name == "wf"
        assert t.lifecycle is TriggerLifecycle.CREATED
        assert t.enabled is True
        assert (t.fire_count, t.error_count) == (0, 0)
        assert t.last_fired is None
        assert t.workflow_params == {"a": 1} and t.metadata == {"owner": "me"}

    def test_trigger_id_is_the_state_id(self):
        t = trigger()
        assert t.trigger_id == t.state.id and t.trigger_id

    def test_each_trigger_has_its_own_id(self):
        assert trigger().trigger_id != trigger().trigger_id

    def test_can_be_built_disabled(self):
        assert trigger(enabled=False).enabled is False

    def test_defaults_are_independent_per_instance(self):
        a, b = trigger(), trigger()
        a.update_workflow_params({"x": 1})
        assert b.workflow_params == {}

    def test_updated_at_comes_from_the_field(self):
        assert isinstance(trigger().state.updated_at, datetime.datetime)


class TestTriggerTypeEnforcement:
    def test_a_concrete_subclass_needs_a_trigger_type(self):
        with pytest.raises(TypeError, match="trigger_type"):

            class Bad(TriggerBase):
                async def start(self):
                    pass

                async def stop(self):
                    pass

    def test_the_trigger_type_must_be_a_trigger_type(self):
        with pytest.raises(TypeError, match="trigger_type"):

            class Wrong(TriggerBase):
                trigger_type = "event"

                async def start(self):
                    pass

                async def stop(self):
                    pass

    def test_an_abstract_intermediate_class_needs_none(self):
        """Regression: an intermediate with an abstract method was rejected."""

        class Intermediate(TriggerBase):
            @abstractmethod
            async def poll(self): ...

        class Concrete(Intermediate):
            trigger_type = TriggerType.CONDITION

            async def poll(self):
                pass

            async def start(self):
                pass

            async def stop(self):
                pass

        assert Concrete("wf").trigger_type is TriggerType.CONDITION

    def test_a_concrete_class_under_an_intermediate_still_needs_one(self):
        class Intermediate(TriggerBase):
            @abstractmethod
            async def poll(self): ...

        with pytest.raises(TypeError, match="trigger_type"):

            class Concrete(Intermediate):
                async def poll(self):
                    pass

                async def start(self):
                    pass

                async def stop(self):
                    pass

    def test_an_intermediate_can_define_the_type_for_its_children(self):
        class Family(TriggerBase):
            trigger_type = TriggerType.SCHEDULE

            @abstractmethod
            async def tick(self): ...

        class Child(Family):
            async def tick(self):
                pass

            async def start(self):
                pass

            async def stop(self):
                pass

        assert Child("wf").trigger_type is TriggerType.SCHEDULE

    def test_the_base_cannot_be_instantiated(self):
        with pytest.raises(TypeError):
            TriggerBase("wf")


class TestActivation:
    @pytest.mark.asyncio
    async def test_calls_the_callback_with_an_activation(self, store):
        t, received = wired(workflow_params={"base": 1}, metadata={"owner": "me"})
        await t.activate(extra=2)

        [activation] = received
        assert isinstance(activation, TriggerActivation)
        assert activation.trigger_id == t.trigger_id
        assert activation.activation_source is TriggerType.EVENT
        assert activation.workflow_params == {
            "base": 1,
            "extra": 2,
            "__trigger_metadata__": {"owner": "me"},
        }
        assert activation.metadata == {"owner": "me"}

    @pytest.mark.asyncio
    async def test_activate_works_at_all(self, store):
        """Regression: activate() imported a non-existent str_to_datetime."""
        t, received = wired()
        await t.activate()
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_records_the_firing(self, store):
        t, _ = wired()
        before = datetime.datetime.now(UTC)
        await t.activate()
        after = datetime.datetime.now(UTC)

        assert t.fire_count == 1
        assert isinstance(t.last_fired, datetime.datetime)
        assert before <= t.last_fired <= after

    @pytest.mark.asyncio
    async def test_the_activation_time_is_the_recorded_firing_time(self, store):
        t, received = wired()
        await t.activate()
        assert isinstance(received[0].activated_at, datetime.datetime)
        assert received[0].activated_at == t.last_fired

    @pytest.mark.asyncio
    async def test_the_firing_is_persisted_with_real_types(self, store):
        t, _ = wired()
        await t.activate()
        await t.activate()
        saved = await stored(t)
        assert saved.fire_count == 2
        assert saved.last_fired == t.last_fired
        assert isinstance(saved.last_fired, datetime.datetime)

    @pytest.mark.asyncio
    async def test_last_fired_moves_forward(self, store):
        t, _ = wired()
        await t.activate()
        first = t.last_fired
        await asyncio.sleep(0.002)
        await t.activate()
        assert t.last_fired > first and t.fire_count == 2

    @pytest.mark.asyncio
    async def test_a_disabled_trigger_does_not_fire(self, store):
        t, received = wired(enabled=False)
        await t.activate()
        assert received == [] and t.fire_count == 0 and t.last_fired is None

    @pytest.mark.asyncio
    async def test_without_a_callback_the_error_is_counted_and_raised(self, store):
        t = trigger()
        with pytest.raises(RuntimeError, match="no activation callback"):
            await t.activate()
        assert t.error_count == 1 and t.fire_count == 0
        assert (await stored(t)).error_count == 1

    @pytest.mark.asyncio
    async def test_activation_data_overrides_workflow_params_with_a_warning(
        self, store, caplog
    ):
        t, received = wired(workflow_params={"mode": "slow", "keep": 1})
        await t.activate(mode="fast")
        assert received[0].workflow_params["mode"] == "fast"
        assert received[0].workflow_params["keep"] == 1
        assert "overlap" in caplog.text and "mode" in caplog.text

    @pytest.mark.asyncio
    async def test_a_failing_callback_is_counted_and_raised(self, store):
        t = trigger()

        async def broken(activation):
            raise ValueError("workflow rejected")

        t.set_activation_callback(broken)
        with pytest.raises(ValueError, match="workflow rejected"):
            await t.activate()

        assert t.error_count == 1
        assert t.fire_count == 1  # it did fire; the callback failed
        saved = await stored(t)
        assert (saved.fire_count, saved.error_count) == (1, 1)

    @pytest.mark.asyncio
    async def test_the_activation_source_can_be_overridden(self, store):
        class Special(DummyTrigger):
            def get_activation_source(self):
                return TriggerType.MANUAL

        t = Special("wf")
        received = []

        async def callback(activation):
            received.append(activation)

        t.set_activation_callback(callback)
        await t.activate()
        assert received[0].activation_source is TriggerType.MANUAL


class TestCallback:
    def test_must_be_a_coroutine_function(self):
        with pytest.raises(TypeError, match="coroutine"):
            trigger().set_activation_callback(lambda activation: None)

    def test_replacing_it_warns(self, caplog):
        t = trigger()
        t.set_activation_callback(AsyncMock())
        t.set_activation_callback(AsyncMock())
        assert "replacing existing activation callback" in caplog.text


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_run_starts_the_trigger_and_marks_it_active(self, store):
        t = trigger()
        await t.run()
        assert t.started == 1 and t.lifecycle is TriggerLifecycle.ACTIVE
        assert (await stored(t)).lifecycle is TriggerLifecycle.ACTIVE

    @pytest.mark.asyncio
    async def test_run_twice_is_an_error(self, store):
        t = trigger()
        await t.run()
        with pytest.raises(RuntimeError, match="already active"):
            await t.run()
        assert t.started == 1

    @pytest.mark.asyncio
    async def test_a_failed_start_is_recorded_not_left_active(self, store):
        t = trigger(fail_start=True)
        with pytest.raises(RuntimeError, match="cannot start"):
            await t.run()
        assert t.lifecycle is TriggerLifecycle.ERROR
        assert (await stored(t)).lifecycle is TriggerLifecycle.ERROR

    @pytest.mark.asyncio
    async def test_a_trigger_that_failed_to_start_can_be_run_again(self, store):
        t = trigger(fail_start=True)
        with pytest.raises(RuntimeError):
            await t.run()
        t.fail_start = False
        await t.run()
        assert t.lifecycle is TriggerLifecycle.ACTIVE and t.started == 1

    @pytest.mark.asyncio
    async def test_a_cancelled_start_is_recorded_too(self, store):
        class Slow(DummyTrigger):
            async def start(self):
                raise asyncio.CancelledError()

        t = Slow("wf")
        with pytest.raises(asyncio.CancelledError):
            await t.run()
        assert t.lifecycle is TriggerLifecycle.ERROR

    @pytest.mark.asyncio
    async def test_end_stops_the_trigger_and_marks_it_stopped(self, store):
        t = trigger()
        await t.run()
        await t.end()
        assert t.stopped == 1 and t.lifecycle is TriggerLifecycle.STOPPED
        assert (await stored(t)).enabled is False

    @pytest.mark.asyncio
    async def test_end_marks_stopped_even_if_stop_fails(self, store):
        t = trigger(fail_stop=True)
        await t.run()
        with pytest.raises(RuntimeError, match="cannot stop"):
            await t.end()
        assert t.lifecycle is TriggerLifecycle.STOPPED

    @pytest.mark.asyncio
    async def test_end_twice_is_an_error(self, store):
        t = trigger()
        await t.run()
        await t.end()
        with pytest.raises(RuntimeError, match="already stopped"):
            await t.end()

    @pytest.mark.asyncio
    async def test_a_trigger_can_be_restarted_and_fires_again(self, store):
        t, received = wired()
        await t.run()
        await t.end()
        await t.run()
        await t.activate()
        assert t.enabled is True and len(received) == 1

    @pytest.mark.asyncio
    async def test_a_trigger_built_disabled_stays_disabled_when_run(self, store):
        t, received = wired(enabled=False)
        await t.run()
        assert t.lifecycle is TriggerLifecycle.ACTIVE and t.enabled is False
        await t.activate()
        assert received == []

    @pytest.mark.asyncio
    async def test_pause_stops_firing_and_resume_restores_it(self, store):
        t, received = wired()
        await t.run()
        await t.pause()
        await t.activate()
        assert received == [] and t.lifecycle is TriggerLifecycle.PAUSED

        await t.resume()
        await t.activate()
        assert len(received) == 1 and t.lifecycle is TriggerLifecycle.ACTIVE

    @pytest.mark.asyncio
    async def test_pause_and_resume_validate_the_lifecycle(self, store):
        t = trigger()
        with pytest.raises(RuntimeError, match="expected 'active'"):
            await t.pause()
        with pytest.raises(RuntimeError, match="expected 'paused'"):
            await t.resume()

    @pytest.mark.asyncio
    async def test_the_engines_own_transitions_leave_dirty_alone(self, store):
        t = trigger()
        await t.run()
        await t.pause()
        await t.resume()
        await t.end()
        assert t.state.dirty is False and (await stored(t)).dirty is False


class TestState:
    @pytest.mark.asyncio
    async def test_initialize_persists_the_record(self, store):
        t = trigger()
        await t.initialize()
        saved = await stored(t)
        assert (
            saved.workflow_name == "wf" and saved.lifecycle is TriggerLifecycle.CREATED
        )

    @pytest.mark.asyncio
    async def test_sync_state_picks_up_a_change_made_elsewhere(self, store):
        """The CLI writes lifecycle and dirty from its own copy of the record."""
        t = trigger()
        await t.run()

        outside = await stored(t)
        outside.lifecycle = TriggerLifecycle.PAUSED
        outside.enabled = False
        outside.dirty = True
        await outside.save()

        await t.sync_state()
        assert t.lifecycle is TriggerLifecycle.PAUSED
        assert t.enabled is False and t.state.dirty is True

    @pytest.mark.asyncio
    async def test_a_synced_trigger_can_be_resumed(self, store):
        t = trigger()
        await t.run()
        outside = await stored(t)
        outside.lifecycle = TriggerLifecycle.PAUSED
        await outside.save()
        await t.sync_state()
        await t.resume()
        assert t.lifecycle is TriggerLifecycle.ACTIVE

    def test_update_workflow_params(self):
        t = trigger(workflow_params={"a": 1})
        t.update_workflow_params({"b": 2})
        assert t.workflow_params == {"a": 1, "b": 2}


class TestCorrelationId:
    @pytest.mark.asyncio
    async def test_format(self, store):
        t, _ = wired()
        parts = t.generate_correlation_id().split(":")
        assert len(parts) == 4
        assert parts[1] == "EV" and parts[2] == "0"
        assert len(parts[3]) == 4 and int(parts[3], 16) >= 0

    @pytest.mark.asyncio
    async def test_carries_the_fire_count(self, store):
        t, _ = wired()
        await t.activate()
        await t.activate()
        assert t.generate_correlation_id().split(":")[2] == "2"

    def test_ids_differ(self):
        t = trigger()
        assert len({t.generate_correlation_id() for _ in range(20)}) > 1

    @pytest.mark.parametrize(
        "node_id, expected",
        [
            ("node_deadbeef1234", "deadbeef"),
            ("node_ab", "ab"),
            ("plainnodeid", "plainnod"),
            ("noded1234567890", "noded123"),
            ("node_node_node1", "node_nod"),
        ],
    )
    def test_prefix_is_removed_as_a_prefix_not_a_character_set(self, node_id, expected):
        """Regression: ``strip("node_")`` stripped any of n, o, d, e, _ from both ends."""
        assert _correlation_prefix(node_id) == expected
