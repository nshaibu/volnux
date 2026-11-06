"""Checkpoint a real event, then rebuild it with EventRehydrator."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from volnux.execution.rehydrator.event.rehydrator import EventRehydrator
from volnux.execution.rehydrator.event.snapshot import EventPhase
from volnux.result import EventResult, ResultSet

from .providers import Counter, CounterProvider
from .sample_events import RoundTripEvent, SeenArgsEvent

PROVIDER = "tests.unittests.event.providers.CounterProvider"


@pytest.fixture
def restored_context():
    context = MagicMock(name="restored_context")
    context.state_id = "ctx-1"
    with patch(
        "volnux.execution.context.ExecutionContext.get",
        new=AsyncMock(return_value=context),
    ):
        yield context


@pytest.fixture
def original(ctx):
    event = RoundTripEvent(ctx, "task-9", sequence_number=4)
    event._phase = EventPhase.PROCESSING
    event._retry_count = 2
    event.exec_status = True
    event.exec_result = {"rows": 10}
    event.cursor = 41  # a plain, serialisable attribute
    return event


async def roundtrip(event, **kwargs):
    snapshot = await event.create_snapshot()
    return snapshot, await EventRehydrator().rehydrate(snapshot, **kwargs)


class TestRehydrate:
    @pytest.mark.asyncio
    async def test_restores_identity_and_progress(self, original, restored_context):
        snapshot, event = await roundtrip(original)
        assert type(event) is RoundTripEvent
        assert event._task_id == "task-9" and event._sequence_number == 4
        assert event.get_phase() == EventPhase.PROCESSING
        assert event._retry_count == 2

    @pytest.mark.asyncio
    async def test_resolves_the_execution_context(self, original, restored_context):
        """The context lookup is a coroutine; the event must receive the context itself."""
        _, event = await roundtrip(original)
        assert event._execution_context is restored_context

    @pytest.mark.asyncio
    async def test_restores_the_execution_result_on_the_public_attributes(
        self, original, restored_context
    ):
        """Regression: the rehydrator used to set ``_exec_status``/``_exec_result``."""
        _, event = await roundtrip(original)
        assert event.exec_status is True
        assert event.exec_result == {"rows": 10}
        assert not hasattr(event, "_exec_status")

    @pytest.mark.asyncio
    async def test_restores_user_attributes(self, original, restored_context):
        _, event = await roundtrip(original)
        assert event.cursor == 41

    @pytest.mark.asyncio
    async def test_snapshot_holds_the_real_call_arguments(self, ctx):
        event = RoundTripEvent(ctx, "t")
        await event(1, 2, mode="fast")
        snapshot = await event.create_snapshot()
        assert snapshot.call_args == {"args": [1, 2], "kwargs": {"mode": "fast"}}

    @pytest.mark.asyncio
    async def test_flow_supplied_keyword_arguments_are_snapshotted(self, ctx):
        """The flows set ``_call_args`` (a kwargs dict) before calling the event."""
        event = RoundTripEvent(ctx, "t")
        event._call_args = {"limit": 5, "name": "x"}
        snapshot = await event.create_snapshot()
        assert snapshot.call_args == {"args": [], "kwargs": {"limit": 5, "name": "x"}}

    @pytest.mark.asyncio
    async def test_restores_call_arguments_in_their_live_shape(
        self, original, restored_context
    ):
        original._call_positional = (1, 2)
        original._call_args = {"mode": "fast"}
        _, event = await roundtrip(original)
        assert event._call_positional == (1, 2)
        assert event._call_args == {"mode": "fast"}
        assert event.get_call_args() == {"mode": "fast"}

    @pytest.mark.asyncio
    async def test_a_rehydrated_event_can_be_called_with_its_original_arguments(
        self, ctx, restored_context
    ):
        seen = {}

        async def process(self, *args, **kwargs):
            seen["args"], seen["kwargs"] = args, kwargs
            return True, "done"

        event = SeenArgsEvent(ctx, "t")
        SeenArgsEvent.process = process
        event._phase = EventPhase.COMMUNICATING
        event._call_positional = (7,)
        event._call_args = {"k": "v"}
        rebuilt = await EventRehydrator().rehydrate(await event.create_snapshot())
        result = await rebuilt(*rebuilt._call_positional, **rebuilt.get_call_args())
        assert result.success
        assert seen == {"args": (7,), "kwargs": {"k": "v"}}

    @pytest.mark.asyncio
    async def test_constructor_accepts_the_serialised_init_args(
        self, original, restored_context
    ):
        """Regression: the deserializer's extra ``kwargs`` key broke ``EventBase.__init__``."""
        snapshot = await original.create_snapshot()
        init_kwargs = EventRehydrator().deserializer.deserialize_init_args(
            snapshot.init_args
        )
        init_kwargs = await EventRehydrator._resolve_awaitables(init_kwargs)
        assert "kwargs" in init_kwargs  # the serialised form carries it...
        event = await EventRehydrator().rehydrate(snapshot)  # ...rehydrate filters it
        assert isinstance(event, RoundTripEvent)

    @pytest.mark.asyncio
    async def test_collaborators_can_be_supplied(
        self, original, restored_context, checkpoint_manager, channel
    ):
        _, event = await roundtrip(
            original, checkpoint_manager=checkpoint_manager, command_channel=channel
        )
        assert event.checkpoint_manager is checkpoint_manager
        assert event._command_channel is channel

    @pytest.mark.asyncio
    async def test_without_collaborators_the_event_has_none(
        self, original, restored_context
    ):
        _, event = await roundtrip(original)
        assert event.checkpoint_manager is None and event._command_channel is None


class TestRetryPolicy:
    @pytest.mark.asyncio
    async def test_snapshot_attempts_are_applied(self, original, restored_context):
        snapshot = await original.create_snapshot()
        snapshot.max_retry_attempts = 9
        event = await EventRehydrator().rehydrate(snapshot)
        assert event.retry_policy.max_attempts == 9

    @pytest.mark.asyncio
    async def test_the_shared_class_policy_is_not_mutated(
        self, original, restored_context
    ):
        """Regression: the class-level policy used to be edited in place."""
        shared = RoundTripEvent.retry_policy
        before = shared.max_attempts
        snapshot = await original.create_snapshot()
        snapshot.max_retry_attempts = 9
        event = await EventRehydrator().rehydrate(snapshot)
        assert RoundTripEvent.retry_policy is shared and shared.max_attempts == before
        assert event.retry_policy is not shared

    @pytest.mark.asyncio
    async def test_event_without_a_policy(self, ctx, restored_context):
        from tests.unittests.event.sample_events import RoundTripEvent as Cls

        saved = Cls.retry_policy
        Cls.retry_policy = None
        try:
            event = Cls(ctx, "t")
            snapshot = await event.create_snapshot()
            rebuilt = await EventRehydrator().rehydrate(snapshot)
            assert rebuilt.retry_policy is None
        finally:
            Cls.retry_policy = saved


class TestResources:
    @pytest.mark.asyncio
    async def test_saved_resource_state_is_kept_for_lazy_restore(
        self, original, restored_context
    ):
        await original._register_resource("counter", Counter(7), PROVIDER)
        snapshot, event = await roundtrip(original)
        assert event._external_resources == snapshot.external_resources
        assert event._external_resources["counter"]["data"] == {"value": 7}
        assert event._resource_instances == {}  # nothing live until acquired

    @pytest.mark.asyncio
    async def test_acquire_resource_restores_from_the_saved_state(
        self, original, restored_context
    ):
        await original._register_resource("counter", Counter(7), PROVIDER)
        _, event = await roundtrip(original)
        try:
            counter = await event.acquire_resource("counter", PROVIDER, {})
            assert isinstance(counter, Counter) and counter.value == 7
        finally:
            if event._resource_monitor.is_started():
                await event._resource_monitor.stop()

    @pytest.mark.asyncio
    async def test_rehydrated_event_can_complete_and_clean_up(
        self, original, restored_context
    ):
        """_completed() reads the saved states, so they must still be ResourceState dicts."""
        await original._register_resource("counter", Counter(7), PROVIDER)
        _, event = await roundtrip(original)
        await event._completed()  # must not raise
        assert event.get_phase() == EventPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_provider_restore_uses_restore_state(self):
        from volnux.execution.rehydrator.event.deserializer import StateDeserializer

        restored = StateDeserializer.deserialize_external_resources(
            {
                "c": {
                    "resource_name": "c",
                    "data": {"value": 5},
                    "provider_path": PROVIDER,
                }
            }
        )
        assert restored["c"].value == 5

    @pytest.mark.asyncio
    async def test_resource_without_provider_path_is_skipped(self):
        from volnux.execution.rehydrator.event.deserializer import StateDeserializer

        assert (
            StateDeserializer.deserialize_external_resources(
                {"c": {"resource_name": "c", "data": {}}}
            )
            == {}
        )


class TestSerialisationEdges:
    @pytest.mark.asyncio
    async def test_resumed_event_finishes_the_remaining_phases(
        self, ctx, restored_context
    ):
        event = RoundTripEvent(ctx, "t1")
        event._phase = EventPhase.POST_PROCESS
        event.exec_status, event.exec_result = True, {"saved": True}
        snapshot = await event.create_snapshot()

        rebuilt = await EventRehydrator().rehydrate(snapshot)
        result = await rebuilt.steps_runner()
        assert result.success and result.content == {"saved": True}
        assert rebuilt.get_phase() == EventPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_snapshot_before_any_call_arguments_are_set(self, ctx):
        snapshot = await RoundTripEvent(ctx, "t").create_snapshot()
        assert snapshot.call_args == {"args": [], "kwargs": {}}

    @pytest.mark.asyncio
    async def test_snapshot_never_contains_collaborators(
        self, ctx, checkpoint_manager, channel
    ):
        event = RoundTripEvent(
            ctx, "t", checkpoint_manager=checkpoint_manager, command_channel=channel
        )
        snapshot = await event.create_snapshot()
        assert "checkpoint_manager" not in snapshot.attribs
        assert "command_channel" not in snapshot.attribs


def stored(*results):
    """Make EventResult.get/save behave like a store holding ``results``."""
    by_id = {r.id: r for r in results}
    get = AsyncMock(side_effect=lambda record_id: by_id[record_id])
    save = AsyncMock()
    return by_id, get, save


def make_result(content="c", persisted=True, **kw):
    result = EventResult(
        error=False,
        event_name="Up",
        content=content,
        workflow_id=None,
        task_id=kw.get("task_id", "up"),
    )
    result.is_persisted = persisted
    return result


class TestEventResultReferences:
    """EventResult is persisted, so a snapshot refers to it by id."""

    @pytest.mark.asyncio
    async def test_previous_results_are_stored_as_ids(self, ctx):
        first, second = make_result(1), make_result(2)
        event = RoundTripEvent(ctx, "t", previous_result=[first, second])
        snapshot = await event.create_snapshot()
        assert snapshot.init_args["previous_result"] == [first.id, second.id]

    @pytest.mark.asyncio
    async def test_unsaved_results_are_persisted_before_they_are_referenced(
        self, ctx, monkeypatch
    ):
        saved, fresh = make_result(1), make_result(2, persisted=False)
        _, _, save = stored(saved, fresh)
        monkeypatch.setattr(EventResult, "save", save)
        event = RoundTripEvent(ctx, "t", previous_result=[saved, fresh])
        await event.create_snapshot()
        assert save.await_count == 1  # only the unsaved one

    @pytest.mark.asyncio
    async def test_previous_results_are_loaded_back_into_a_result_set(
        self, ctx, restored_context, monkeypatch
    ):
        first, second = make_result(1), make_result(2)
        _, get, _ = stored(first, second)
        monkeypatch.setattr(EventResult, "get", classmethod(lambda cls, i: get(i)))
        event = RoundTripEvent(ctx, "t", previous_result=[first, second])
        rebuilt = await EventRehydrator().rehydrate(await event.create_snapshot())
        assert isinstance(rebuilt.previous_result, ResultSet)
        assert set(rebuilt.previous_result) == {first, second}

    @pytest.mark.asyncio
    async def test_no_previous_results_leaves_the_default(self, ctx, restored_context):
        from volnux.constants import EMPTY

        rebuilt = await EventRehydrator().rehydrate(
            await RoundTripEvent(ctx, "t").create_snapshot()
        )
        assert rebuilt.previous_result is EMPTY

    @pytest.mark.asyncio
    async def test_a_response_is_injected_into_previous_results_on_rehydration(
        self, ctx, restored_context, monkeypatch
    ):
        """communicate() suspends instead of returning; the answer arrives here."""
        earlier = make_result("upstream")
        _, get, _ = stored(earlier)
        monkeypatch.setattr(EventResult, "get", classmethod(lambda cls, i: get(i)))
        response = make_result(
            {"decision": "approve", "type": "human_response"}, task_id="t"
        )
        event = RoundTripEvent(ctx, "t", previous_result=[earlier])
        rebuilt = await EventRehydrator().rehydrate(
            await event.create_snapshot(), previous_results=[response]
        )
        assert set(rebuilt.previous_result) == {earlier, response}

    @pytest.mark.asyncio
    async def test_a_response_alone_creates_the_previous_result_set(
        self, ctx, restored_context
    ):
        response = make_result({"decision": "reject"})
        rebuilt = await EventRehydrator().rehydrate(
            await RoundTripEvent(ctx, "t").create_snapshot(),
            previous_results=[response],
        )
        assert list(rebuilt.previous_result) == [response]

    @pytest.mark.asyncio
    async def test_a_missing_previous_result_fails_loudly(
        self, ctx, restored_context, monkeypatch
    ):
        result = make_result()
        _, get, _ = stored(result)
        event = RoundTripEvent(ctx, "t", previous_result=[result])
        snapshot = await event.create_snapshot()
        monkeypatch.setattr(
            EventResult, "get", classmethod(lambda cls, i: stored()[1](i))
        )
        with pytest.raises(KeyError):
            await EventRehydrator().rehydrate(snapshot)

    @pytest.mark.asyncio
    async def test_an_event_result_exec_result_is_stored_by_reference(
        self, ctx, restored_context, monkeypatch
    ):
        outcome = make_result({"rows": 3})
        _, get, _ = stored(outcome)
        monkeypatch.setattr(EventResult, "get", classmethod(lambda cls, i: get(i)))
        event = RoundTripEvent(ctx, "t")
        event.exec_status, event.exec_result = True, outcome
        snapshot = await event.create_snapshot()
        assert snapshot.exec_result["__type__"] == "EventResult"
        assert snapshot.exec_result["id"] == outcome.id
        rebuilt = await EventRehydrator().rehydrate(snapshot)
        assert rebuilt.exec_result is outcome

    @pytest.mark.asyncio
    async def test_results_nested_in_containers_are_referenced_and_restored(
        self, ctx, restored_context, monkeypatch
    ):
        a, b = make_result("a"), make_result("b")
        _, get, _ = stored(a, b)
        monkeypatch.setattr(EventResult, "get", classmethod(lambda cls, i: get(i)))
        event = RoundTripEvent(ctx, "t")
        event.exec_status, event.exec_result = True, {"all": [a, b], "n": 2}
        event._call_args = {"source": a}
        rebuilt = await EventRehydrator().rehydrate(await event.create_snapshot())
        assert rebuilt.exec_result == {"all": [a, b], "n": 2}
        assert rebuilt._call_args == {"source": a}

    @pytest.mark.asyncio
    async def test_results_referenced_from_call_arguments_are_persisted(
        self, ctx, monkeypatch
    ):
        fresh = make_result(persisted=False)
        _, _, save = stored(fresh)
        monkeypatch.setattr(EventResult, "save", save)
        event = RoundTripEvent(ctx, "t")
        event._call_args = {"source": fresh}
        await event.create_snapshot()
        save.assert_awaited_once()

    def test_a_result_without_an_id_cannot_be_referenced(self):
        from volnux.execution.rehydrator.event.serializer import StateSerializer

        class Anonymous:
            id = None

        with pytest.raises(TypeError, match="no id"):
            StateSerializer.serialize_init_args({"previous_result": [Anonymous()]})

    def test_reference_round_trip_through_the_exec_result_serializer(self):
        from volnux.execution.rehydrator.event.event_result_serializer import (
            EventResultRef,
            ExecResultSerializer,
        )

        result = make_result()
        serializer = ExecResultSerializer()
        data = serializer.serialize_exec_result([result, {"k": result}])
        back = serializer.deserialize_exec_result(data)
        ref = EventResultRef(id=result.id, class_path=data[0]["__class_path__"])
        assert back == [ref, {"k": ref}]
