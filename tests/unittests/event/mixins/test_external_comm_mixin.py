import uuid
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from volnux.exceptions import ExternalCommunicationSuspensionRequest, SuspendTask
from volnux.execution.rehydrator.event.snapshot import EventPhase
from volnux.mixins.event.communication.providers import slack as slack_module
from volnux.mixins.event.communication.providers.slack import SlackAdapter
from volnux.mixins.event.external import ExternalCommunicationType


def communicating(event):
    event._active_phase = EventPhase.COMMUNICATING
    return event


@pytest.fixture
def event(build, checkpoint_manager):
    return communicating(build(checkpoint_manager=checkpoint_manager))


async def suspension(awaitable):
    with pytest.raises(ExternalCommunicationSuspensionRequest) as info:
        await awaitable
    return info.value


class TestPhaseGuard:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "phase", [None, EventPhase.PROCESSING, EventPhase.PRE_PROCESS]
    )
    async def test_calls_outside_communicate_are_rejected(self, build, phase):
        event = build()
        event._active_phase = phase
        for call in (
            event.request_human_input("t", "d", {}),
            event.wait_for_event("x", "t", "d"),
            event.wait_for_condition(lambda: True, "t"),
        ):
            with pytest.raises(RuntimeError, match="communicate"):
                await call

    @pytest.mark.asyncio
    async def test_rejected_call_does_not_checkpoint(self, build, checkpoint_manager):
        event = build(checkpoint_manager=checkpoint_manager)
        with pytest.raises(RuntimeError):
            await event.request_human_input("t", "d", {})
        checkpoint_manager.enqueue.assert_not_called()


class TestRequestHumanInput:
    @pytest.mark.asyncio
    async def test_raises_a_hitl_suspension_with_all_fields(self, event):
        exc = await suspension(
            event.request_human_input(
                "Approve order",
                "Order 7 is large",
                {"order": 7},
                options=["approve", "reject"],
                timeout_hours=4,
            )
        )
        assert exc.request_type is ExternalCommunicationType.HITL
        assert (exc.title, exc.description) == ("Approve order", "Order 7 is large")
        assert exc.payload == {"order": 7}
        assert exc.options == ["approve", "reject"] and exc.timeout_hours == 4
        assert exc.task_id == "task-1"
        assert exc.suspension_type is SuspendTask.SuspensionType.HITL
        uuid.UUID(exc.request_id)  # a valid uuid

    @pytest.mark.asyncio
    async def test_defaults(self, event):
        exc = await suspension(event.request_human_input("t", "d", {}))
        assert exc.options is None and exc.timeout_hours == 24

    @pytest.mark.asyncio
    async def test_each_request_gets_a_fresh_id(self, event):
        a = await suspension(event.request_human_input("t", "d", {}))
        b = await suspension(event.request_human_input("t", "d", {}))
        assert a.request_id != b.request_id

    @pytest.mark.asyncio
    async def test_state_is_checkpointed_before_suspending(
        self, event, checkpoint_manager
    ):
        await suspension(event.request_human_input("t", "d", {}))
        checkpoint_manager.enqueue.assert_called_once()

    @pytest.mark.asyncio
    async def test_suspension_is_a_control_flow_exception(self, event):
        exc = await suspension(event.request_human_input("t", "d", {}))
        assert isinstance(exc, SuspendTask)


class TestWaitForEvent:
    @pytest.mark.asyncio
    async def test_fields(self, event):
        exc = await suspension(
            event.wait_for_event(
                "payment.confirmed",
                "Wait for payment",
                "Payment must arrive",
                event_filter={"order": 9},
                timeout_hours=2,
            )
        )
        assert exc.request_type is ExternalCommunicationType.EVENT
        assert exc.event_type == "payment.confirmed"
        assert exc.event_filter == {"order": 9} and exc.timeout_hours == 2
        assert exc.payload == {} and exc.options == []
        assert exc.suspension_type is SuspendTask.SuspensionType.EXTERNAL_EVENT

    @pytest.mark.asyncio
    async def test_filter_defaults_to_empty(self, event):
        exc = await suspension(event.wait_for_event("x", "t", "d"))
        assert exc.event_filter == {} and exc.timeout_hours is None


class TestWaitForCondition:
    @pytest.mark.asyncio
    async def test_fields(self, event):
        def ready():
            return True

        exc = await suspension(
            event.wait_for_condition(
                ready,
                "Wait for file",
                "s3 object",
                timeout_hours=3,
                poll_interval_seconds=5,
            )
        )
        assert exc.request_type is ExternalCommunicationType.CONDITION
        assert exc.condition_fn is ready and exc.poll_interval_seconds == 5
        assert exc.timeout_hours == 3 and exc.description == "s3 object"
        assert exc.suspension_type is SuspendTask.SuspensionType.CONDITION

    @pytest.mark.asyncio
    async def test_defaults(self, event):
        exc = await suspension(event.wait_for_condition(lambda: True, "t"))
        assert exc.description == "" and exc.poll_interval_seconds == 30.0
        assert exc.timeout_hours is None


class TestCommunicateHook:
    @pytest.mark.asyncio
    async def test_default_communicate_is_a_noop(self, build):
        assert await build().communicate() is None

    @pytest.mark.asyncio
    async def test_hook_runs_in_the_communicating_phase(self, build):
        seen = []

        async def communicate(self, *a, **k):
            seen.append(self._active_phase)

        event = build({"communicate": communicate})
        await event._communicate()
        assert seen == [EventPhase.COMMUNICATING]
        assert event.get_phase() == EventPhase.COMMUNICATING

    @pytest.mark.asyncio
    async def test_hook_receives_the_call_arguments(self, build):
        got = {}

        async def communicate(self, *args, **kwargs):
            got["args"], got["kwargs"] = args, kwargs

        await build({"communicate": communicate})._communicate(1, a=2)
        assert got == {"args": (1,), "kwargs": {"a": 2}}

    @pytest.mark.asyncio
    async def test_communicate_hands_nothing_to_process(self, build):
        """communicate() suspends rather than returns: answers come via previous_result."""
        seen = {}

        async def communicate(self, *a, **k):
            return {"injected": 1}

        async def process(self, **kwargs):
            seen.update(kwargs)
            return True, "ok"

        await build({"communicate": communicate, "process": process})(x=1)
        assert seen == {"x": 1}

    @pytest.mark.asyncio
    async def test_full_run_checkpoints_before_the_communicating_phase_completes(
        self, build, checkpoint_manager
    ):
        """The resumable state is the checkpoint, taken at the end of PRE_PROCESS."""

        async def communicate(self, *a, **k):
            await self.request_human_input("t", "d", {"k": 1})

        event = build(
            {"communicate": communicate}, checkpoint_manager=checkpoint_manager
        )
        with pytest.raises(ExternalCommunicationSuspensionRequest):
            await event()
        snapshot = checkpoint_manager.enqueue.call_args[0][0]
        assert snapshot.task_id == "task-1"
        assert snapshot.phase == EventPhase.PRE_PROCESS

    @pytest.mark.asyncio
    async def test_a_suspended_event_checkpoints_then_cleans_up_its_resources(
        self, build, checkpoint_manager
    ):
        from .. import providers

        providers.CLEANED.clear()
        order = []
        checkpoint_manager.enqueue.side_effect = lambda snapshot: order.append(
            "checkpoint"
        )

        async def communicate(self, *a, **k):
            await self.acquire_resource(
                "c", "tests.unittests.event.providers.CounterProvider", {"value": 3}
            )
            await self.request_human_input("t", "d", {})

        event = build(
            {"communicate": communicate}, checkpoint_manager=checkpoint_manager
        )
        original = providers.CounterProvider.cleanup.__func__

        def tracking(cls, resource):
            order.append("release")
            original(cls, resource)

        providers.CounterProvider.cleanup = classmethod(tracking)
        try:
            with pytest.raises(ExternalCommunicationSuspensionRequest):
                await event()
        finally:
            providers.CounterProvider.cleanup = classmethod(original)
        assert order.index("checkpoint") < order.index("release")
        assert len(providers.CLEANED) == 1
        snapshot = checkpoint_manager.enqueue.call_args_list[-1][0][0]
        assert snapshot.external_resources["c"]["data"] == {"value": 3}
        assert event._resource_monitor.is_started() is False

    @pytest.mark.asyncio
    async def test_a_suspended_event_releases_its_resources(self, build):
        """A suspension checkpoints the task and releases everything it holds:
        __call__ always finishes with _completed(), even when the event suspends."""
        cleaned = []

        async def communicate(self, *a, **k):
            await self.request_human_input("t", "d", {})

        async def cleanup(self, *a, **k):
            cleaned.append(1)

        event = build({"communicate": communicate, "cleanup": cleanup})
        with pytest.raises(ExternalCommunicationSuspensionRequest):
            await event()
        assert cleaned == [1]


class FakeResponse:
    def __init__(self, status=200):
        self.status = status
        self.checked = False

    def raise_for_status(self):
        self.checked = True
        if self.status >= 400:
            raise httpx.HTTPStatusError(
                "bad", request=MagicMock(), response=MagicMock(status_code=self.status)
            )


class FakeClient:
    instances = []

    def __init__(self, response, **kwargs):
        self.kwargs = kwargs
        self.response = response
        self.posts = []
        FakeClient.instances.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None):
        self.posts.append((url, json))
        return self.response


class TestSlackAdapter:
    @pytest.fixture
    def request_obj(self, event):
        exc = ExternalCommunicationSuspensionRequest(
            task=event,
            task_id="task-1",
            request_id="req-1",
            request_type=ExternalCommunicationType.HITL,
            title="Approve order",
            description="Order *7*",
            payload={},
            options=["approve", "reject"],
            timeout_hours=4,
        )
        return exc

    @pytest.fixture
    def http(self, monkeypatch):
        FakeClient.instances.clear()
        state = {"response": FakeResponse()}

        def factory(**kwargs):
            return FakeClient(state["response"], **kwargs)

        monkeypatch.setattr(slack_module.httpx, "AsyncClient", factory)
        return state

    @pytest.mark.asyncio
    async def test_posts_blocks_to_the_webhook(self, http, request_obj):
        adapter = SlackAdapter("https://hooks.test/x", "#ops")
        await adapter.notify(request_obj)
        [client] = FakeClient.instances
        [(url, body)] = client.posts
        assert url == "https://hooks.test/x"
        assert body["channel"] == "#ops"
        assert "Approve order" in body["text"]
        assert http["response"].checked is True

    @pytest.mark.asyncio
    async def test_request_has_a_timeout(self, http, request_obj):
        await SlackAdapter("u", "#c").notify(request_obj)
        assert (
            FakeClient.instances[0].kwargs["timeout"]
            == SlackAdapter.REQUEST_TIMEOUT_SECONDS
        )

    @pytest.mark.asyncio
    async def test_rejected_webhook_is_an_error(self, http, request_obj):
        http["response"] = FakeResponse(status=404)
        with pytest.raises(httpx.HTTPStatusError):
            await SlackAdapter("u", "#c").notify(request_obj)

    def test_blocks_describe_the_request_and_buttons(self, request_obj):
        blocks = SlackAdapter("u", "#c")._build_blocks(request_obj)
        assert blocks[0]["text"]["text"] == "Approve order"
        details = blocks[2]["text"]["text"]
        assert "req-1" in details and "task-1" in details and "4h" in details
        buttons = blocks[3]["elements"]
        assert [b["value"] for b in buttons] == ["req-1:approve", "req-1:reject"]
        assert buttons[0]["style"] == "primary" and buttons[1]["style"] == "danger"

    def test_no_buttons_without_options(self, event):
        request = ExternalCommunicationSuspensionRequest(
            task=event,
            task_id="task-1",
            request_id="r",
            request_type=ExternalCommunicationType.HITL,
            title="t",
            description="d",
            payload={},
            options=None,
            timeout_hours=None,
        )
        blocks = SlackAdapter("u", "#c")._build_blocks(request)
        assert all(b["type"] != "actions" for b in blocks)

    @pytest.mark.asyncio
    async def test_response_url(self, request_obj):
        assert await SlackAdapter("u", "#ops").get_response_url(request_obj) == (
            "slack://channel/#ops"
        )
