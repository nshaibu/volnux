import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock

import pytest

from volnux.backends.messaging.base import (
    Message,
    PubSubCapabilityMixin,
    PushPopCapabilityMixin,
    QueueSide,
    StreamCapabilityMixin,
    StreamEntry,
    StreamOffset,
)
from volnux.backends.messaging.decorators import (
    ensure_pubsub,
    ensure_pushpop,
    ensure_streaming,
)
from volnux.backends.storage_route import StorageRoute
from volnux.concurrency.async_utils import as_coroutine
from volnux.mixins.messaging import MessagingBackendIntegrationMixin

SCHEMA = "volnux:test:messages"


class FakeSubscription:
    """Async context manager yielding pre-loaded Messages."""

    def __init__(self, messages):
        self.messages = messages
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *exc):
        self.exited = True

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for message in self.messages:
            yield message


def make_backend(*capabilities):
    """Backend implementing ``capabilities`` with every operation mocked."""
    backend_cls = type("FakeBackend", tuple(capabilities) or (object,), {})
    backend = backend_cls()
    backend.resolve_physical_target = MagicMock(
        side_effect=lambda route, override=None: ":".join(route.components)
    )
    for name in (
        "publish",
        "push",
        "pop",
        "pop_many",
        "enqueue",
        "dequeue",
        "queue_length",
        "queue_range",
        "queue_peek",
        "stream_append",
        "stream_ensure_consumer_group",
        "stream_read",
        "stream_ack",
        "stream_claim_pending",
        "stream_length",
        "stream_trim",
        "stream_delete",
        "stream_read_and_ack",
    ):
        setattr(backend, name, AsyncMock(name=name))
    backend.subscribe = MagicMock(name="subscribe")
    backend.psubscribe = MagicMock(name="psubscribe")
    return backend


ALL_CAPABILITIES = (
    PubSubCapabilityMixin,
    PushPopCapabilityMixin,
    StreamCapabilityMixin,
)


class Note(MessagingBackendIntegrationMixin):
    @classmethod
    def get_storage_route(cls):
        return StorageRoute(components=SCHEMA.split(":"))


@pytest.fixture
def backend():
    return make_backend(*ALL_CAPABILITIES)


@pytest.fixture
def model(backend):
    """A fresh model class bound to ``backend``."""
    cls = type("BoundNote", (Note,), {})
    cls._backend_store = backend
    cls._backend_initialized = True
    return cls


def message(data, channel=SCHEMA, pattern=None):
    return Message(channel=channel, data=data, pattern=pattern, raw=b"raw")


class TestCapabilityDecorators:
    @pytest.mark.asyncio
    async def test_backend_is_resolved_and_injected(self, model, backend):
        backend.queue_length.return_value = 3
        assert await model.queue_length() == 3
        backend.queue_length.assert_awaited_once_with(SCHEMA)

    @pytest.mark.asyncio
    async def test_get_backend_is_awaited(self, backend):
        calls = []

        class Lazy(Note):
            @classmethod
            async def get_backend(cls):
                calls.append(cls)
                return backend

        await Lazy.queue_length()
        assert calls  # coroutine was awaited, not passed through

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method, args, kwargs, capability",
        [
            ("publish", ("r",), {}, "PubSubCapabilityMixin"),
            ("subscribe", (), {"callback": print}, "PubSubCapabilityMixin"),
            ("psubscribe", ("p*",), {"callback": print}, "PubSubCapabilityMixin"),
            ("push", ("r",), {}, "PushPopCapabilityMixin"),
            ("pop", (), {}, "PushPopCapabilityMixin"),
            ("pop_many", (), {}, "PushPopCapabilityMixin"),
            ("enqueue", ("r",), {}, "PushPopCapabilityMixin"),
            ("dequeue", (), {}, "PushPopCapabilityMixin"),
            ("queue_length", (), {}, "PushPopCapabilityMixin"),
            ("queue_range", (), {}, "PushPopCapabilityMixin"),
            ("queue_peek", (), {}, "PushPopCapabilityMixin"),
            ("stream_append", ("r",), {}, "StreamCapabilityMixin"),
            ("stream_ensure_consumer_group", ("g",), {}, "StreamCapabilityMixin"),
            ("stream_read", ("g",), {}, "StreamCapabilityMixin"),
            ("stream_ack", ("g", "1-0"), {}, "StreamCapabilityMixin"),
            ("stream_claim_pending", ("g", "c"), {}, "StreamCapabilityMixin"),
            ("stream_length", (), {}, "StreamCapabilityMixin"),
            ("stream_trim", (10,), {}, "StreamCapabilityMixin"),
            ("stream_delete", (), {}, "StreamCapabilityMixin"),
            ("stream_read_and_ack", ("g",), {}, "StreamCapabilityMixin"),
        ],
    )
    async def test_missing_capability_rejected(self, method, args, kwargs, capability):
        backend = make_backend()  # no capabilities
        cls = type("Unsupported", (Note,), {})
        cls._backend_store = backend
        cls._backend_initialized = True

        with pytest.raises(TypeError, match=capability) as exc_info:
            await getattr(cls, method)(*args, **kwargs)
        assert "Unsupported" in str(exc_info.value)
        assert getattr(backend, method).call_count == 0

    @pytest.mark.asyncio
    async def test_only_required_capability_needed(self):
        backend = make_backend(PushPopCapabilityMixin)
        cls = type("QueueOnly", (Note,), {})
        cls._backend_store = backend
        cls._backend_initialized = True

        backend.queue_length.return_value = 0
        assert await cls.queue_length() == 0
        with pytest.raises(TypeError, match="PubSubCapabilityMixin"):
            await cls.publish("r")

    @pytest.mark.parametrize(
        "decorator", [ensure_pubsub, ensure_pushpop, ensure_streaming]
    )
    def test_preserves_wrapped_metadata(self, decorator):
        async def operation(cls, backend):
            """Docs."""

        wrapped = decorator(operation)
        assert wrapped.__name__ == "operation"
        assert wrapped.__doc__ == "Docs."
        assert wrapped.__wrapped__ is operation

    @pytest.mark.asyncio
    async def test_change_backend(self, backend):
        cls = type("Swappable", (Note,), {})
        backend.queue_length.return_value = 9
        async with cls.change_backend(backend):
            assert await cls.queue_length() == 9


class TestPublish:
    @pytest.mark.asyncio
    async def test_publish(self, model, backend):
        backend.publish.return_value = 2
        assert await model.publish("record") == 2
        backend.publish.assert_awaited_once_with(channel=SCHEMA, record="record")


class TestSubscribe:
    @pytest.mark.asyncio
    async def test_async_callback_receives_records(self, model, backend):
        subscription = FakeSubscription([message("a"), message("b")])
        backend.subscribe.return_value = subscription
        received = []

        async def callback(record):
            received.append(record)

        await model.subscribe(callback)

        assert received == ["a", "b"]
        backend.subscribe.assert_called_once_with(SCHEMA, record_class=model)
        assert subscription.entered and subscription.exited

    @pytest.mark.asyncio
    async def test_sync_callback_runs(self, model, backend):
        backend.subscribe.return_value = FakeSubscription([message("a")])
        received = []
        await model.subscribe(received.append)
        assert received == ["a"]

    @pytest.mark.asyncio
    async def test_callback_error_stops_and_exits_subscription(self, model, backend):
        subscription = FakeSubscription([message("a"), message("b")])
        backend.subscribe.return_value = subscription
        seen = []

        async def callback(record):
            seen.append(record)
            raise RuntimeError("handler failed")

        with pytest.raises(RuntimeError, match="handler failed"):
            await model.subscribe(callback)
        assert seen == ["a"]
        assert subscription.exited

    @pytest.mark.asyncio
    async def test_cancellation_exits_subscription(self, model, backend):
        class Endless(FakeSubscription):
            async def _iterate(self):
                while True:
                    await asyncio.sleep(0.01)
                    yield message("tick")

        subscription = Endless([])
        backend.subscribe.return_value = subscription
        task = asyncio.create_task(model.subscribe(lambda record: None))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert subscription.exited

    @pytest.mark.asyncio
    async def test_psubscribe_includes_schema_and_patterns(self, model, backend):
        backend.psubscribe.return_value = FakeSubscription(
            [message("a", channel="volnux:x", pattern="volnux:*")]
        )
        received = []
        await model.psubscribe("volnux:*", "other:*", callback=received.append)

        assert received == ["a"]
        backend.psubscribe.assert_called_once_with(
            SCHEMA, "volnux:*", "other:*", record_class=model
        )

    @pytest.mark.asyncio
    async def test_psubscribe_deduplicates_preserving_order(self, model, backend):
        backend.psubscribe.return_value = FakeSubscription([])
        await model.psubscribe("b:*", SCHEMA, "a:*", "b:*", callback=print)
        assert backend.psubscribe.call_args.args == (SCHEMA, "b:*", "a:*")

    @pytest.mark.asyncio
    async def test_psubscribe_without_patterns(self, model, backend):
        backend.psubscribe.return_value = FakeSubscription([])
        await model.psubscribe(callback=print)
        assert backend.psubscribe.call_args.args == (SCHEMA,)


class TestPushPop:
    @pytest.mark.asyncio
    async def test_push_defaults_to_right(self, model, backend):
        backend.push.return_value = 2
        assert await model.push("a", "b") == 2
        backend.push.assert_awaited_once_with(SCHEMA, "a", "b", side=QueueSide.RIGHT)

    @pytest.mark.asyncio
    async def test_push_left(self, model, backend):
        await model.push("a", side=QueueSide.LEFT)
        backend.push.assert_awaited_once_with(SCHEMA, "a", side=QueueSide.LEFT)

    @pytest.mark.asyncio
    async def test_pop_defaults(self, model, backend):
        backend.pop.return_value = "a"
        assert await model.pop() == "a"
        backend.pop.assert_awaited_once_with(
            SCHEMA, record_class=model, timeout=None, side=QueueSide.LEFT
        )

    @pytest.mark.asyncio
    async def test_pop_with_options(self, model, backend):
        await model.pop(timeout=2.5, side=QueueSide.RIGHT)
        backend.pop.assert_awaited_once_with(
            SCHEMA, record_class=model, timeout=2.5, side=QueueSide.RIGHT
        )

    @pytest.mark.asyncio
    async def test_pop_options_are_keyword_only(self, model):
        with pytest.raises(TypeError):
            await model.pop(1.0)

    @pytest.mark.asyncio
    async def test_pop_many_defaults(self, model, backend):
        backend.pop_many.return_value = ["a"]
        assert await model.pop_many() == ["a"]
        backend.pop_many.assert_awaited_once_with(
            SCHEMA, record_class=model, limit=None, timeout=None, side=QueueSide.LEFT
        )

    @pytest.mark.asyncio
    async def test_pop_many_with_options(self, model, backend):
        await model.pop_many(limit=5, timeout=1, side=QueueSide.RIGHT)
        backend.pop_many.assert_awaited_once_with(
            SCHEMA, record_class=model, limit=5, timeout=1, side=QueueSide.RIGHT
        )

    @pytest.mark.asyncio
    async def test_enqueue(self, model, backend):
        backend.enqueue.return_value = 1
        assert await model.enqueue("a") == 1
        backend.enqueue.assert_awaited_once_with(SCHEMA, "a")

    @pytest.mark.asyncio
    async def test_dequeue(self, model, backend):
        backend.dequeue.return_value = "a"
        assert await model.dequeue(timeout=3) == "a"
        backend.dequeue.assert_awaited_once_with(SCHEMA, record_class=model, timeout=3)

    @pytest.mark.asyncio
    async def test_queue_range(self, model, backend):
        backend.queue_range.return_value = ["a", "b"]
        assert await model.queue_range(1, 2) == ["a", "b"]
        backend.queue_range.assert_awaited_once_with(
            SCHEMA, record_class=model, start=1, stop=2
        )

    @pytest.mark.asyncio
    async def test_queue_range_defaults_to_whole_queue(self, model, backend):
        await model.queue_range()
        assert backend.queue_range.call_args.kwargs["start"] == 0
        assert backend.queue_range.call_args.kwargs["stop"] == -1

    @pytest.mark.asyncio
    async def test_queue_peek(self, model, backend):
        backend.queue_peek.return_value = "head"
        assert await model.queue_peek() == "head"
        backend.queue_peek.assert_awaited_once_with(SCHEMA, record_class=model)

    @pytest.mark.asyncio
    async def test_backend_errors_propagate(self, model, backend):
        backend.push.side_effect = ConnectionError("down")
        with pytest.raises(ConnectionError, match="down"):
            await model.push("a")


class TestStreams:
    @pytest.mark.asyncio
    async def test_append_defaults(self, model, backend):
        backend.stream_append.return_value = "1-0"
        assert await model.stream_append("r") == "1-0"
        backend.stream_append.assert_awaited_once_with(
            stream_key=SCHEMA, record="r", max_len=None, approximate_trim=True
        )

    @pytest.mark.asyncio
    async def test_append_with_trim(self, model, backend):
        await model.stream_append("r", max_len=100, approximate_trim=False)
        assert backend.stream_append.call_args.kwargs["max_len"] == 100
        assert backend.stream_append.call_args.kwargs["approximate_trim"] is False

    @pytest.mark.asyncio
    async def test_ensure_consumer_group_defaults_to_beginning(self, model, backend):
        await model.stream_ensure_consumer_group("g")
        backend.stream_ensure_consumer_group.assert_awaited_once_with(
            stream_key=SCHEMA, group_name="g", start_from=StreamOffset.BEGINNING
        )

    @pytest.mark.asyncio
    async def test_ensure_consumer_group_from_id(self, model, backend):
        await model.stream_ensure_consumer_group("g", start_from="5-0")
        assert backend.stream_ensure_consumer_group.call_args.kwargs["start_from"] == (
            "5-0"
        )

    @pytest.mark.asyncio
    async def test_read_defaults(self, model, backend):
        entries = [StreamEntry(entry_id="1-0", record="r")]
        backend.stream_read.return_value = entries
        assert await model.stream_read("g") == entries
        backend.stream_read.assert_awaited_once_with(
            stream_key=SCHEMA,
            group_name="g",
            record_class=model,
            consumer_id=None,
            count=1,
            block_ms=5000,
        )

    @pytest.mark.asyncio
    async def test_read_with_options(self, model, backend):
        await model.stream_read("g", consumer_id="c1", count=10, block_ms=None)
        kwargs = backend.stream_read.call_args.kwargs
        assert (kwargs["consumer_id"], kwargs["count"], kwargs["block_ms"]) == (
            "c1",
            10,
            None,
        )

    @pytest.mark.asyncio
    async def test_ack(self, model, backend):
        backend.stream_ack.return_value = 2
        assert await model.stream_ack("g", "1-0", "2-0") == 2
        backend.stream_ack.assert_awaited_once_with(SCHEMA, "g", "1-0", "2-0")

    @pytest.mark.asyncio
    async def test_claim_pending_defaults(self, model, backend):
        backend.stream_claim_pending.return_value = []
        assert await model.stream_claim_pending("g", "c2") == []
        backend.stream_claim_pending.assert_awaited_once_with(
            stream_key=SCHEMA,
            group_name="g",
            consumer_id="c2",
            record_class=model,
            min_idle_ms=30_000,
            count=100,
        )

    @pytest.mark.asyncio
    async def test_length(self, model, backend):
        backend.stream_length.return_value = 4
        assert await model.stream_length() == 4
        backend.stream_length.assert_awaited_once_with(SCHEMA)

    @pytest.mark.asyncio
    async def test_trim(self, model, backend):
        backend.stream_trim.return_value = 3
        assert await model.stream_trim(10, approximate=False) == 3
        backend.stream_trim.assert_awaited_once_with(
            stream_key=SCHEMA, max_len=10, approximate=False
        )

    @pytest.mark.asyncio
    async def test_delete(self, model, backend):
        backend.stream_delete.return_value = True
        assert await model.stream_delete() is True
        backend.stream_delete.assert_awaited_once_with(SCHEMA)

    @pytest.mark.asyncio
    async def test_read_and_ack(self, model, backend):
        backend.stream_read_and_ack.return_value = ["r"]
        assert await model.stream_read_and_ack("g", consumer_id="c1", count=5) == ["r"]
        backend.stream_read_and_ack.assert_awaited_once_with(
            stream_key=SCHEMA,
            group_name="g",
            record_class=model,
            consumer_id="c1",
            count=5,
            block_ms=5000,
        )


class TestSchemaRouting:
    @pytest.mark.asyncio
    async def test_schema_name_resolved_through_backend(self, model, backend):
        await model.queue_length()
        route = backend.resolve_physical_target.call_args.args[0]
        assert route.components == SCHEMA.split(":")

    @pytest.mark.asyncio
    async def test_each_model_uses_its_own_route(self, backend):
        class Other(MessagingBackendIntegrationMixin):
            @classmethod
            def get_storage_route(cls):
                return StorageRoute(components=["volnux", "other"])

        Other._backend_store = backend
        Other._backend_initialized = True
        await Other.queue_length()
        backend.queue_length.assert_awaited_once_with("volnux:other")


class TestNoDuplicateDefinitions:
    def test_stream_methods_defined_once(self):
        import ast
        import inspect

        import volnux.mixins.messaging as module

        tree = ast.parse(inspect.getsource(module))
        cls = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef)
            and node.name == "MessagingBackendIntegrationMixin"
        )
        names = [
            node.name
            for node in cls.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ]
        assert len(names) == len(set(names))


class TestAsCoroutine:
    @pytest.mark.asyncio
    async def test_async_function(self):
        async def fn(x, y=0):
            return x + y

        assert await as_coroutine(fn, 1, y=2) == 3

    @pytest.mark.asyncio
    async def test_sync_function_runs_in_thread(self):
        threads = []

        def fn(x):
            threads.append(threading.current_thread())
            return x * 2

        assert await as_coroutine(fn, 4) == 8
        assert threads and threads[0] is not threading.main_thread()

    @pytest.mark.asyncio
    async def test_sync_wrapper_returning_awaitable(self):
        async def work():
            return "done"

        assert await as_coroutine(lambda: work()) == "done"

    @pytest.mark.asyncio
    async def test_async_callable_object(self):
        class Handler:
            async def __call__(self, x):
                return x + 1

        assert await as_coroutine(Handler(), 1) == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize("is_async", [True, False])
    async def test_exceptions_propagate(self, is_async):
        if is_async:

            async def fn():
                raise ValueError("boom")

        else:

            def fn():
                raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await as_coroutine(fn)


class TestSubscribeOnError:
    @pytest.mark.asyncio
    async def test_on_error_keeps_the_subscription_running(self, model, backend):
        subscription = FakeSubscription([message("a"), message("bad"), message("c")])
        backend.subscribe.return_value = subscription
        seen, errors = [], []

        async def callback(record):
            if record == "bad":
                raise RuntimeError("boom")
            seen.append(record)

        async def on_error(exc, record):
            errors.append((str(exc), record))

        await model.subscribe(callback, on_error=on_error)

        assert seen == ["a", "c"]
        assert errors == [("boom", "bad")]
        assert subscription.exited

    @pytest.mark.asyncio
    async def test_sync_on_error_and_psubscribe(self, model, backend):
        backend.psubscribe.return_value = FakeSubscription([message("x")])
        errors = []

        def callback(record):
            raise ValueError("nope")

        await model.psubscribe(
            callback=callback, on_error=lambda exc, rec: errors.append(rec)
        )
        assert errors == ["x"]

    @pytest.mark.asyncio
    async def test_failing_on_error_propagates(self, model, backend):
        backend.subscribe.return_value = FakeSubscription([message("a")])

        async def callback(record):
            raise RuntimeError("first")

        async def on_error(exc, record):
            raise KeyError("second")

        with pytest.raises(KeyError):
            await model.subscribe(callback, on_error=on_error)


class TestArgumentValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method, kwargs",
        [
            ("pop", {"timeout": -1}),
            ("pop_many", {"timeout": -0.5}),
            ("dequeue", {"timeout": -1}),
            ("stream_read", {"group_name": "g", "count": 0}),
            ("stream_read", {"group_name": "g", "block_ms": -1}),
            ("stream_read_and_ack", {"group_name": "g", "count": 0}),
            (
                "stream_claim_pending",
                {"group_name": "g", "consumer_id": "c", "count": 0},
            ),
            (
                "stream_claim_pending",
                {"group_name": "g", "consumer_id": "c", "min_idle_ms": -1},
            ),
            ("stream_trim", {"max_len": -1}),
            ("stream_append", {"record": "r", "max_len": -1}),
        ],
    )
    async def test_invalid_arguments_rejected_before_reaching_backend(
        self, model, backend, method, kwargs
    ):
        with pytest.raises(ValueError):
            await getattr(model, method)(**kwargs)
        for name in ("pop", "pop_many", "dequeue", "stream_read", "stream_trim"):
            getattr(backend, name).assert_not_awaited()

    @pytest.mark.asyncio
    async def test_push_requires_an_instance(self, model, backend):
        with pytest.raises(ValueError):
            await model.push()
        backend.push.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_zero_timeout_and_block_ms_remain_valid(self, model, backend):
        backend.pop.return_value = None
        await model.pop(timeout=0)
        await model.stream_read("g", block_ms=0)
        backend.pop.assert_awaited_once()
        backend.stream_read.assert_awaited_once()
