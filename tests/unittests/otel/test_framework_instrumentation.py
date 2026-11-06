"""Instrumentation of the framework's key parts: triggers, workflow executors, the
registry, checkpoints and rehydration, sagas, messaging, remote streaming."""

import asyncio
import importlib
import inspect
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from formax import BaseModel, InitStrategy
from opentelemetry.trace import StatusCode

from tests.unittests.support import SerializingMemoryBackend
from volnux.backends.messaging.base import QueueSide
from volnux.backends.saga import Saga, SagaStatus, SagaStep
from volnux.backends.saga._dlq import DeadLetterEntry
from volnux.backends.saga.recovery import SagaRecoveryState
from volnux.backends.storage_route import StorageRoute
from volnux.engine.workflows.registry import WorkflowRegistry
from volnux.engine.workflows.source import RegistrySource
from volnux.engine.workflows.trigger.engine import TriggerEngine
from volnux.engine.workflows.trigger.executors.asyncio_executor import (
    AsyncTaskWorkflowExecutor,
)
from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import (
    ManualTrigger,
    TriggerActivation,
    TriggerType,
)
from volnux.engine.workflows.workflow import TriggerRegistry, WorkflowExecutionError
from volnux.mixins.messaging import MessagingBackendIntegrationMixin
from volnux.otel.instrumentations import framework
from volnux.otel.instrumentations._patching import is_patched, unpatch_all

pytestmark = pytest.mark.asyncio


def wrapper_for(owner: str, attribute: str):
    """The instrumenter the table has for ``owner.attribute``."""
    for _module, name, attr, wrapper in framework.PATCH_TABLE:
        if name == owner and attr == attribute:
            return wrapper
    raise AssertionError(f"{owner}.{attribute} is not in the table")


# --------------------------------------------------------------- the table
class TestPatchTable:
    def resolve(self, module_name, owner_name):
        module = importlib.import_module(module_name)
        return getattr(module, owner_name) if owner_name else module

    async def test_every_entry_names_something_that_exists(self):
        for module_name, owner_name, attribute, _ in framework.PATCH_TABLE:
            owner = self.resolve(module_name, owner_name)
            assert hasattr(owner, attribute), f"{module_name}.{owner_name}.{attribute}"

    async def test_patch_all_patches_every_entry_once(self, otel):
        assert framework.patch_all() == len(framework.PATCH_TABLE)
        assert framework.patch_all() == 0
        for module_name, owner_name, attribute, _ in framework.PATCH_TABLE:
            assert is_patched(self.resolve(module_name, owner_name), attribute)

    async def test_the_kind_of_every_method_is_kept(self, otel):
        def kinds():
            out = {}
            for module_name, owner_name, attribute, _ in framework.PATCH_TABLE:
                owner = self.resolve(module_name, owner_name)
                raw = inspect.getattr_static(owner, attribute)
                function = getattr(raw, "__func__", raw)
                out[(module_name, owner_name, attribute)] = (
                    type(raw).__name__,
                    inspect.iscoroutinefunction(function),
                )
            return out

        before = kinds()
        framework.patch_all()
        assert kinds() == before

    async def test_unpatching_restores_every_entry(self, otel):
        originals = {
            (m, o, a): inspect.getattr_static(self.resolve(m, o), a)
            for m, o, a, _ in framework.PATCH_TABLE
        }
        framework.patch_all()
        unpatch_all()
        for (m, o, a), original in originals.items():
            assert inspect.getattr_static(self.resolve(m, o), a) is original

    async def test_every_messaging_operation_exists_on_the_mixin(self):
        for operation in framework.MESSAGING_OPERATIONS:
            assert hasattr(MessagingBackendIntegrationMixin, operation)


# ------------------------------------------------------------ trigger engine
class FakeConfig:
    def __init__(self):
        self.triggers = TriggerRegistry()


class FakeRegistry:
    def __init__(self, *names):
        self.configs = {n: FakeConfig() for n in names}
        self.ready = True

    def is_ready(self):
        return self.ready

    def get_workflow_config(self, name):
        config = self.configs.get(name)
        if config is not None and not hasattr(config, "run_workflow"):
            config.run_workflow = _async_value("result")
        return config


def _async_value(value):
    async def run_workflow(params, run_type):
        if isinstance(value, BaseException):
            raise value
        return value

    return run_workflow


@pytest.fixture
def trigger_store(monkeypatch):
    backend = SerializingMemoryBackend()
    monkeypatch.setattr(TriggerStateRecord, "_backend_store", backend, raising=False)
    monkeypatch.setattr(TriggerStateRecord, "_backend_initialized", True, raising=False)
    yield backend
    backend.close()


@pytest.fixture
def engine(otel, trigger_store):
    framework.patch_all()
    registry = FakeRegistry("wf")
    executor = AsyncTaskWorkflowExecutor(registry)
    return TriggerEngine(
        executor, consumer_concurrency=1, drain_timeout=1, state_sync_interval=0.05
    )


class TestTriggerEngine:
    async def test_activation(self, otel, engine):
        trigger = ManualTrigger("wf")
        engine.register(trigger)
        activation = TriggerActivation(
            trigger_id=trigger.trigger_id,
            activated_at=datetime.now(timezone.utc),
            activation_source=TriggerType.MANUAL,
            workflow_params={"k": 1},
        )
        await engine._handle_activation(activation)
        span = otel.span("trigger.activate")
        assert span.attributes["trigger.id"] == trigger.trigger_id
        assert span.attributes["trigger.type"] == "manual"
        assert span.attributes["workflow.name"] == "wf"
        assert (
            otel.total("trigger.activations", trigger_type="manual", outcome="success")
            == 1
        )

    async def test_activation_of_an_unknown_trigger_has_no_workflow(self, otel, engine):
        await engine._handle_activation(
            TriggerActivation(
                trigger_id="nope",
                activated_at=datetime.now(timezone.utc),
                activation_source=TriggerType.MANUAL,
            )
        )
        assert "workflow.name" not in otel.span("trigger.activate").attributes

    async def test_fire_trigger(self, otel, engine):
        engine.register(ManualTrigger("wf"))
        await engine.fire_trigger("wf", {}, TriggerType.MANUAL)
        span = otel.span("trigger.fire")
        assert span.attributes["workflow.name"] == "wf"
        assert span.attributes["trigger.type"] == "manual"

    async def test_fire_trigger_errors_are_recorded(self, otel, engine):
        with pytest.raises(RuntimeError):
            await engine.fire_trigger("missing", {}, TriggerType.MANUAL)
        assert otel.span("trigger.fire").status.status_code is StatusCode.ERROR

    async def test_start_stop(self, otel, engine):
        engine.register(ManualTrigger("wf"))
        await engine.start()
        await engine.stop()
        start = otel.span("trigger_engine.start")
        assert start.attributes["trigger_engine.triggers"] == 1
        assert start.attributes["trigger_engine.consumers"] == 1
        otel.span("trigger_engine.stop")

    async def test_force_stop_reports_the_dropped_runs(self, otel, engine):
        await engine.start()
        gate = asyncio.Event()

        async def hold(workflow_name, params):
            await gate.wait()

        engine.workflow_executor.execute = hold
        for _ in range(3):
            engine.task_queue.put_nowait(("wf", {}))
        await asyncio.sleep(0.05)
        dropped = await engine.force_stop()
        assert (
            otel.span("trigger_engine.force_stop").attributes["trigger_engine.dropped"]
            == dropped
        )


# ------------------------------------------------------------ workflow executor
class TestWorkflowRun:
    @pytest.fixture
    def executor(self, otel):
        framework.patch_all()
        return AsyncTaskWorkflowExecutor(FakeRegistry("wf"))

    async def test_a_run_has_a_workflow_span_and_an_executor_span(self, otel, executor):
        assert await executor("wf", {"run_type": "single", "a": 1}) == "result"
        run = otel.span("workflow.run")
        assert run.attributes["workflow.name"] == "wf"
        assert run.attributes["executor.type"] == "AsyncTaskWorkflowExecutor"
        assert run.attributes["run.type"] == "single"
        execute = otel.span("executor.AsyncTaskWorkflowExecutor.execute")
        assert execute.parent.span_id == run.context.span_id

    async def test_the_run_metrics(self, otel, executor):
        await executor("wf", {})
        assert (
            otel.total(
                "workflow.run.count",
                workflow="wf",
                outcome="success",
                executor="AsyncTaskWorkflowExecutor",
            )
            == 1
        )
        (point,) = otel.points("workflow.run.duration")
        assert point["count"] == 1

    async def test_the_volnux_ids_are_attributes(self, otel, executor):
        from volnux.context import volnux_context

        with volnux_context(correlation_id="req-1"):
            await executor("wf", {})
        assert otel.span("workflow.run").attributes["volnux.correlation_id"] == "req-1"

    async def test_a_failing_run(self, otel, executor):
        with pytest.raises(WorkflowExecutionError):
            await executor("missing", {})
        assert otel.span("workflow.run").status.status_code is StatusCode.ERROR
        assert otel.total("workflow.run.count", outcome="error") == 1

    async def test_the_params_are_not_recorded(self, otel, executor):
        await executor("wf", {"password": "hunter2"})
        assert "hunter2" not in str([dict(s.attributes) for s in otel.spans()])

    async def test_without_a_tracer(self, no_otel):
        framework.patch_all()
        try:
            executor = AsyncTaskWorkflowExecutor(FakeRegistry("wf"))
            assert await executor("wf", {}) == "result"
        finally:
            unpatch_all()


# ------------------------------------------------------------------- registry
class FakeSource:
    def __init__(self, name, result=None, error=None):
        self.name = name
        self.source_type = RegistrySource.LOCAL
        self.location = name
        self._result, self._error = result, error

    async def load_workflow_config(self, registry, params):
        if self._error:
            raise self._error
        return self._result or SimpleNamespace(error=False, content="")


class TestRegistry:
    async def test_load_and_each_source(self, otel, tmp_path):
        framework.patch_all()
        registry = WorkflowRegistry(cache_dir=tmp_path / "c")
        registry._workflow_local_sources["good"] = FakeSource("good")
        registry._workflow_local_sources["bad"] = FakeSource(
            "bad", result=SimpleNamespace(error=True, content="no")
        )
        registry._workflow_local_sources["raising"] = FakeSource(
            "raising", error=OSError("disk")
        )
        await registry.load_workflow_configs()

        load = otel.span("registry.load")
        assert load.attributes["registry.sources"] == 3
        assert load.attributes["registry.failed_sources"] == 2
        sources = {
            s.attributes["source.name"]: s
            for s in otel.spans_named("registry.load_source")
        }
        assert set(sources) == {"good", "bad", "raising"}
        assert sources["good"].attributes["source.type"] == "local"
        assert sources["good"].attributes["source.error"] is False
        assert sources["bad"].attributes["source.error"] is True
        assert sources["raising"].status.status_code is StatusCode.ERROR
        for source in sources.values():
            assert source.parent.span_id == load.context.span_id
        assert otel.points("registry.load.duration")[0]["count"] == 1


# ------------------------------------------------------------------ messaging
class Note(MessagingBackendIntegrationMixin, BaseModel):
    class Config:
        init_strategy = InitStrategy.DATACLASS

    n: int

    @classmethod
    def get_storage_route(cls):
        return StorageRoute(
            components=["notes", "{box}"], routing_keys=lambda: {"box": "a"}
        )


@pytest.fixture
def notes(otel, monkeypatch):
    backend = SerializingMemoryBackend(namespace_prefix="otel")
    monkeypatch.setattr(Note, "_backend_store", backend, raising=False)
    monkeypatch.setattr(Note, "_backend_initialized", True, raising=False)
    framework.patch_all()
    yield otel
    backend.close()


class TestMessaging:
    async def test_push_and_pop(self, notes):
        assert await Note.push(Note(n=1), Note(n=2)) == 2
        assert (await Note.pop()).n == 1
        assert [n.n for n in await Note.pop_many()] == [2]
        push = notes.span("messaging.push")
        assert push.attributes["messaging.model"] == "Note"
        assert push.attributes["messaging.operation"] == "push"
        assert notes.span("messaging.pop").attributes["messaging.model"] == "Note"
        notes.span("messaging.pop_many")

    async def test_the_other_operations(self, notes):
        await Note.enqueue(Note(n=1))
        assert (await Note.dequeue()).n == 1
        await Note.push(Note(n=2), route_override={"box": "b"})
        assert await Note.queue_delete(route_override={"box": "b"}) is True
        for name in ("enqueue", "dequeue", "queue_delete"):
            notes.span(f"messaging.{name}")

    async def test_the_timeout_is_an_attribute(self, notes):
        assert await Note.pop(timeout=0.01) is None
        assert notes.span("messaging.pop").attributes["messaging.timeout"] == 0.01

    async def test_metrics_by_operation_model_and_outcome(self, notes):
        await Note.push(Note(n=1))
        await Note.pop()
        assert (
            notes.total(
                "messaging.operations",
                operation="push",
                model="Note",
                outcome="success",
            )
            == 1
        )
        assert notes.total("messaging.operations", operation="pop") == 1
        assert notes.points("messaging.duration")

    async def test_a_refused_operation_is_an_error(self, notes):
        with pytest.raises(ValueError):
            await Note.push()  # nothing to push
        assert notes.span("messaging.push").status.status_code is StatusCode.ERROR
        assert (
            notes.total("messaging.operations", operation="push", outcome="error") == 1
        )

    async def test_the_contents_of_a_message_are_not_recorded(self, notes):
        await Note.push(Note(n=424242))
        assert "424242" not in str([dict(s.attributes) for s in notes.spans()])

    async def test_ids_are_not_metric_attributes(self, notes):
        await Note.push(Note(n=1), route_override={"box": "unique-id-123"})
        for point in notes.points("messaging.operations"):
            assert "unique-id-123" not in str(point["attributes"])

    async def test_subscribe_is_not_traced(self, notes):
        # (it runs until cancelled: a span around it would last for ever)
        assert not is_patched(MessagingBackendIntegrationMixin, "subscribe")


# --------------------------------------------------------------------- sagas
@pytest.fixture
def saga_backends(monkeypatch):
    store = {}

    async def save(self):
        store[self.id] = self

    async def get(cls, key):
        if key not in store:
            raise KeyError(key)
        return store[key]

    async def delete(self):
        store.pop(self.id, None)

    async def enqueue(cls, entry):
        store.setdefault("__dlq__", []).append(entry)

    monkeypatch.setattr(SagaRecoveryState, "save", save)
    monkeypatch.setattr(SagaRecoveryState, "get", classmethod(get))
    monkeypatch.setattr(SagaRecoveryState, "delete", delete)
    monkeypatch.setattr(DeadLetterEntry, "enqueue", classmethod(enqueue))
    return store


def build_saga(log, fail_at=None):
    saga = Saga(name="order")
    for n in range(3):

        def do(n=n):
            if n == fail_at:
                raise RuntimeError(f"step {n} failed")
            log.append(f"do{n}")

        def undo(n=n):
            log.append(f"undo{n}")

        saga.add_step(f"s{n}", do, undo)
    return saga


class TestSaga:
    async def test_a_successful_saga(self, otel, saga_backends):
        framework.patch_all()
        log = []
        result = await build_saga(log).execute()
        assert result.status is SagaStatus.COMPLETED
        span = otel.span("saga.execute")
        assert span.attributes["saga.name"] == "order"
        assert span.attributes["saga.steps"] == 3
        assert span.attributes["saga.status"] == SagaStatus.COMPLETED.value
        steps = otel.spans_named("saga.step")
        assert [s.attributes["saga.step.name"] for s in steps] == ["s0", "s1", "s2"]
        assert [s.attributes["saga.step.index"] for s in steps] == [0, 1, 2]
        for step in steps:
            assert step.parent.span_id == span.context.span_id
        assert otel.total("saga.executions", outcome="success") == 1
        # recovery state is saved around the steps
        assert otel.spans_named("saga.recovery.save")

    async def test_a_failing_saga_compensates(self, otel, saga_backends):
        framework.patch_all()
        log = []
        result = await build_saga(log, fail_at=1).execute()
        assert result.status is not SagaStatus.COMPLETED
        compensate = otel.span("saga.compensate")
        assert compensate.attributes["saga.failed_step"] == 1
        assert otel.span("saga.execute").attributes["saga.failed_step"] == 1
        failed_step = [
            s
            for s in otel.spans_named("saga.step")
            if s.attributes["saga.step.index"] == 1
        ][0]
        assert failed_step.status.status_code is StatusCode.ERROR

    async def test_recovery_manager_spans_carry_the_saga_id(self, otel, saga_backends):
        framework.patch_all()
        from volnux.backends.saga import SagaRecoveryManager

        await SagaRecoveryManager.save_state("sg-1", "n", 0, "running", [], 1.0)
        assert await SagaRecoveryManager.load_state("sg-1") is not None
        await SagaRecoveryManager.delete_state("sg-1")
        for op in ("save", "load", "delete"):
            assert otel.span(f"saga.recovery.{op}").attributes["saga.id"] == "sg-1"

    async def test_resume(self, otel, saga_backends):
        framework.patch_all()
        log = []
        saga = build_saga(log, fail_at=1)
        await saga.execute()
        # interrupted sagas are resumed by id with the same steps
        from volnux.backends.saga import SagaRecoveryManager

        state = await SagaRecoveryManager.load_state(saga.saga_id)
        if state is not None:
            await Saga.resume(saga.saga_id, build_saga([], fail_at=None)._steps)
            assert otel.span("saga.resume").attributes["saga.id"] == saga.saga_id


# ---------------------------------------- checkpoints, rehydration, remote
class TestWrappedEntries:
    """The entries whose owners are heavy to build are checked through their
    instrumenter on a stand-in with the same method shape."""

    async def test_checkpoint_persist(self, otel):
        class Manager:
            async def _persist_with_retry(self, snapshot):
                return "saved"

        Manager._persist_with_retry = wrapper_for(
            "VolnuxCheckPointManager", "_persist_with_retry"
        )(Manager._persist_with_retry)
        snapshot = type("EventCheckpointSnapshot", (), {})()
        assert await Manager()._persist_with_retry(snapshot) == "saved"
        span = otel.span("checkpoint.persist")
        assert span.attributes["checkpoint.snapshot"] == "EventCheckpointSnapshot"
        assert (
            otel.total("checkpoint.operations", operation="persist", outcome="success")
            == 1
        )
        assert otel.points("checkpoint.duration")

    async def test_a_failing_persist_is_an_error(self, otel):
        class Manager:
            async def _persist_with_retry(self, snapshot):
                raise ConnectionError("store down")

        Manager._persist_with_retry = wrapper_for(
            "VolnuxCheckPointManager", "_persist_with_retry"
        )(Manager._persist_with_retry)
        with pytest.raises(ConnectionError):
            await Manager()._persist_with_retry(object())
        assert otel.span("checkpoint.persist").status.status_code is StatusCode.ERROR
        assert (
            otel.total("checkpoint.operations", operation="persist", outcome="error")
            == 1
        )

    async def test_snapshot_build(self, otel):
        class Builder:
            async def build(self, event):
                return "snapshot"

        Builder.build = wrapper_for("SnapshotBuilder", "build")(Builder.build)
        event = type("MyEvent", (), {})()
        await Builder().build(event)
        assert (
            otel.span("checkpoint.build_event").attributes["event.class"] == "MyEvent"
        )

    async def test_event_rehydration(self, otel):
        class Rehydrator:
            async def rehydrate(self, snapshot, checkpoint_manager=None):
                return "event"

        Rehydrator.rehydrate = wrapper_for("EventRehydrator", "rehydrate")(
            Rehydrator.rehydrate
        )
        snapshot = SimpleNamespace(
            class_path="pkg.mod.MyEvent", phase=SimpleNamespace(name="PROCESSING")
        )
        await Rehydrator().rehydrate(snapshot)
        span = otel.span("rehydrate.event")
        assert span.attributes["event.class"] == "MyEvent"
        assert span.attributes["event.phase"] == "PROCESSING"
        assert otel.total("checkpoint.operations", operation="rehydrate_event") == 1

    async def test_engine_rehydration(self, otel):
        class Lazy:
            async def resume_from(self, state_id, engine_class=None):
                return "ctx", None

            async def rehydrate_node(self, state_id):
                return "node"

        for name in ("resume_from", "rehydrate_node"):
            setattr(
                Lazy, name, wrapper_for("LazyRehydrator", name)(getattr(Lazy, name))
            )
        await Lazy().resume_from("state-1")
        await Lazy().rehydrate_node("state-2")
        assert (
            otel.span("rehydrate.resume").attributes["rehydrate.state_id"] == "state-1"
        )
        assert otel.span("rehydrate.node").attributes["rehydrate.state_id"] == "state-2"

    async def test_remote_task_execution(self, otel):
        async def execute_remote_task(payload, flush_callback=None, batch_key=None):
            return SimpleNamespace(
                context_delta={"mode": "batch", "final_status": "COMPLETED"},
                execution_duration=1.5,
            )

        execute = wrapper_for("", "execute_remote_task")(execute_remote_task)
        payload = SimpleNamespace(
            event_class_path="pkg.MyEvent",
            task_id="t-1",
            mini_context={"state_id": "s-1"},
        )
        await execute(payload)
        span = otel.span("remote.execute_task")
        assert span.attributes["remote.event"] == "MyEvent"
        assert span.attributes["remote.task_id"] == "t-1"
        assert span.attributes["remote.state_id"] == "s-1"
        assert span.attributes["remote.mode"] == "batch"
        assert span.attributes["remote.final_status"] == "COMPLETED"

    async def test_reading_a_remote_stream(self, otel, monkeypatch):
        from volnux.execution.remote.stream import (
            BatchStreamReader,
            RemoteBatch,
            batch_route,
            done_entry,
        )

        backend = SerializingMemoryBackend(namespace_prefix="remote-otel")
        monkeypatch.setattr(RemoteBatch, "_backend_store", backend, raising=False)
        monkeypatch.setattr(RemoteBatch, "_backend_initialized", True, raising=False)
        framework.patch_all()
        route = batch_route("S", "T")
        await RemoteBatch.push(
            RemoteBatch(kind="result_batch", data=[1, 2], count=2), route_override=route
        )
        await RemoteBatch.push(done_entry("COMPLETED"), route_override=route)

        results, errors = await BatchStreamReader("S", "T").read_all(timeout=1)
        assert (results, errors) == ([1, 2], [])
        span = otel.span("remote.read_stream")
        assert span.attributes["remote.stream"] == "volnux:batches:S:T"
        # the pops it made are messaging spans under it
        pops = otel.spans_named("messaging.pop")
        assert pops and all(p.parent.span_id == span.context.span_id for p in pops)
        backend.close()
