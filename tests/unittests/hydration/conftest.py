"""Shared fixtures for the hydration path: suspend -> checkpoint -> rehydrate -> resume.

Every persisted model is pointed at one in-memory backend (key-value store AND
messaging queues), so no Redis or database is needed.
"""

import itertools
from typing import Any, Callable, List, Type

import pytest

from volnux import EventBase, Pipeline
from volnux.backends.stores.inmemory import InMemoryKeyValueStoreBackend

_names = itertools.count()


def persisted_models() -> List[Type[Any]]:
    from volnux.execution.context import ExecutionContext
    from volnux.execution.rehydrator.engine.snapshot import (
        EngineStateSnapshot,
        TaskSnapshot,
    )
    from volnux.execution.rehydrator.event.snapshot import EventCheckpointSnapshot
    from volnux.result import EventResult

    models = [
        ExecutionContext,
        EngineStateSnapshot,
        TaskSnapshot,
        EventCheckpointSnapshot,
        EventResult,
    ]
    try:  # present once the external-communication models are rebuilt
        from volnux.mixins.event.communication.datastructures import (
            ExternalCommunicationQueueEntry,
            ExternalCommunicationResponse,
            ResumeClaim,
        )

        models += [
            ExternalCommunicationQueueEntry,
            ExternalCommunicationResponse,
            ResumeClaim,
        ]
    except ImportError:
        pass
    return models


class SerializingMemoryBackend(InMemoryKeyValueStoreBackend):
    """The in-memory backend, storing JSON like a real store does.

    The plain in-memory backend hands back the very objects that were saved, which
    hides every bug in how state is written and rebuilt. Here a record read back is
    a copy rebuilt from its serialised state, as it would be from Redis or a database.
    """

    def insert(self, schema_name, record_key, record, ttl=None):
        super().insert(schema_name, record_key, self._serialize_record(record), ttl)

    def update(self, schema_name, record_key, record):
        super().update(schema_name, record_key, self._serialize_record(record))

    def get(self, schema_name, record_key, record_klass):
        raw = super().get(schema_name, record_key, record_klass)
        return self._deserialize_record(raw, record_klass)

    def get_many(self, schema_name, record_keys, record_klass):
        return {
            key: self._deserialize_record(raw, record_klass)
            for key, raw in super()
            .get_many(schema_name, record_keys, record_klass)
            .items()
        }

    def filter(
        self,
        schema_name,
        record_klass,
        limit=None,
        offset=None,
        order_by=None,
        **filters,
    ):
        predicate = self.create_filter_predicate(**filters)
        bucket = self._get_schema_bucket(schema_name)
        keys = [
            key
            for key, raw in bucket.items()
            if predicate(self._deserialize_record(raw, record_klass))
        ]
        return self._create_result_stream(record_keys=keys, record_klass=record_klass)


def _install(monkeypatch, backend):
    for model in persisted_models():
        monkeypatch.setattr(model, "_backend_store", backend, raising=False)
        monkeypatch.setattr(model, "_backend_initialized", True, raising=False)


@pytest.fixture
def memory_store(monkeypatch):
    """One in-memory backend behind every persisted model (live objects)."""
    backend = InMemoryKeyValueStoreBackend()
    _install(monkeypatch, backend)
    yield backend
    backend.close()


@pytest.fixture
def serialized_store(monkeypatch):
    """Like ``memory_store`` but records round-trip through JSON, as in production."""
    backend = SerializingMemoryBackend()
    _install(monkeypatch, backend)
    yield backend
    backend.close()


def unique(prefix: str) -> str:
    return "%s%d" % (prefix, next(_names))


def _register(cls):
    """Make a generated class importable by path, as a real event or pipeline is."""
    from . import generated

    setattr(generated, cls.__name__, cls)
    return cls


@pytest.fixture
def make_event() -> Callable[..., Type[EventBase]]:
    def factory(process: Callable[..., Any], name: str = "Step", **attrs: Any):
        attrs = {"__module__": generated_module(), "process": process, **attrs}
        return _register(type(unique(name), (EventBase,), attrs))

    return factory


@pytest.fixture
def make_pipeline() -> Callable[[str], Type[Pipeline]]:
    def factory(pointy: str, **attrs: Any) -> Type[Pipeline]:
        namespace = {
            "__module__": generated_module(),
            "Meta": type("Meta", (), {"pointy": pointy}),
            **attrs,
        }
        return _register(type(unique("Pipe"), (Pipeline,), namespace))

    return factory


def generated_module() -> str:
    from . import generated

    return generated.__name__


@pytest.fixture
def seen():
    return {}


@pytest.fixture
def workflow_factory(make_event, make_pipeline, seen):
    """``workflow_factory(timeout_hours=None)`` -> a pipeline class A -> B -> C.

    B asks a human in communicate() unless it already has the answer.
    """
    ran = seen.setdefault("ran", [])

    def build(timeout_hours=None):
        return _build_workflow(make_event, make_pipeline, seen, ran, timeout_hours)

    return build


@pytest.fixture
def workflow(workflow_factory):
    return workflow_factory()


def _build_workflow(make_event, make_pipeline, seen, ran, timeout_hours):

    async def a(self):
        ran.append("A")
        return True, "a-result"

    async def communicate(self, *args, **kwargs):
        answer = self.get_response()
        seen.setdefault("answers", []).append(answer)
        if answer is None:
            await self.request_human_input(
                "Approve order",
                "Order 7",
                {"order": 7},
                options=["approve", "reject"],
                timeout_hours=timeout_hours,
            )

    async def b(self, *args, **kwargs):
        ran.append("B")
        seen["decision"] = self.get_response().content["decision"]
        return True, "b-result"

    async def c(self):
        ran.append("C")
        return True, "c-result"

    A = make_event(a, "StepA")
    B = make_event(b, "StepB", communicate=communicate)
    C = make_event(c, "StepC")
    return make_pipeline("%s -> %s -> %s" % (A.__name__, B.__name__, C.__name__))
