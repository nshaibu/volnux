"""Fixtures for the trigger tests: the state record goes to an in-memory backend that
stores JSON, so a record read back is a copy rebuilt from its serialised state (as it
is from SQLite), not the very object that was saved."""

import pytest

from tests.unittests.support import SerializingMemoryBackend
from volnux.engine.workflows.trigger.state import TriggerStateRecord


@pytest.fixture
def store(monkeypatch):
    backend = SerializingMemoryBackend()
    monkeypatch.setattr(TriggerStateRecord, "_backend_store", backend, raising=False)
    monkeypatch.setattr(TriggerStateRecord, "_backend_initialized", True, raising=False)
    yield backend
    backend.close()


@pytest.fixture
def message_bus(monkeypatch):
    """TriggerEvent travels over an in-memory messaging backend."""
    from volnux.engine.workflows.trigger.event import TriggerEvent
    from volnux.engine.workflows.trigger.triggers.base import TriggerBase

    backend = SerializingMemoryBackend()
    monkeypatch.setattr(TriggerEvent, "_backend_store", backend, raising=False)
    monkeypatch.setattr(TriggerEvent, "_backend_initialized", True, raising=False)
    yield backend
    backend.close()
