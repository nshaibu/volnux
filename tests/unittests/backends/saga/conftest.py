"""Fixtures for saga tests: in-memory checkpoint store and DLQ, no Redis."""

from typing import Any, Dict, List

import pytest

from volnux.backends.saga._dlq import DeadLetterEntry
from volnux.backends.saga.recovery import SagaRecoveryState


@pytest.fixture
def checkpoints(monkeypatch) -> Dict[str, SagaRecoveryState]:
    """Replace SagaRecoveryState persistence with a dict keyed by object id."""
    store: Dict[str, SagaRecoveryState] = {}

    async def save(self) -> None:
        store[self.id] = self

    async def get(cls, key: str):
        if key not in store:
            raise KeyError(key)
        return store[key]

    async def delete(self) -> None:
        store.pop(self.id, None)

    monkeypatch.setattr(SagaRecoveryState, "save", save)
    monkeypatch.setattr(SagaRecoveryState, "get", classmethod(get))
    monkeypatch.setattr(SagaRecoveryState, "delete", delete)
    return store


@pytest.fixture
def dlq(monkeypatch) -> List[Any]:
    """Collect DeadLetterEntry objects instead of writing to Redis."""
    entries: List[Any] = []

    async def enqueue(cls, entry) -> None:
        entries.append(entry)

    monkeypatch.setattr(DeadLetterEntry, "enqueue", classmethod(enqueue))
    return entries


@pytest.fixture(autouse=True)
def _saga_backends(checkpoints, dlq):
    yield
